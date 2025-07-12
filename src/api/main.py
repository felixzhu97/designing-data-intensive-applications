#!/usr/bin/env python3
"""
数据密集型应用系统 - API服务
提供REST API接口，展示系统各个组件的功能
"""
import asyncio
import time
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import uvicorn

# 导入我们的组件
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '../..'))

from src.data_models.relational import User, Post, UserCreate, PostCreate
from src.data_models.document import UserProfile, PostDocument, ActivityLog
from src.data_models.timeseries import UserActivity, SystemMetrics
from src.storage.lsm_tree import LSMTree
from src.replication.master_slave import MasterSlaveReplication
from src.stream_processing.event_stream import StreamProcessingTopology, EventPublisher
from config.settings import settings

# 创建FastAPI应用
app = FastAPI(
    title="数据密集型应用系统",
    description="《数据密集型应用系统设计》完整案例的API接口",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc"
)

# 添加CORS中间件
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 全局组件实例
lsm_storage = None
replication_system = None
stream_topology = None
event_publisher = None
system_stats = {
    "start_time": datetime.utcnow(),
    "requests_count": 0,
    "errors_count": 0,
    "active_users": set(),
}

@app.on_event("startup")
async def startup_event():
    """应用启动时初始化组件"""
    global lsm_storage, replication_system, stream_topology, event_publisher
    
    print("🚀 启动数据密集型应用系统...")
    
    # 初始化LSM-Tree存储
    lsm_storage = LSMTree("/tmp/ddia_api_storage", memtable_size=1024*1024)
    print("✅ LSM-Tree存储引擎已启动")
    
    # 初始化复制系统
    replication_system = MasterSlaveReplication()
    master = replication_system.create_master("api-master")
    slave1 = replication_system.add_slave("api-slave-1")
    slave2 = replication_system.add_slave("api-slave-2")
    print("✅ 主从复制系统已启动")
    
    # 初始化流处理拓扑
    stream_topology = StreamProcessingTopology("api-topology")
    user_events_stream = stream_topology.create_stream("user_events", partitions=8)
    system_events_stream = stream_topology.create_stream("system_events", partitions=4)
    
    event_publisher = stream_topology.create_publisher("user_events")
    
    # 创建事件消费器
    analytics_consumer = stream_topology.create_consumer("analytics", "user_events")
    analytics_consumer.register_handler("user_action", handle_user_action_event)
    
    stream_topology.start_all()
    print("✅ 流处理系统已启动")
    
    print("🎉 系统启动完成！API服务运行在 http://localhost:8000")

@app.on_event("shutdown")
async def shutdown_event():
    """应用关闭时清理资源"""
    global stream_topology
    
    if stream_topology:
        stream_topology.stop_all()
    
    print("👋 系统已关闭")

# 中间件：请求统计
@app.middleware("http")
async def stats_middleware(request, call_next):
    start_time = time.time()
    system_stats["requests_count"] += 1
    
    try:
        response = await call_next(request)
        return response
    except Exception as e:
        system_stats["errors_count"] += 1
        raise
    finally:
        process_time = time.time() - start_time
        # 记录系统指标
        if event_publisher:
            event_publisher.publish_system_event(
                component="api_server",
                event_name="request_processed",
                metrics={
                    "path": str(request.url.path),
                    "method": request.method,
                    "process_time_ms": process_time * 1000,
                    "status": "success"
                }
            )

# API请求/响应模型
class ApiResponse(BaseModel):
    """标准API响应"""
    success: bool
    data: Any = None
    message: str = ""
    timestamp: datetime

class UserActionRequest(BaseModel):
    """用户行为请求"""
    user_id: str
    action: str
    details: Dict[str, Any] = {}

class DataWriteRequest(BaseModel):
    """数据写入请求"""
    key: str
    value: Any

class QueryRequest(BaseModel):
    """查询请求"""
    query: str
    filters: Dict[str, Any] = {}
    limit: int = 100

# 根路径
@app.get("/", response_model=ApiResponse)
async def root():
    """根路径 - 系统概览"""
    uptime = datetime.utcnow() - system_stats["start_time"]
    
    return ApiResponse(
        success=True,
        data={
            "system_name": "数据密集型应用系统",
            "version": "1.0.0",
            "uptime_seconds": uptime.total_seconds(),
            "components": {
                "storage_engine": "LSM-Tree",
                "replication": "Master-Slave",
                "stream_processing": "Event-Driven",
                "api_framework": "FastAPI"
            },
            "stats": {
                "requests_count": system_stats["requests_count"],
                "errors_count": system_stats["errors_count"],
                "active_users": len(system_stats["active_users"])
            }
        },
        message="系统运行正常",
        timestamp=datetime.utcnow()
    )

# 存储引擎API
@app.post("/storage/write", response_model=ApiResponse)
async def write_data(request: DataWriteRequest):
    """写入数据到LSM-Tree存储"""
    try:
        # 写入LSM-Tree
        lsm_storage.put(request.key, json.dumps(request.value))
        
        # 同时写入复制系统
        replication_system.write(request.key, request.value)
        
        # 发布数据写入事件
        event_publisher.publish(
            event_type="data_write",
            data={
                "key": request.key,
                "operation": "write",
                "timestamp": datetime.utcnow().isoformat()
            },
            partition_key=request.key
        )
        
        return ApiResponse(
            success=True,
            data={"key": request.key, "stored": True},
            message="数据写入成功",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"写入失败: {str(e)}")

@app.get("/storage/read/{key}", response_model=ApiResponse)
async def read_data(key: str, from_replica: bool = False):
    """从存储引擎读取数据"""
    try:
        if from_replica:
            # 从复制系统读取（可能从从节点读取）
            value = replication_system.read(key)
        else:
            # 从LSM-Tree读取
            value_json = lsm_storage.get(key)
            value = json.loads(value_json) if value_json else None
        
        # 发布数据读取事件
        event_publisher.publish(
            event_type="data_read",
            data={
                "key": key,
                "operation": "read",
                "from_replica": from_replica,
                "found": value is not None,
                "timestamp": datetime.utcnow().isoformat()
            },
            partition_key=key
        )
        
        return ApiResponse(
            success=True,
            data={"key": key, "value": value, "from_replica": from_replica},
            message="数据读取成功" if value else "数据不存在",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"读取失败: {str(e)}")

@app.delete("/storage/delete/{key}", response_model=ApiResponse)
async def delete_data(key: str):
    """删除数据"""
    try:
        # 从LSM-Tree删除
        lsm_storage.delete(key)
        
        # 从复制系统删除
        replication_system.delete(key)
        
        # 发布数据删除事件
        event_publisher.publish(
            event_type="data_delete",
            data={
                "key": key,
                "operation": "delete",
                "timestamp": datetime.utcnow().isoformat()
            },
            partition_key=key
        )
        
        return ApiResponse(
            success=True,
            data={"key": key, "deleted": True},
            message="数据删除成功",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"删除失败: {str(e)}")

# 复制系统API
@app.get("/replication/status", response_model=ApiResponse)
async def get_replication_status():
    """获取复制系统状态"""
    try:
        status = replication_system.get_cluster_status()
        
        return ApiResponse(
            success=True,
            data=status,
            message="复制状态获取成功",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取状态失败: {str(e)}")

@app.post("/replication/failover/{slave_id}", response_model=ApiResponse)
async def trigger_failover(slave_id: str):
    """触发故障转移"""
    try:
        success = replication_system.promote_slave_to_master(slave_id)
        
        if success:
            # 发布故障转移事件
            event_publisher.publish_system_event(
                component="replication",
                event_name="failover_completed",
                metrics={
                    "new_master": slave_id,
                    "timestamp": datetime.utcnow().isoformat()
                }
            )
        
        return ApiResponse(
            success=success,
            data={"new_master": slave_id if success else None},
            message="故障转移成功" if success else "故障转移失败",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"故障转移失败: {str(e)}")

# 流处理API
@app.post("/events/user-action", response_model=ApiResponse)
async def publish_user_action(request: UserActionRequest):
    """发布用户行为事件"""
    try:
        event_id = event_publisher.publish_user_action(
            user_id=request.user_id,
            action=request.action,
            details=request.details
        )
        
        # 更新活跃用户统计
        system_stats["active_users"].add(request.user_id)
        
        return ApiResponse(
            success=True,
            data={"event_id": event_id, "user_id": request.user_id},
            message="用户行为事件发布成功",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"事件发布失败: {str(e)}")

@app.get("/events/stats", response_model=ApiResponse)
async def get_stream_stats():
    """获取流处理统计"""
    try:
        stats = stream_topology.get_stats()
        
        return ApiResponse(
            success=True,
            data=stats,
            message="流处理统计获取成功",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取统计失败: {str(e)}")

# 系统监控API
@app.get("/monitoring/health", response_model=ApiResponse)
async def health_check():
    """健康检查"""
    try:
        # 检查各个组件状态
        lsm_stats = lsm_storage.stats()
        replication_status = replication_system.get_cluster_status()
        stream_stats = stream_topology.get_stats()
        
        health_status = {
            "overall": "healthy",
            "components": {
                "storage": "healthy" if lsm_stats["memtable_size"] < 10*1024*1024 else "warning",
                "replication": "healthy" if replication_status["healthy_slaves"] > 0 else "warning",
                "streaming": "healthy" if len(stream_stats["processors"]) > 0 else "warning"
            },
            "metrics": {
                "storage_size_mb": lsm_stats["total_sstable_size"] / (1024*1024),
                "replication_lag_ms": replication_status.get("average_lag_ms", 0),
                "event_throughput": sum(stream_stats["publishers"].values())
            }
        }
        
        return ApiResponse(
            success=True,
            data=health_status,
            message="系统健康检查完成",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"健康检查失败: {str(e)}")

@app.get("/monitoring/metrics", response_model=ApiResponse)
async def get_system_metrics():
    """获取系统指标"""
    try:
        uptime = datetime.utcnow() - system_stats["start_time"]
        
        metrics = {
            "system": {
                "uptime_seconds": uptime.total_seconds(),
                "requests_total": system_stats["requests_count"],
                "errors_total": system_stats["errors_count"],
                "error_rate": system_stats["errors_count"] / max(system_stats["requests_count"], 1),
                "active_users": len(system_stats["active_users"])
            },
            "storage": lsm_storage.stats(),
            "replication": replication_system.get_cluster_status(),
            "streaming": stream_topology.get_stats()
        }
        
        return ApiResponse(
            success=True,
            data=metrics,
            message="系统指标获取成功",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"获取指标失败: {str(e)}")

# 演示API
@app.get("/demo/consistency", response_model=ApiResponse)
async def demo_consistency():
    """演示数据一致性"""
    try:
        demo_key = f"demo_consistency_{int(time.time())}"
        demo_value = {"message": "一致性测试", "timestamp": datetime.utcnow().isoformat()}
        
        # 写入数据
        replication_system.write(demo_key, demo_value)
        
        # 立即从主节点读取
        master_value = replication_system.master.get(demo_key)
        
        # 等待一小段时间后从从节点读取
        await asyncio.sleep(0.1)
        replication_system.read_preference = "secondary"
        slave_value = replication_system.read(demo_key)
        replication_system.read_preference = "primary"
        
        return ApiResponse(
            success=True,
            data={
                "demo_key": demo_key,
                "master_value": master_value,
                "slave_value": slave_value,
                "consistent": master_value == slave_value
            },
            message="一致性演示完成",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"一致性演示失败: {str(e)}")

@app.post("/demo/load-test", response_model=ApiResponse)
async def demo_load_test(background_tasks: BackgroundTasks, duration_seconds: int = 10):
    """演示负载测试"""
    try:
        background_tasks.add_task(run_load_test, duration_seconds)
        
        return ApiResponse(
            success=True,
            data={"duration_seconds": duration_seconds, "status": "started"},
            message="负载测试已启动",
            timestamp=datetime.utcnow()
        )
        
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"负载测试启动失败: {str(e)}")

# 背景任务
async def run_load_test(duration_seconds: int):
    """运行负载测试"""
    start_time = time.time()
    operation_count = 0
    
    while time.time() - start_time < duration_seconds:
        try:
            # 随机写入数据
            key = f"load_test_{operation_count}"
            value = {
                "operation_id": operation_count,
                "timestamp": datetime.utcnow().isoformat(),
                "data": f"test_data_{operation_count}"
            }
            
            replication_system.write(key, value)
            
            # 发布负载测试事件
            event_publisher.publish(
                event_type="load_test",
                data={
                    "operation_id": operation_count,
                    "operation": "write",
                    "timestamp": datetime.utcnow().isoformat()
                },
                partition_key=str(operation_count % 8)
            )
            
            operation_count += 1
            await asyncio.sleep(0.01)  # 100 ops/sec
            
        except Exception as e:
            print(f"Load test error: {e}")
            break
    
    print(f"Load test completed: {operation_count} operations in {duration_seconds} seconds")

# 事件处理器
def handle_user_action_event(event):
    """处理用户行为事件"""
    try:
        user_id = event.data.get("user_id")
        action = event.data.get("action")
        
        # 更新用户活跃度统计
        if user_id:
            system_stats["active_users"].add(user_id)
        
        print(f"User action processed: {user_id} - {action}")
        
    except Exception as e:
        print(f"Error processing user action event: {e}")

# 启动服务
if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.api_host,
        port=settings.api_port,
        reload=settings.debug,
        log_level="info"
    ) 