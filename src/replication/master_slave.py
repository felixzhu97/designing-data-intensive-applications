"""
主从复制系统实现
展示单主复制（Master-Slave）的设计模式
"""
import time
import threading
import asyncio
import json
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
import uuid
import queue
import socket

from ..data_models.base import BaseModel


class ReplicationLogEntry(BaseModel):
    """复制日志条目"""
    log_sequence_number: int
    timestamp: float
    operation: str  # INSERT, UPDATE, DELETE
    table: str
    key: str
    value: Any
    checksum: str


class ReplicationStatus(str, Enum):
    """复制状态"""
    HEALTHY = "healthy"
    LAGGING = "lagging"
    FAILED = "failed"
    RECOVERING = "recovering"


@dataclass
class SlaveInfo:
    """从节点信息"""
    slave_id: str
    host: str
    port: int
    last_log_sequence: int
    lag_ms: float
    status: ReplicationStatus
    last_heartbeat: float


class ReplicationMaster:
    """复制主节点"""
    
    def __init__(self, node_id: str = None):
        self.node_id = node_id or str(uuid.uuid4())
        self.data: Dict[str, Any] = {}
        self.replication_log: List[ReplicationLogEntry] = []
        self.slaves: Dict[str, SlaveInfo] = {}
        self.current_lsn = 0  # Log Sequence Number
        
        # 配置
        self.sync_replication = False  # 同步/异步复制
        self.min_sync_slaves = 1      # 最少同步从节点数
        self.slave_timeout = 30       # 从节点超时时间（秒）
        
        # 线程安全
        self.lock = threading.RLock()
        self.log_lock = threading.Lock()
        
        # 启动心跳检查线程
        self.heartbeat_thread = threading.Thread(target=self._heartbeat_monitor, daemon=True)
        self.heartbeat_thread.start()
    
    def register_slave(self, slave_id: str, host: str, port: int) -> bool:
        """注册从节点"""
        with self.lock:
            self.slaves[slave_id] = SlaveInfo(
                slave_id=slave_id,
                host=host,
                port=port,
                last_log_sequence=0,
                lag_ms=0.0,
                status=ReplicationStatus.HEALTHY,
                last_heartbeat=time.time()
            )
            print(f"Slave {slave_id} registered at {host}:{port}")
            return True
    
    def unregister_slave(self, slave_id: str):
        """注销从节点"""
        with self.lock:
            if slave_id in self.slaves:
                del self.slaves[slave_id]
                print(f"Slave {slave_id} unregistered")
    
    def put(self, key: str, value: Any) -> bool:
        """写入数据"""
        try:
            # 1. 先写WAL（Write-Ahead Log）
            log_entry = self._create_log_entry("INSERT", "data", key, value)
            self._append_to_log(log_entry)
            
            # 2. 更新本地数据
            with self.lock:
                self.data[key] = value
            
            # 3. 复制到从节点
            if self.sync_replication:
                return self._sync_replicate(log_entry)
            else:
                self._async_replicate(log_entry)
                return True
                
        except Exception as e:
            print(f"Master write failed: {e}")
            return False
    
    def get(self, key: str) -> Optional[Any]:
        """读取数据"""
        with self.lock:
            return self.data.get(key)
    
    def delete(self, key: str) -> bool:
        """删除数据"""
        try:
            log_entry = self._create_log_entry("DELETE", "data", key, None)
            self._append_to_log(log_entry)
            
            with self.lock:
                if key in self.data:
                    del self.data[key]
            
            if self.sync_replication:
                return self._sync_replicate(log_entry)
            else:
                self._async_replicate(log_entry)
                return True
                
        except Exception as e:
            print(f"Master delete failed: {e}")
            return False
    
    def _create_log_entry(self, operation: str, table: str, key: str, value: Any) -> ReplicationLogEntry:
        """创建日志条目"""
        with self.log_lock:
            self.current_lsn += 1
            return ReplicationLogEntry(
                log_sequence_number=self.current_lsn,
                timestamp=time.time(),
                operation=operation,
                table=table,
                key=key,
                value=value,
                checksum=self._calculate_checksum(key, value)
            )
    
    def _append_to_log(self, entry: ReplicationLogEntry):
        """追加到复制日志"""
        with self.log_lock:
            self.replication_log.append(entry)
    
    def _calculate_checksum(self, key: str, value: Any) -> str:
        """计算校验和"""
        import hashlib
        data = f"{key}:{json.dumps(value, sort_keys=True)}"
        return hashlib.md5(data.encode()).hexdigest()
    
    def _sync_replicate(self, log_entry: ReplicationLogEntry) -> bool:
        """同步复制"""
        successful_replicas = 0
        
        for slave_id, slave_info in self.slaves.items():
            if self._send_log_entry_to_slave(slave_info, log_entry):
                successful_replicas += 1
        
        # 检查是否满足最小同步从节点数
        return successful_replicas >= self.min_sync_slaves
    
    def _async_replicate(self, log_entry: ReplicationLogEntry):
        """异步复制"""
        def replicate_async():
            for slave_id, slave_info in self.slaves.items():
                self._send_log_entry_to_slave(slave_info, log_entry)
        
        threading.Thread(target=replicate_async, daemon=True).start()
    
    def _send_log_entry_to_slave(self, slave_info: SlaveInfo, log_entry: ReplicationLogEntry) -> bool:
        """发送日志条目到从节点"""
        try:
            # 模拟网络发送（实际应该使用TCP/HTTP等协议）
            # 这里只是更新从节点状态
            current_time = time.time()
            lag = (current_time - log_entry.timestamp) * 1000  # 毫秒
            
            slave_info.last_log_sequence = log_entry.log_sequence_number
            slave_info.lag_ms = lag
            slave_info.last_heartbeat = current_time
            
            # 模拟网络延迟
            time.sleep(0.001)  # 1ms
            
            print(f"Replicated LSN {log_entry.log_sequence_number} to slave {slave_info.slave_id}")
            return True
            
        except Exception as e:
            print(f"Failed to replicate to slave {slave_info.slave_id}: {e}")
            slave_info.status = ReplicationStatus.FAILED
            return False
    
    def _heartbeat_monitor(self):
        """心跳监控线程"""
        while True:
            try:
                current_time = time.time()
                
                with self.lock:
                    for slave_id, slave_info in self.slaves.items():
                        # 检查从节点状态
                        time_since_heartbeat = current_time - slave_info.last_heartbeat
                        
                        if time_since_heartbeat > self.slave_timeout:
                            if slave_info.status != ReplicationStatus.FAILED:
                                print(f"Slave {slave_id} is not responding (timeout: {time_since_heartbeat:.1f}s)")
                                slave_info.status = ReplicationStatus.FAILED
                        elif slave_info.lag_ms > 5000:  # 5秒延迟
                            slave_info.status = ReplicationStatus.LAGGING
                        else:
                            slave_info.status = ReplicationStatus.HEALTHY
                
                time.sleep(5)  # 每5秒检查一次
                
            except Exception as e:
                print(f"Heartbeat monitor error: {e}")
    
    def get_replication_status(self) -> Dict[str, Any]:
        """获取复制状态"""
        with self.lock:
            return {
                "master_id": self.node_id,
                "current_lsn": self.current_lsn,
                "sync_replication": self.sync_replication,
                "slaves": {
                    slave_id: {
                        "status": slave_info.status.value,
                        "lag_ms": slave_info.lag_ms,
                        "last_lsn": slave_info.last_log_sequence,
                        "last_heartbeat": slave_info.last_heartbeat
                    }
                    for slave_id, slave_info in self.slaves.items()
                }
            }
    
    def get_log_entries(self, from_lsn: int, limit: int = 100) -> List[ReplicationLogEntry]:
        """获取日志条目（供从节点拉取）"""
        with self.log_lock:
            matching_entries = [
                entry for entry in self.replication_log
                if entry.log_sequence_number > from_lsn
            ]
            return matching_entries[:limit]


class ReplicationSlave:
    """复制从节点"""
    
    def __init__(self, slave_id: str = None, master_host: str = "localhost", master_port: int = 5432):
        self.slave_id = slave_id or str(uuid.uuid4())
        self.master_host = master_host
        self.master_port = master_port
        
        self.data: Dict[str, Any] = {}
        self.last_applied_lsn = 0
        self.is_running = False
        
        # 复制延迟统计
        self.lag_ms = 0.0
        self.status = ReplicationStatus.HEALTHY
        
        # 线程安全
        self.lock = threading.RLock()
    
    def start_replication(self, master: ReplicationMaster):
        """启动复制（用于演示，实际应该通过网络连接）"""
        self.master = master
        self.is_running = True
        
        # 注册到主节点
        self.master.register_slave(self.slave_id, "localhost", 5433)
        
        # 启动复制线程
        self.replication_thread = threading.Thread(target=self._replication_loop, daemon=True)
        self.replication_thread.start()
        
        print(f"Slave {self.slave_id} started replication")
    
    def stop_replication(self):
        """停止复制"""
        self.is_running = False
        if hasattr(self, 'master'):
            self.master.unregister_slave(self.slave_id)
        print(f"Slave {self.slave_id} stopped replication")
    
    def _replication_loop(self):
        """复制循环"""
        while self.is_running:
            try:
                # 从主节点拉取日志
                log_entries = self.master.get_log_entries(self.last_applied_lsn, limit=10)
                
                for entry in log_entries:
                    self._apply_log_entry(entry)
                
                if log_entries:
                    print(f"Slave {self.slave_id} applied {len(log_entries)} log entries")
                
                time.sleep(0.1)  # 100ms拉取间隔
                
            except Exception as e:
                print(f"Replication error on slave {self.slave_id}: {e}")
                self.status = ReplicationStatus.FAILED
                time.sleep(1)  # 错误时等待更长时间
    
    def _apply_log_entry(self, entry: ReplicationLogEntry):
        """应用日志条目"""
        try:
            with self.lock:
                if entry.operation == "INSERT" or entry.operation == "UPDATE":
                    self.data[entry.key] = entry.value
                elif entry.operation == "DELETE":
                    if entry.key in self.data:
                        del self.data[entry.key]
                
                self.last_applied_lsn = entry.log_sequence_number
                
                # 计算复制延迟
                current_time = time.time()
                self.lag_ms = (current_time - entry.timestamp) * 1000
                
                # 验证数据完整性
                expected_checksum = self.master._calculate_checksum(entry.key, entry.value)
                if entry.checksum != expected_checksum:
                    print(f"Checksum mismatch for key {entry.key}")
                
        except Exception as e:
            print(f"Failed to apply log entry {entry.log_sequence_number}: {e}")
            raise
    
    def get(self, key: str) -> Optional[Any]:
        """读取数据"""
        with self.lock:
            return self.data.get(key)
    
    def get_slave_status(self) -> Dict[str, Any]:
        """获取从节点状态"""
        with self.lock:
            return {
                "slave_id": self.slave_id,
                "last_applied_lsn": self.last_applied_lsn,
                "lag_ms": self.lag_ms,
                "status": self.status.value,
                "data_count": len(self.data),
                "is_running": self.is_running
            }


class MasterSlaveReplication:
    """主从复制系统管理器"""
    
    def __init__(self):
        self.master: Optional[ReplicationMaster] = None
        self.slaves: Dict[str, ReplicationSlave] = {}
        self.read_preference = "primary"  # primary, secondary, nearest
    
    def create_master(self, node_id: str = None) -> ReplicationMaster:
        """创建主节点"""
        self.master = ReplicationMaster(node_id)
        print(f"Master node created: {self.master.node_id}")
        return self.master
    
    def add_slave(self, slave_id: str = None) -> ReplicationSlave:
        """添加从节点"""
        if not self.master:
            raise ValueError("Master node must be created first")
        
        slave = ReplicationSlave(slave_id)
        slave.start_replication(self.master)
        self.slaves[slave.slave_id] = slave
        
        print(f"Slave node added: {slave.slave_id}")
        return slave
    
    def remove_slave(self, slave_id: str):
        """移除从节点"""
        if slave_id in self.slaves:
            self.slaves[slave_id].stop_replication()
            del self.slaves[slave_id]
            print(f"Slave node removed: {slave_id}")
    
    def write(self, key: str, value: Any) -> bool:
        """写操作（总是路由到主节点）"""
        if not self.master:
            raise ValueError("No master node available")
        return self.master.put(key, value)
    
    def read(self, key: str) -> Optional[Any]:
        """读操作（根据读偏好路由）"""
        if self.read_preference == "primary" or not self.slaves:
            return self.master.get(key) if self.master else None
        
        elif self.read_preference == "secondary":
            # 选择最健康的从节点
            healthy_slaves = [
                slave for slave in self.slaves.values()
                if slave.status == ReplicationStatus.HEALTHY
            ]
            
            if healthy_slaves:
                # 选择延迟最小的从节点
                best_slave = min(healthy_slaves, key=lambda s: s.lag_ms)
                return best_slave.get(key)
            else:
                # 回退到主节点
                return self.master.get(key) if self.master else None
        
        # 其他读偏好...
        return self.master.get(key) if self.master else None
    
    def delete(self, key: str) -> bool:
        """删除操作（总是路由到主节点）"""
        if not self.master:
            raise ValueError("No master node available")
        return self.master.delete(key)
    
    def get_cluster_status(self) -> Dict[str, Any]:
        """获取集群状态"""
        status = {
            "master": self.master.get_replication_status() if self.master else None,
            "slaves": {},
            "total_slaves": len(self.slaves),
            "healthy_slaves": 0,
            "average_lag_ms": 0.0
        }
        
        if self.slaves:
            total_lag = 0
            for slave_id, slave in self.slaves.items():
                slave_status = slave.get_slave_status()
                status["slaves"][slave_id] = slave_status
                
                if slave_status["status"] == "healthy":
                    status["healthy_slaves"] += 1
                
                total_lag += slave_status["lag_ms"]
            
            status["average_lag_ms"] = total_lag / len(self.slaves)
        
        return status
    
    def promote_slave_to_master(self, slave_id: str) -> bool:
        """提升从节点为主节点（故障转移）"""
        if slave_id not in self.slaves:
            return False
        
        print(f"Promoting slave {slave_id} to master...")
        
        # 停止当前主节点
        if self.master:
            print("Stopping current master...")
        
        # 选择的从节点成为新主节点
        promoted_slave = self.slaves[slave_id]
        promoted_slave.stop_replication()
        
        # 创建新的主节点，使用从节点的数据
        new_master = ReplicationMaster(promoted_slave.slave_id)
        new_master.data = promoted_slave.data.copy()
        new_master.current_lsn = promoted_slave.last_applied_lsn
        
        # 更新系统状态
        self.master = new_master
        del self.slaves[slave_id]
        
        # 重新配置其他从节点
        for slave in self.slaves.values():
            slave.stop_replication()
            slave.start_replication(new_master)
        
        print(f"Failover completed. New master: {new_master.node_id}")
        return True 