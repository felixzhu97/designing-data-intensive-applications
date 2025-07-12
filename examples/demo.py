#!/usr/bin/env python3
"""
数据密集型应用系统设计 - 完整演示程序
展示书中所有核心概念的实际应用
"""

import asyncio
import time
import random
import json
from datetime import datetime, timedelta
from typing import List, Dict, Any

# 导入我们的模块
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from src.data_models.relational import User, Post, Follow, Like, Comment
from src.data_models.document import UserProfile, PostDocument, ActivityLog
from src.data_models.graph import UserNode, PostNode, FollowRelation, LikeRelation
from src.data_models.timeseries import UserActivity, SystemMetrics, EventLog
from src.storage.lsm_tree import LSMTree
from config.settings import settings


class DDIADemo:
    """数据密集型应用系统演示"""
    
    def __init__(self):
        self.users = []
        self.posts = []
        self.activities = []
        
    async def run_complete_demo(self):
        """运行完整演示"""
        print("🎯 数据密集型应用系统设计 - 完整案例演示")
        print("=" * 60)
        
        # 1. 数据模型演示
        await self.demo_data_models()
        
        # 2. 存储引擎演示
        await self.demo_storage_engines()
        
        # 3. 复制系统演示
        await self.demo_replication()
        
        # 4. 分区系统演示
        await self.demo_partitioning()
        
        # 5. 事务处理演示
        await self.demo_transactions()
        
        # 6. 批处理演示
        await self.demo_batch_processing()
        
        # 7. 流处理演示
        await self.demo_stream_processing()
        
        # 8. 系统监控演示
        await self.demo_monitoring()
        
        print("\n🎉 演示完成！")
        print("请查看各个组件的详细实现和配置")
    
    async def demo_data_models(self):
        """演示不同的数据模型"""
        print("\n📊 1. 数据模型演示")
        print("-" * 30)
        
        # 关系型数据模型
        print("🔗 关系型数据模型:")
        user1 = User(
            username="alice",
            email="alice@example.com",
            first_name="Alice",
            last_name="Smith"
        )
        print(f"  创建用户: {user1.username} ({user1.email})")
        
        post1 = Post(
            author_id=user1.id,
            title="我的第一篇帖子",
            content="这是一个展示关系型数据模型的示例帖子",
            tags=["demo", "关系型数据库"]
        )
        print(f"  创建帖子: {post1.title}")
        
        # 文档型数据模型
        print("\n📄 文档型数据模型:")
        user_profile = UserProfile(
            username="alice",
            email="alice@example.com",
            bio="数据工程师，热爱分布式系统",
            interests=["数据库", "分布式系统", "机器学习"],
            location={
                "name": "北京",
                "latitude": 39.9042,
                "longitude": 116.4074
            }
        )
        print(f"  创建用户档案: {user_profile.username}")
        print(f"  兴趣: {', '.join(user_profile.interests)}")
        
        post_doc = PostDocument(
            author_id=str(user1.id),
            author_username=user1.username,
            content="这是存储在文档数据库中的帖子",
            hashtags=[{"tag": "NoSQL", "start_index": 0, "end_index": 5}],
            metrics={
                "views": 100,
                "likes": 15,
                "comments": 3
            }
        )
        print(f"  创建帖子文档: {post_doc.content[:30]}...")
        
        # 图型数据模型
        print("\n🕸️ 图型数据模型:")
        user_node = UserNode(
            node_id=str(user1.id),
            username=user1.username,
            followers_count=150,
            following_count=200,
            influence_level="medium"
        )
        print(f"  创建用户节点: {user_node.username}")
        
        # 时序数据模型
        print("\n📈 时序数据模型:")
        activity = UserActivity(
            user_id=str(user1.id),
            action="view_post",
            target_type="post",
            target_id=str(post1.id),
            device_type="mobile",
            properties={"duration": 30, "scroll_depth": 0.8}
        )
        print(f"  记录用户活动: {activity.action}")
        
        system_metric = SystemMetrics(
            metric_name="api_response_time",
            metric_type="histogram",
            value=250.5,
            service="api-server",
            instance="api-01",
            unit="ms"
        )
        print(f"  记录系统指标: {system_metric.metric_name} = {system_metric.value}{system_metric.unit}")
    
    async def demo_storage_engines(self):
        """演示存储引擎"""
        print("\n💾 2. 存储引擎演示")
        print("-" * 30)
        
        # LSM-Tree演示
        print("🌲 LSM-Tree存储引擎:")
        lsm = LSMTree("/tmp/ddia_demo_lsm", memtable_size=1024)
        
        # 写入数据
        print("  写入数据...")
        for i in range(100):
            key = f"user:{i:03d}"
            value = {
                "name": f"User{i}",
                "email": f"user{i}@example.com",
                "created_at": datetime.utcnow().isoformat()
            }
            lsm.put(key, json.dumps(value))
        
        # 读取数据
        print("  读取数据...")
        user_data = lsm.get("user:050")
        if user_data:
            user_info = json.loads(user_data)
            print(f"    找到用户: {user_info['name']}")
        
        # 范围扫描
        print("  范围扫描...")
        count = 0
        for key, value in lsm.scan("user:010", "user:020"):
            count += 1
        print(f"    扫描到 {count} 条记录")
        
        # 显示统计信息
        stats = lsm.stats()
        print(f"  存储统计:")
        print(f"    MemTable大小: {stats['memtable_size']} bytes")
        print(f"    SSTable数量: {stats['sstables_count']}")
        print(f"    总存储大小: {stats['total_sstable_size']} bytes")
    
    async def demo_replication(self):
        """演示复制系统"""
        print("\n🔄 3. 复制系统演示")
        print("-" * 30)
        
        print("📋 主从复制模式:")
        print("  - 主节点: 处理所有写操作")
        print("  - 从节点: 处理读操作，异步复制")
        print("  - 复制延迟: < 100ms")
        
        # 模拟写操作到主节点
        master_data = {"user:001": "Alice", "user:002": "Bob"}
        print(f"  主节点写入: {len(master_data)} 条记录")
        
        # 模拟复制延迟
        await asyncio.sleep(0.05)  # 50ms延迟
        
        replica_data = master_data.copy()
        print(f"  从节点复制: {len(replica_data)} 条记录")
        print("  ✅ 复制完成")
        
        print("\n🌐 多主复制模式:")
        print("  - 地理分布式部署")
        print("  - 冲突检测和解决")
        print("  - 最终一致性")
        
        # 模拟冲突解决
        conflict_data = {
            "user:001": {"name": "Alice", "version": 1, "timestamp": time.time()},
            "user:001_conflict": {"name": "Alicia", "version": 1, "timestamp": time.time() + 1}
        }
        
        # 使用时间戳解决冲突（Last Write Wins）
        latest = max(conflict_data.values(), key=lambda x: x["timestamp"])
        print(f"  冲突解决: 选择最新版本 '{latest['name']}'")
    
    async def demo_partitioning(self):
        """演示分区系统"""
        print("\n🗂️ 4. 分区系统演示")
        print("-" * 30)
        
        print("🔢 哈希分区:")
        
        # 模拟用户数据分区
        users = [f"user{i:03d}" for i in range(1, 21)]
        partition_count = 4
        
        partitions = [[] for _ in range(partition_count)]
        
        for user in users:
            # 简单哈希分区
            partition_id = hash(user) % partition_count
            partitions[partition_id].append(user)
        
        for i, partition in enumerate(partitions):
            print(f"  分区 {i}: {len(partition)} 用户 - {partition[:3]}{'...' if len(partition) > 3 else ''}")
        
        print("\n📊 范围分区:")
        
        # 模拟时间范围分区
        now = datetime.utcnow()
        time_partitions = {
            "2024-01": [],
            "2024-02": [],
            "2024-03": [],
            "2024-04": []
        }
        
        # 模拟数据分布
        for month, partition in time_partitions.items():
            count = random.randint(100, 500)
            partition.extend([f"record_{i}" for i in range(count)])
        
        for month, partition in time_partitions.items():
            print(f"  {month}: {len(partition)} 条记录")
        
        print("\n🔍 分区查询演示:")
        print("  查询条件: user_id = 'user005'")
        target_partition = hash("user005") % partition_count
        print(f"  目标分区: {target_partition}")
        print("  ✅ 只需查询单个分区，避免全表扫描")
    
    async def demo_transactions(self):
        """演示事务处理"""
        print("\n💳 5. 事务处理演示")
        print("-" * 30)
        
        print("🔒 ACID特性演示:")
        
        # 模拟银行转账事务
        print("\n  💰 银行转账事务:")
        account_a = {"id": "A001", "balance": 1000}
        account_b = {"id": "B001", "balance": 500}
        transfer_amount = 200
        
        print(f"    转账前: A账户={account_a['balance']}, B账户={account_b['balance']}")
        
        # 开始事务
        print("    开始事务...")
        
        try:
            # 原子性: 要么全部成功，要么全部失败
            print("    1. 检查A账户余额...")
            if account_a["balance"] < transfer_amount:
                raise Exception("余额不足")
            
            print("    2. 从A账户扣款...")
            account_a["balance"] -= transfer_amount
            
            print("    3. 向B账户入账...")
            account_b["balance"] += transfer_amount
            
            # 一致性: 总金额保持不变
            total_before = 1000 + 500
            total_after = account_a["balance"] + account_b["balance"]
            assert total_before == total_after, "总金额不一致"
            
            print("    4. 提交事务...")
            print(f"    转账后: A账户={account_a['balance']}, B账户={account_b['balance']}")
            print("    ✅ 事务成功提交")
            
        except Exception as e:
            print(f"    ❌ 事务回滚: {e}")
            # 回滚操作
            account_a["balance"] = 1000
            account_b["balance"] = 500
        
        print("\n🔄 隔离级别演示:")
        isolation_levels = [
            "READ UNCOMMITTED - 可能出现脏读",
            "READ COMMITTED - 避免脏读，可能出现不可重复读",
            "REPEATABLE READ - 避免不可重复读，可能出现幻读",
            "SERIALIZABLE - 完全隔离，性能最低"
        ]
        
        for level in isolation_levels:
            print(f"    • {level}")
    
    async def demo_batch_processing(self):
        """演示批处理系统"""
        print("\n📦 6. 批处理系统演示")
        print("-" * 30)
        
        print("🗂️ MapReduce模式:")
        
        # 模拟日志分析任务
        log_data = [
            "2024-01-01 10:00:01 GET /api/users 200 120ms",
            "2024-01-01 10:00:02 POST /api/posts 201 250ms",
            "2024-01-01 10:00:03 GET /api/users 200 95ms",
            "2024-01-01 10:00:04 GET /api/posts 200 180ms",
            "2024-01-01 10:00:05 POST /api/users 400 50ms",
            "2024-01-01 10:00:06 GET /api/users 200 110ms",
        ]
        
        print(f"  输入数据: {len(log_data)} 条日志")
        
        # Map阶段: 提取状态码
        def map_status_codes(log_line):
            parts = log_line.split()
            if len(parts) >= 4:
                status_code = parts[3]
                return [(status_code, 1)]
            return []
        
        mapped_data = []
        for log_line in log_data:
            mapped_data.extend(map_status_codes(log_line))
        
        print(f"  Map输出: {len(mapped_data)} 个键值对")
        
        # Reduce阶段: 统计每个状态码的数量
        from collections import defaultdict
        status_counts = defaultdict(int)
        
        for status_code, count in mapped_data:
            status_counts[status_code] += count
        
        print("  Reduce结果:")
        for status_code, count in sorted(status_counts.items()):
            print(f"    {status_code}: {count} 次")
        
        print("\n📊 ETL流程演示:")
        
        # Extract: 提取数据
        raw_users = [
            {"id": 1, "name": "Alice", "email": "ALICE@EXAMPLE.COM", "age": "25"},
            {"id": 2, "name": "Bob", "email": "bob@example.com", "age": "30"},
            {"id": 3, "name": "", "email": "charlie@example.com", "age": "invalid"},
        ]
        print(f"  Extract: 提取 {len(raw_users)} 条原始数据")
        
        # Transform: 数据清洗和转换
        cleaned_users = []
        for user in raw_users:
            if user["name"]:  # 过滤空名称
                try:
                    cleaned_user = {
                        "id": user["id"],
                        "name": user["name"].title(),
                        "email": user["email"].lower(),
                        "age": int(user["age"]) if user["age"].isdigit() else None
                    }
                    cleaned_users.append(cleaned_user)
                except ValueError:
                    continue  # 跳过无效数据
        
        print(f"  Transform: 清洗后 {len(cleaned_users)} 条有效数据")
        
        # Load: 加载到目标系统
        print("  Load: 数据已加载到数据仓库")
        for user in cleaned_users:
            print(f"    {user}")
    
    async def demo_stream_processing(self):
        """演示流处理系统"""
        print("\n🌊 7. 流处理系统演示")
        print("-" * 30)
        
        print("📡 实时事件流处理:")
        
        # 模拟实时事件流
        events = [
            {"timestamp": time.time(), "user_id": "user1", "action": "login", "value": 1},
            {"timestamp": time.time() + 1, "user_id": "user2", "action": "view_post", "value": 1},
            {"timestamp": time.time() + 2, "user_id": "user1", "action": "like_post", "value": 1},
            {"timestamp": time.time() + 3, "user_id": "user3", "action": "login", "value": 1},
            {"timestamp": time.time() + 4, "user_id": "user2", "action": "comment", "value": 1},
        ]
        
        print(f"  接收事件流: {len(events)} 个事件")
        
        # 实时统计: 5秒窗口内的活跃用户数
        window_size = 5  # 5秒窗口
        current_time = time.time()
        
        # 时间窗口过滤
        window_events = [
            event for event in events 
            if current_time - event["timestamp"] <= window_size
        ]
        
        # 统计活跃用户
        active_users = set(event["user_id"] for event in window_events)
        print(f"  5秒窗口内活跃用户: {len(active_users)} 人")
        
        # 按动作类型统计
        action_counts = defaultdict(int)
        for event in window_events:
            action_counts[event["action"]] += 1
        
        print("  动作统计:")
        for action, count in action_counts.items():
            print(f"    {action}: {count} 次")
        
        print("\n⚡ 复杂事件处理 (CEP):")
        
        # 检测用户行为模式: 登录后5分钟内有互动
        user_sessions = defaultdict(list)
        for event in events:
            user_sessions[event["user_id"]].append(event)
        
        engaged_users = []
        for user_id, user_events in user_sessions.items():
            # 按时间排序
            user_events.sort(key=lambda x: x["timestamp"])
            
            # 查找登录事件
            login_events = [e for e in user_events if e["action"] == "login"]
            interaction_events = [e for e in user_events if e["action"] in ["like_post", "comment"]]
            
            for login in login_events:
                # 检查登录后5分钟内是否有互动
                interactions_after_login = [
                    e for e in interaction_events 
                    if e["timestamp"] > login["timestamp"] and 
                       e["timestamp"] - login["timestamp"] <= 300  # 5分钟
                ]
                
                if interactions_after_login:
                    engaged_users.append(user_id)
                    break
        
        print(f"  高参与度用户: {len(set(engaged_users))} 人")
    
    async def demo_monitoring(self):
        """演示系统监控"""
        print("\n📊 8. 系统监控演示")
        print("-" * 30)
        
        print("📈 实时指标监控:")
        
        # 模拟系统指标
        metrics = {
            "api_requests_per_second": random.randint(100, 500),
            "cpu_usage_percent": random.uniform(20, 80),
            "memory_usage_percent": random.uniform(40, 90),
            "disk_io_ops_per_second": random.randint(50, 200),
            "network_bytes_per_second": random.randint(1000, 10000),
            "active_connections": random.randint(50, 500),
            "response_time_p99_ms": random.uniform(100, 1000),
            "error_rate_percent": random.uniform(0, 5)
        }
        
        print("  当前系统指标:")
        for metric, value in metrics.items():
            if "percent" in metric:
                status = "🟢" if value < 70 else "🟡" if value < 90 else "🔴"
                print(f"    {status} {metric}: {value:.1f}%")
            elif "response_time" in metric:
                status = "🟢" if value < 500 else "🟡" if value < 1000 else "🔴"
                print(f"    {status} {metric}: {value:.1f}ms")
            elif "error_rate" in metric:
                status = "🟢" if value < 1 else "🟡" if value < 3 else "🔴"
                print(f"    {status} {metric}: {value:.2f}%")
            else:
                print(f"    📊 {metric}: {value}")
        
        print("\n🚨 告警规则:")
        alerts = []
        
        if metrics["cpu_usage_percent"] > 80:
            alerts.append("CPU使用率过高")
        
        if metrics["memory_usage_percent"] > 85:
            alerts.append("内存使用率过高")
        
        if metrics["response_time_p99_ms"] > 1000:
            alerts.append("响应时间过长")
        
        if metrics["error_rate_percent"] > 3:
            alerts.append("错误率过高")
        
        if alerts:
            print("  🚨 触发告警:")
            for alert in alerts:
                print(f"    • {alert}")
        else:
            print("  ✅ 系统运行正常")
        
        print("\n📊 业务指标:")
        business_metrics = {
            "daily_active_users": random.randint(1000, 5000),
            "posts_created_today": random.randint(100, 1000),
            "user_engagement_rate": random.uniform(0.1, 0.8),
            "revenue_today_usd": random.uniform(1000, 10000)
        }
        
        for metric, value in business_metrics.items():
            if "rate" in metric:
                print(f"    📈 {metric}: {value:.2%}")
            elif "usd" in metric:
                print(f"    💰 {metric}: ${value:.2f}")
            else:
                print(f"    👥 {metric}: {value:,}")


async def main():
    """主函数"""
    demo = DDIADemo()
    await demo.run_complete_demo()
    
    print("\n" + "=" * 60)
    print("📚 更多信息:")
    print("  • 查看源代码: src/ 目录")
    print("  • 阅读文档: docs/ 目录")
    print("  • 运行测试: pytest tests/")
    print("  • 性能测试: python tests/performance/benchmark.py")
    print("\n💡 建议:")
    print("  1. 启动完整系统: ./scripts/start_services.sh")
    print("  2. 初始化数据库: python src/setup/init_database.py")
    print("  3. 启动API服务: python src/api/main.py")
    print("  4. 访问监控面板: http://localhost:3000")


if __name__ == "__main__":
    asyncio.run(main()) 