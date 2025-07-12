#!/usr/bin/env python3
"""
数据密集型应用系统设计 - 简化演示程序
展示核心概念，不依赖外部库
"""

import time
import json
import random
from datetime import datetime, timedelta
from collections import defaultdict, deque
import threading

print("🎯 数据密集型应用系统设计 - 简化演示")
print("=" * 60)

class SimpleEvent:
    """简单事件类"""
    def __init__(self, event_type, data):
        self.event_id = f"event_{int(time.time() * 1000)}"
        self.event_type = event_type
        self.timestamp = time.time()
        self.data = data

class SimpleLSMTree:
    """简化版LSM-Tree"""
    def __init__(self):
        self.memtable = {}
        self.sstables = []
    
    def put(self, key, value):
        self.memtable[key] = value
        print(f"  LSM-Tree: 写入 {key} = {value}")
    
    def get(self, key):
        if key in self.memtable:
            return self.memtable[key]
        return None

class SimpleReplication:
    """简化版复制系统"""
    def __init__(self):
        self.master_data = {}
        self.slave_data = {}
        self.replication_log = []
    
    def write(self, key, value):
        # 写入主节点
        self.master_data[key] = value
        log_entry = {"operation": "write", "key": key, "value": value, "timestamp": time.time()}
        self.replication_log.append(log_entry)
        
        # 异步复制到从节点（模拟延迟）
        time.sleep(0.01)  # 10ms延迟
        self.slave_data[key] = value
        
        print(f"  复制: 主节点写入 {key}, 已复制到从节点")
    
    def read(self, from_slave=False):
        source = "从节点" if from_slave else "主节点"
        data_source = self.slave_data if from_slave else self.master_data
        print(f"  复制: 从{source}读取，数据量: {len(data_source)}")
        return data_source

class SimpleStreamProcessor:
    """简化版流处理器"""
    def __init__(self):
        self.events = deque()
        self.processed_count = 0
        self.is_running = False
    
    def publish_event(self, event_type, data):
        event = SimpleEvent(event_type, data)
        self.events.append(event)
        print(f"  流处理: 发布事件 {event_type}")
        return event
    
    def start_processing(self):
        self.is_running = True
        print("  流处理: 开始处理事件流")
        
        while self.events and self.processed_count < 5:  # 处理前5个事件作为演示
            event = self.events.popleft()
            self.process_event(event)
            self.processed_count += 1
            time.sleep(0.1)  # 模拟处理时间
    
    def process_event(self, event):
        print(f"    处理事件: {event.event_type} - {event.data}")

def demo_data_models():
    """演示数据模型"""
    print("\n📊 1. 数据模型演示")
    print("-" * 30)
    
    # 关系型数据
    print("🔗 关系型数据模型:")
    user = {
        "id": "user_001",
        "username": "alice",
        "email": "alice@example.com",
        "created_at": datetime.now().isoformat()
    }
    print(f"  用户: {user['username']} ({user['email']})")
    
    post = {
        "id": "post_001",
        "author_id": user["id"],
        "title": "我的第一篇帖子",
        "content": "这是关系型数据模型的示例",
        "tags": ["demo", "数据库"]
    }
    print(f"  帖子: {post['title']}")
    
    # 文档型数据
    print("\n📄 文档型数据模型:")
    user_profile = {
        "username": "alice",
        "bio": "数据工程师",
        "interests": ["数据库", "分布式系统"],
        "location": {"city": "北京", "coordinates": [39.9042, 116.4074]},
        "settings": {"theme": "dark", "notifications": True}
    }
    print(f"  用户档案: {user_profile['username']}")
    print(f"  兴趣: {', '.join(user_profile['interests'])}")
    
    # 时序数据
    print("\n📈 时序数据模型:")
    activities = [
        {"timestamp": time.time(), "user_id": "alice", "action": "login"},
        {"timestamp": time.time() + 1, "user_id": "alice", "action": "view_post"},
        {"timestamp": time.time() + 2, "user_id": "alice", "action": "like_post"}
    ]
    for activity in activities:
        print(f"  活动: {activity['action']} by {activity['user_id']}")

def demo_storage_engines():
    """演示存储引擎"""
    print("\n💾 2. 存储引擎演示")
    print("-" * 30)
    
    print("🌲 LSM-Tree存储引擎:")
    lsm = SimpleLSMTree()
    
    # 写入数据
    print("  写入测试数据...")
    for i in range(5):
        key = f"user:{i:03d}"
        value = {"name": f"User{i}", "score": random.randint(0, 100)}
        lsm.put(key, value)
    
    # 读取数据
    print("  读取数据...")
    test_key = "user:002"
    result = lsm.get(test_key)
    if result:
        print(f"    找到: {test_key} = {result}")
    
    print(f"  内存表大小: {len(lsm.memtable)} 条记录")

def demo_replication():
    """演示复制系统"""
    print("\n🔄 3. 复制系统演示")
    print("-" * 30)
    
    print("📋 主从复制:")
    replication = SimpleReplication()
    
    # 写入数据
    print("  写入数据到主节点...")
    test_data = {
        "user:alice": {"name": "Alice", "status": "active"},
        "user:bob": {"name": "Bob", "status": "active"},
        "user:charlie": {"name": "Charlie", "status": "inactive"}
    }
    
    for key, value in test_data.items():
        replication.write(key, value)
    
    # 从不同节点读取
    print("  从主节点读取:")
    master_data = replication.read(from_slave=False)
    
    print("  从从节点读取:")
    slave_data = replication.read(from_slave=True)
    
    print(f"  数据一致性: {'✅ 一致' if len(master_data) == len(slave_data) else '❌ 不一致'}")

def demo_partitioning():
    """演示分区系统"""
    print("\n🗂️ 4. 分区系统演示")
    print("-" * 30)
    
    print("🔢 哈希分区:")
    
    # 模拟用户数据分区
    users = [f"user{i:03d}" for i in range(1, 21)]
    partition_count = 4
    partitions = [[] for _ in range(partition_count)]
    
    for user in users:
        partition_id = hash(user) % partition_count
        partitions[partition_id].append(user)
    
    for i, partition in enumerate(partitions):
        print(f"  分区 {i}: {len(partition)} 用户")
    
    print("\n📊 负载均衡检查:")
    sizes = [len(p) for p in partitions]
    balance_score = 1 - (max(sizes) - min(sizes)) / max(sizes)
    print(f"  负载均衡度: {balance_score:.2%}")

def demo_stream_processing():
    """演示流处理"""
    print("\n🌊 5. 流处理系统演示")
    print("-" * 30)
    
    print("📡 实时事件流:")
    processor = SimpleStreamProcessor()
    
    # 发布各种事件
    events_to_publish = [
        ("user_login", {"user_id": "alice", "timestamp": time.time()}),
        ("page_view", {"user_id": "alice", "page": "/home"}),
        ("post_like", {"user_id": "alice", "post_id": "post_123"}),
        ("user_logout", {"user_id": "alice", "duration": 1800}),
        ("system_alert", {"component": "database", "level": "warning"})
    ]
    
    print("  发布事件...")
    for event_type, data in events_to_publish:
        processor.publish_event(event_type, data)
    
    print("  处理事件流...")
    processor.start_processing()
    
    print(f"  处理完成: {processor.processed_count} 个事件")

def demo_consistency():
    """演示一致性模型"""
    print("\n💳 6. 一致性模型演示")
    print("-" * 30)
    
    print("🔒 ACID事务模拟:")
    
    # 模拟银行转账
    accounts = {"alice": 1000, "bob": 500}
    transfer_amount = 200
    
    print(f"  转账前: Alice={accounts['alice']}, Bob={accounts['bob']}")
    
    # 原子性操作
    print("  执行转账事务...")
    try:
        if accounts["alice"] >= transfer_amount:
            accounts["alice"] -= transfer_amount
            accounts["bob"] += transfer_amount
            print("  ✅ 事务提交成功")
        else:
            print("  ❌ 余额不足，事务回滚")
    except Exception as e:
        print(f"  ❌ 事务失败: {e}")
    
    print(f"  转账后: Alice={accounts['alice']}, Bob={accounts['bob']}")
    
    # 验证一致性
    total_before = 1000 + 500
    total_after = accounts["alice"] + accounts["bob"]
    print(f"  一致性检查: {'✅ 通过' if total_before == total_after else '❌ 失败'}")

def demo_monitoring():
    """演示系统监控"""
    print("\n📊 7. 系统监控演示")
    print("-" * 30)
    
    print("📈 模拟系统指标:")
    
    # 生成随机指标
    metrics = {
        "cpu_usage": random.uniform(10, 80),
        "memory_usage": random.uniform(30, 90),
        "disk_io": random.randint(100, 1000),
        "network_in": random.randint(1000, 10000),
        "network_out": random.randint(500, 5000),
        "active_connections": random.randint(50, 500),
        "response_time_ms": random.uniform(50, 500)
    }
    
    for metric, value in metrics.items():
        if "usage" in metric:
            status = "🟢" if value < 70 else "🟡" if value < 90 else "🔴"
            print(f"  {status} {metric}: {value:.1f}%")
        elif "time" in metric:
            status = "🟢" if value < 200 else "🟡" if value < 500 else "🔴"
            print(f"  {status} {metric}: {value:.1f}ms")
        else:
            print(f"  📊 {metric}: {value}")
    
    # 健康检查
    print("\n🏥 系统健康状态:")
    health_issues = []
    
    if metrics["cpu_usage"] > 80:
        health_issues.append("CPU使用率过高")
    if metrics["memory_usage"] > 85:
        health_issues.append("内存使用率过高")
    if metrics["response_time_ms"] > 1000:
        health_issues.append("响应时间过长")
    
    if health_issues:
        print("  🚨 发现问题:")
        for issue in health_issues:
            print(f"    • {issue}")
    else:
        print("  ✅ 系统运行正常")

def main():
    """主函数"""
    start_time = time.time()
    
    # 运行所有演示
    demo_data_models()
    demo_storage_engines()
    demo_replication()
    demo_partitioning()
    demo_stream_processing()
    demo_consistency()
    demo_monitoring()
    
    elapsed_time = time.time() - start_time
    
    print("\n" + "=" * 60)
    print("🎉 演示完成！")
    print(f"⏱️  总耗时: {elapsed_time:.2f} 秒")
    
    print("\n📚 核心概念总结:")
    concepts = [
        "数据模型: 关系型、文档型、图型、时序",
        "存储引擎: LSM-Tree、B-Tree、列式存储",
        "复制系统: 主从复制、多主复制、无主复制",
        "分区策略: 哈希分区、范围分区、一致性哈希",
        "事务处理: ACID特性、隔离级别、分布式事务",
        "流处理: 事件流、窗口操作、状态管理",
        "系统监控: 指标收集、健康检查、告警系统"
    ]
    
    for i, concept in enumerate(concepts, 1):
        print(f"  {i}. {concept}")
    
    print("\n💡 接下来的步骤:")
    print("  1. 安装完整依赖: pip install -r requirements.txt")
    print("  2. 启动基础设施: ./scripts/start_services.sh")
    print("  3. 运行完整演示: python examples/demo.py")
    print("  4. 启动API服务: python src/api/main.py")
    print("  5. 访问监控面板: http://localhost:3000")
    
    print("\n🔗 相关资源:")
    print("  • 《数据密集型应用系统设计》- Martin Kleppmann")
    print("  • 项目代码: src/ 目录")
    print("  • 配置文件: config/settings.py")
    print("  • 文档: README.md, QUICKSTART.md")

if __name__ == "__main__":
    main() 