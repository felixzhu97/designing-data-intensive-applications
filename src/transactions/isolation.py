"""
事务隔离级别实现
展示《数据密集型应用系统设计》中的事务隔离概念
"""

from enum import Enum
from typing import Dict, List, Set, Any, Optional
import threading
from dataclasses import dataclass
from datetime import datetime


class IsolationLevel(Enum):
    """隔离级别"""
    READ_UNCOMMITTED = "READ_UNCOMMITTED"
    READ_COMMITTED = "READ_COMMITTED"
    REPEATABLE_READ = "REPEATABLE_READ"
    SERIALIZABLE = "SERIALIZABLE"


@dataclass
class ReadVersion:
    """读取版本"""
    transaction_id: str
    version: int
    value: Any
    timestamp: datetime
    committed: bool = False


class ReadUncommitted:
    """读未提交隔离级别"""
    
    def __init__(self):
        self.name = "READ_UNCOMMITTED"
        self.level = IsolationLevel.READ_UNCOMMITTED
    
    def can_read(self, transaction_id: str, resource_id: str, version: ReadVersion) -> bool:
        """检查是否可以读取"""
        # 读未提交：可以读取任何版本的数据
        return True
    
    def get_read_version(self, transaction_id: str, resource_id: str, versions: List[ReadVersion]) -> Optional[ReadVersion]:
        """获取可读取的版本"""
        # 返回最新版本
        return max(versions, key=lambda v: v.version) if versions else None


class ReadCommitted:
    """读已提交隔离级别"""
    
    def __init__(self):
        self.name = "READ_COMMITTED"
        self.level = IsolationLevel.READ_COMMITTED
    
    def can_read(self, transaction_id: str, resource_id: str, version: ReadVersion) -> bool:
        """检查是否可以读取"""
        # 读已提交：只能读取已提交的数据
        return version.committed or version.transaction_id == transaction_id
    
    def get_read_version(self, transaction_id: str, resource_id: str, versions: List[ReadVersion]) -> Optional[ReadVersion]:
        """获取可读取的版本"""
        # 返回最新的已提交版本
        committed_versions = [v for v in versions if v.committed or v.transaction_id == transaction_id]
        return max(committed_versions, key=lambda v: v.version) if committed_versions else None


class RepeatableRead:
    """可重复读隔离级别"""
    
    def __init__(self):
        self.name = "REPEATABLE_READ"
        self.level = IsolationLevel.REPEATABLE_READ
        self.read_snapshots: Dict[str, Dict[str, ReadVersion]] = {}  # transaction_id -> {resource_id: version}
        self.lock = threading.RLock()
    
    def can_read(self, transaction_id: str, resource_id: str, version: ReadVersion) -> bool:
        """检查是否可以读取"""
        with self.lock:
            # 如果之前已经读过这个资源，使用相同的版本
            if transaction_id in self.read_snapshots:
                if resource_id in self.read_snapshots[transaction_id]:
                    snapshot_version = self.read_snapshots[transaction_id][resource_id]
                    return version.version == snapshot_version.version
            
            # 第一次读取：只能读取已提交的数据
            return version.committed or version.transaction_id == transaction_id
    
    def get_read_version(self, transaction_id: str, resource_id: str, versions: List[ReadVersion]) -> Optional[ReadVersion]:
        """获取可读取的版本"""
        with self.lock:
            # 如果之前已经读过这个资源，返回快照版本
            if transaction_id in self.read_snapshots:
                if resource_id in self.read_snapshots[transaction_id]:
                    return self.read_snapshots[transaction_id][resource_id]
            
            # 第一次读取：获取最新的已提交版本
            committed_versions = [v for v in versions if v.committed or v.transaction_id == transaction_id]
            if not committed_versions:
                return None
            
            selected_version = max(committed_versions, key=lambda v: v.version)
            
            # 保存快照
            if transaction_id not in self.read_snapshots:
                self.read_snapshots[transaction_id] = {}
            self.read_snapshots[transaction_id][resource_id] = selected_version
            
            return selected_version
    
    def clear_snapshot(self, transaction_id: str):
        """清除快照"""
        with self.lock:
            self.read_snapshots.pop(transaction_id, None)


class Serializable:
    """可串行化隔离级别"""
    
    def __init__(self):
        self.name = "SERIALIZABLE"
        self.level = IsolationLevel.SERIALIZABLE
        self.read_snapshots: Dict[str, Dict[str, ReadVersion]] = {}
        self.write_sets: Dict[str, Set[str]] = {}  # transaction_id -> {resource_ids}
        self.read_sets: Dict[str, Set[str]] = {}   # transaction_id -> {resource_ids}
        self.lock = threading.RLock()
    
    def can_read(self, transaction_id: str, resource_id: str, version: ReadVersion) -> bool:
        """检查是否可以读取"""
        with self.lock:
            # 检查是否存在写冲突
            for other_tx_id, write_set in self.write_sets.items():
                if other_tx_id != transaction_id and resource_id in write_set:
                    return False
            
            # 可重复读的逻辑
            if transaction_id in self.read_snapshots:
                if resource_id in self.read_snapshots[transaction_id]:
                    snapshot_version = self.read_snapshots[transaction_id][resource_id]
                    return version.version == snapshot_version.version
            
            return version.committed or version.transaction_id == transaction_id
    
    def can_write(self, transaction_id: str, resource_id: str) -> bool:
        """检查是否可以写入"""
        with self.lock:
            # 检查是否存在读写冲突
            for other_tx_id, read_set in self.read_sets.items():
                if other_tx_id != transaction_id and resource_id in read_set:
                    return False
            
            # 检查是否存在写写冲突
            for other_tx_id, write_set in self.write_sets.items():
                if other_tx_id != transaction_id and resource_id in write_set:
                    return False
            
            return True
    
    def get_read_version(self, transaction_id: str, resource_id: str, versions: List[ReadVersion]) -> Optional[ReadVersion]:
        """获取可读取的版本"""
        with self.lock:
            # 记录读集合
            if transaction_id not in self.read_sets:
                self.read_sets[transaction_id] = set()
            self.read_sets[transaction_id].add(resource_id)
            
            # 可重复读的逻辑
            if transaction_id in self.read_snapshots:
                if resource_id in self.read_snapshots[transaction_id]:
                    return self.read_snapshots[transaction_id][resource_id]
            
            committed_versions = [v for v in versions if v.committed or v.transaction_id == transaction_id]
            if not committed_versions:
                return None
            
            selected_version = max(committed_versions, key=lambda v: v.version)
            
            # 保存快照
            if transaction_id not in self.read_snapshots:
                self.read_snapshots[transaction_id] = {}
            self.read_snapshots[transaction_id][resource_id] = selected_version
            
            return selected_version
    
    def record_write(self, transaction_id: str, resource_id: str):
        """记录写操作"""
        with self.lock:
            if transaction_id not in self.write_sets:
                self.write_sets[transaction_id] = set()
            self.write_sets[transaction_id].add(resource_id)
    
    def clear_transaction(self, transaction_id: str):
        """清除事务数据"""
        with self.lock:
            self.read_snapshots.pop(transaction_id, None)
            self.write_sets.pop(transaction_id, None)
            self.read_sets.pop(transaction_id, None)
    
    def detect_conflicts(self, transaction_id: str) -> List[str]:
        """检测冲突"""
        with self.lock:
            conflicts = []
            
            # 检查读写冲突
            read_set = self.read_sets.get(transaction_id, set())
            for other_tx_id, write_set in self.write_sets.items():
                if other_tx_id != transaction_id:
                    conflict_resources = read_set & write_set
                    if conflict_resources:
                        conflicts.append(f"Read-Write conflict with {other_tx_id}: {conflict_resources}")
            
            # 检查写读冲突
            write_set = self.write_sets.get(transaction_id, set())
            for other_tx_id, other_read_set in self.read_sets.items():
                if other_tx_id != transaction_id:
                    conflict_resources = write_set & other_read_set
                    if conflict_resources:
                        conflicts.append(f"Write-Read conflict with {other_tx_id}: {conflict_resources}")
            
            return conflicts 