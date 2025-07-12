"""
ACID事务处理系统
展示《数据密集型应用系统设计》中的ACID特性
"""

import threading
import time
import uuid
from enum import Enum
from typing import Dict, List, Any, Optional, Callable, Set
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field


class TransactionState(Enum):
    """事务状态"""
    ACTIVE = "active"
    PREPARED = "prepared"
    COMMITTED = "committed"
    ABORTED = "aborted"


class LockType(Enum):
    """锁类型"""
    SHARED = "shared"      # 共享锁（读锁）
    EXCLUSIVE = "exclusive"  # 排他锁（写锁）


@dataclass
class TransactionOperation:
    """事务操作"""
    operation_id: str
    operation_type: str  # 'read', 'write', 'delete'
    resource_id: str
    old_value: Any = None
    new_value: Any = None
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'operation_id': self.operation_id,
            'operation_type': self.operation_type,
            'resource_id': self.resource_id,
            'old_value': self.old_value,
            'new_value': self.new_value,
            'timestamp': self.timestamp.isoformat()
        }


@dataclass
class Lock:
    """锁对象"""
    resource_id: str
    lock_type: LockType
    transaction_id: str
    granted_at: datetime = field(default_factory=datetime.utcnow)
    
    def is_compatible(self, other_lock_type: LockType) -> bool:
        """检查锁的兼容性"""
        # 共享锁之间兼容，排他锁与任何锁都不兼容
        if self.lock_type == LockType.SHARED and other_lock_type == LockType.SHARED:
            return True
        return False


class LockManager:
    """锁管理器"""
    
    def __init__(self):
        self.locks: Dict[str, List[Lock]] = defaultdict(list)  # resource_id -> locks
        self.waiting_queue: Dict[str, List[Dict[str, Any]]] = defaultdict(list)  # resource_id -> waiting requests
        self.lock_table: Dict[str, Set[str]] = defaultdict(set)  # transaction_id -> resource_ids
        self.lock = threading.RLock()
    
    def acquire_lock(self, transaction_id: str, resource_id: str, lock_type: LockType, timeout: float = 5.0) -> bool:
        """获取锁"""
        with self.lock:
            # 检查是否可以立即获取锁
            if self._can_acquire_lock(resource_id, lock_type, transaction_id):
                lock = Lock(resource_id, lock_type, transaction_id)
                self.locks[resource_id].append(lock)
                self.lock_table[transaction_id].add(resource_id)
                return True
            
            # 如果不能立即获取，添加到等待队列
            wait_request = {
                'transaction_id': transaction_id,
                'lock_type': lock_type,
                'requested_at': datetime.utcnow(),
                'timeout': timeout
            }
            self.waiting_queue[resource_id].append(wait_request)
            
            # 等待锁可用（简化实现：直接返回False）
            return False
    
    def _can_acquire_lock(self, resource_id: str, lock_type: LockType, transaction_id: str) -> bool:
        """检查是否可以获取锁"""
        existing_locks = self.locks[resource_id]
        
        # 如果没有现有锁，可以获取
        if not existing_locks:
            return True
        
        # 检查与现有锁的兼容性
        for existing_lock in existing_locks:
            # 同一个事务可以重复获取锁
            if existing_lock.transaction_id == transaction_id:
                continue
            
            # 检查锁兼容性
            if not existing_lock.is_compatible(lock_type):
                return False
        
        return True
    
    def release_lock(self, transaction_id: str, resource_id: str):
        """释放锁"""
        with self.lock:
            # 移除锁
            self.locks[resource_id] = [
                lock for lock in self.locks[resource_id] 
                if lock.transaction_id != transaction_id
            ]
            
            # 从锁表中移除
            self.lock_table[transaction_id].discard(resource_id)
            
            # 处理等待队列
            self._process_waiting_queue(resource_id)
    
    def release_all_locks(self, transaction_id: str):
        """释放事务的所有锁"""
        with self.lock:
            resource_ids = self.lock_table[transaction_id].copy()
            for resource_id in resource_ids:
                self.release_lock(transaction_id, resource_id)
    
    def _process_waiting_queue(self, resource_id: str):
        """处理等待队列"""
        waiting_requests = self.waiting_queue[resource_id]
        if not waiting_requests:
            return
        
        # 尝试授予等待中的锁
        granted_requests = []
        for request in waiting_requests:
            if self._can_acquire_lock(resource_id, request['lock_type'], request['transaction_id']):
                lock = Lock(resource_id, request['lock_type'], request['transaction_id'])
                self.locks[resource_id].append(lock)
                self.lock_table[request['transaction_id']].add(resource_id)
                granted_requests.append(request)
        
        # 从等待队列中移除已授予的请求
        for request in granted_requests:
            waiting_requests.remove(request)
    
    def get_lock_info(self, resource_id: str) -> Dict[str, Any]:
        """获取锁信息"""
        with self.lock:
            locks = self.locks[resource_id]
            waiting = self.waiting_queue[resource_id]
            
            return {
                'resource_id': resource_id,
                'active_locks': [
                    {
                        'transaction_id': lock.transaction_id,
                        'lock_type': lock.lock_type.value,
                        'granted_at': lock.granted_at.isoformat()
                    } for lock in locks
                ],
                'waiting_requests': [
                    {
                        'transaction_id': req['transaction_id'],
                        'lock_type': req['lock_type'].value,
                        'requested_at': req['requested_at'].isoformat()
                    } for req in waiting
                ]
            }


class TransactionLog:
    """事务日志"""
    
    def __init__(self):
        self.log_entries: List[Dict[str, Any]] = []
        self.lock = threading.RLock()
    
    def log_operation(self, transaction_id: str, operation: TransactionOperation):
        """记录操作"""
        with self.lock:
            log_entry = {
                'transaction_id': transaction_id,
                'operation': operation.to_dict(),
                'logged_at': datetime.utcnow().isoformat()
            }
            self.log_entries.append(log_entry)
    
    def log_transaction_event(self, transaction_id: str, event_type: str, details: Dict[str, Any] = None):
        """记录事务事件"""
        with self.lock:
            log_entry = {
                'transaction_id': transaction_id,
                'event_type': event_type,
                'details': details or {},
                'logged_at': datetime.utcnow().isoformat()
            }
            self.log_entries.append(log_entry)
    
    def get_transaction_log(self, transaction_id: str) -> List[Dict[str, Any]]:
        """获取事务日志"""
        with self.lock:
            return [
                entry for entry in self.log_entries 
                if entry.get('transaction_id') == transaction_id
            ]
    
    def get_all_logs(self, limit: int = 100) -> List[Dict[str, Any]]:
        """获取所有日志"""
        with self.lock:
            return self.log_entries[-limit:]


class Transaction:
    """事务"""
    
    def __init__(self, transaction_id: str, isolation_level: str = "READ_COMMITTED"):
        self.transaction_id = transaction_id
        self.isolation_level = isolation_level
        self.state = TransactionState.ACTIVE
        self.operations: List[TransactionOperation] = []
        self.read_set: Set[str] = set()  # 读取的资源
        self.write_set: Set[str] = set()  # 写入的资源
        self.start_time = datetime.utcnow()
        self.end_time: Optional[datetime] = None
        self.lock = threading.RLock()
    
    def add_operation(self, operation: TransactionOperation):
        """添加操作"""
        with self.lock:
            self.operations.append(operation)
            
            if operation.operation_type == 'read':
                self.read_set.add(operation.resource_id)
            elif operation.operation_type in ['write', 'delete']:
                self.write_set.add(operation.resource_id)
    
    def prepare(self) -> bool:
        """准备提交"""
        with self.lock:
            if self.state != TransactionState.ACTIVE:
                return False
            
            self.state = TransactionState.PREPARED
            return True
    
    def commit(self) -> bool:
        """提交事务"""
        with self.lock:
            if self.state not in [TransactionState.ACTIVE, TransactionState.PREPARED]:
                return False
            
            self.state = TransactionState.COMMITTED
            self.end_time = datetime.utcnow()
            return True
    
    def abort(self) -> bool:
        """中止事务"""
        with self.lock:
            if self.state == TransactionState.COMMITTED:
                return False
            
            self.state = TransactionState.ABORTED
            self.end_time = datetime.utcnow()
            return True
    
    def get_info(self) -> Dict[str, Any]:
        """获取事务信息"""
        with self.lock:
            return {
                'transaction_id': self.transaction_id,
                'isolation_level': self.isolation_level,
                'state': self.state.value,
                'operation_count': len(self.operations),
                'read_set_size': len(self.read_set),
                'write_set_size': len(self.write_set),
                'start_time': self.start_time.isoformat(),
                'end_time': self.end_time.isoformat() if self.end_time else None
            }


class TransactionManager:
    """事务管理器"""
    
    def __init__(self):
        self.transactions: Dict[str, Transaction] = {}
        self.lock_manager = LockManager()
        self.transaction_log = TransactionLog()
        self.data_store: Dict[str, Any] = {}  # 简化的数据存储
        self.lock = threading.RLock()
    
    def begin_transaction(self, isolation_level: str = "READ_COMMITTED") -> str:
        """开始事务"""
        with self.lock:
            transaction_id = str(uuid.uuid4())
            transaction = Transaction(transaction_id, isolation_level)
            self.transactions[transaction_id] = transaction
            
            # 记录事务开始
            self.transaction_log.log_transaction_event(
                transaction_id, 
                "BEGIN", 
                {'isolation_level': isolation_level}
            )
            
            return transaction_id
    
    def read(self, transaction_id: str, resource_id: str) -> Optional[Any]:
        """读取数据"""
        with self.lock:
            transaction = self.transactions.get(transaction_id)
            if not transaction or transaction.state != TransactionState.ACTIVE:
                return None
            
            # 获取共享锁
            if not self.lock_manager.acquire_lock(transaction_id, resource_id, LockType.SHARED):
                return None
            
            # 读取数据
            value = self.data_store.get(resource_id)
            
            # 记录操作
            operation = TransactionOperation(
                operation_id=str(uuid.uuid4()),
                operation_type='read',
                resource_id=resource_id,
                old_value=value
            )
            
            transaction.add_operation(operation)
            self.transaction_log.log_operation(transaction_id, operation)
            
            return value
    
    def write(self, transaction_id: str, resource_id: str, value: Any) -> bool:
        """写入数据"""
        with self.lock:
            transaction = self.transactions.get(transaction_id)
            if not transaction or transaction.state != TransactionState.ACTIVE:
                return False
            
            # 获取排他锁
            if not self.lock_manager.acquire_lock(transaction_id, resource_id, LockType.EXCLUSIVE):
                return False
            
            # 记录旧值
            old_value = self.data_store.get(resource_id)
            
            # 写入数据（在事务提交前不实际写入）
            operation = TransactionOperation(
                operation_id=str(uuid.uuid4()),
                operation_type='write',
                resource_id=resource_id,
                old_value=old_value,
                new_value=value
            )
            
            transaction.add_operation(operation)
            self.transaction_log.log_operation(transaction_id, operation)
            
            return True
    
    def commit(self, transaction_id: str) -> bool:
        """提交事务"""
        with self.lock:
            transaction = self.transactions.get(transaction_id)
            if not transaction:
                return False
            
            try:
                # 准备阶段
                if not transaction.prepare():
                    return False
                
                # 应用所有写操作
                for operation in transaction.operations:
                    if operation.operation_type == 'write':
                        self.data_store[operation.resource_id] = operation.new_value
                    elif operation.operation_type == 'delete':
                        self.data_store.pop(operation.resource_id, None)
                
                # 提交事务
                if transaction.commit():
                    # 记录提交事件
                    self.transaction_log.log_transaction_event(
                        transaction_id, 
                        "COMMIT", 
                        {'committed_operations': len(transaction.operations)}
                    )
                    
                    # 释放所有锁
                    self.lock_manager.release_all_locks(transaction_id)
                    
                    return True
                
                return False
                
            except Exception as e:
                # 发生错误时回滚
                self.rollback(transaction_id)
                return False
    
    def rollback(self, transaction_id: str) -> bool:
        """回滚事务"""
        with self.lock:
            transaction = self.transactions.get(transaction_id)
            if not transaction:
                return False
            
            # 中止事务
            if transaction.abort():
                # 记录回滚事件
                self.transaction_log.log_transaction_event(
                    transaction_id, 
                    "ROLLBACK", 
                    {'rolled_back_operations': len(transaction.operations)}
                )
                
                # 释放所有锁
                self.lock_manager.release_all_locks(transaction_id)
                
                return True
            
            return False
    
    def get_transaction_info(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """获取事务信息"""
        with self.lock:
            transaction = self.transactions.get(transaction_id)
            if not transaction:
                return None
            
            return transaction.get_info()
    
    def get_active_transactions(self) -> List[Dict[str, Any]]:
        """获取活跃事务"""
        with self.lock:
            return [
                transaction.get_info() 
                for transaction in self.transactions.values() 
                if transaction.state == TransactionState.ACTIVE
            ]
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            states = defaultdict(int)
            for transaction in self.transactions.values():
                states[transaction.state.value] += 1
            
            return {
                'total_transactions': len(self.transactions),
                'transaction_states': dict(states),
                'data_items': len(self.data_store),
                'active_locks': sum(len(locks) for locks in self.lock_manager.locks.values())
            } 