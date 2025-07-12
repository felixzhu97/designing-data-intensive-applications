"""
分布式事务处理
展示《数据密集型应用系统设计》中的分布式事务概念
"""

import threading
import time
import uuid
from enum import Enum
from typing import Dict, List, Set, Any, Optional, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from collections import defaultdict


class TransactionPhase(Enum):
    """事务阶段"""
    PHASE_1_PREPARE = "PHASE_1_PREPARE"
    PHASE_2_COMMIT = "PHASE_2_COMMIT"
    PHASE_2_ABORT = "PHASE_2_ABORT"


class ParticipantState(Enum):
    """参与者状态"""
    ACTIVE = "ACTIVE"
    PREPARED = "PREPARED"
    COMMITTED = "COMMITTED"
    ABORTED = "ABORTED"
    FAILED = "FAILED"


@dataclass
class DistributedTransaction:
    """分布式事务"""
    transaction_id: str
    coordinator_id: str
    participants: List[str]
    operations: List[Dict[str, Any]] = field(default_factory=list)
    state: ParticipantState = ParticipantState.ACTIVE
    created_at: datetime = field(default_factory=datetime.utcnow)
    timeout: float = 30.0  # 30秒超时
    
    def add_operation(self, participant_id: str, operation_type: str, resource_id: str, 
                     old_value: Any = None, new_value: Any = None):
        """添加操作"""
        operation = {
            'operation_id': str(uuid.uuid4()),
            'participant_id': participant_id,
            'operation_type': operation_type,
            'resource_id': resource_id,
            'old_value': old_value,
            'new_value': new_value,
            'timestamp': datetime.utcnow().isoformat()
        }
        self.operations.append(operation)
    
    def is_expired(self) -> bool:
        """检查是否过期"""
        return (datetime.utcnow() - self.created_at).total_seconds() > self.timeout


class TransactionParticipant:
    """事务参与者"""
    
    def __init__(self, participant_id: str):
        self.participant_id = participant_id
        self.local_transactions: Dict[str, Dict[str, Any]] = {}
        self.data_store: Dict[str, Any] = {}
        self.lock = threading.RLock()
    
    def prepare(self, transaction_id: str, operations: List[Dict[str, Any]]) -> bool:
        """准备阶段"""
        with self.lock:
            try:
                # 验证所有操作
                for operation in operations:
                    if operation['participant_id'] != self.participant_id:
                        continue
                    
                    if not self._validate_operation(operation):
                        return False
                
                # 记录本地事务
                self.local_transactions[transaction_id] = {
                    'operations': operations,
                    'state': ParticipantState.PREPARED,
                    'prepared_at': datetime.utcnow()
                }
                
                return True
                
            except Exception as e:
                print(f"Prepare failed for transaction {transaction_id}: {e}")
                return False
    
    def commit(self, transaction_id: str) -> bool:
        """提交阶段"""
        with self.lock:
            if transaction_id not in self.local_transactions:
                return False
            
            try:
                tx_data = self.local_transactions[transaction_id]
                operations = tx_data['operations']
                
                # 应用所有操作
                for operation in operations:
                    if operation['participant_id'] != self.participant_id:
                        continue
                    
                    self._apply_operation(operation)
                
                # 更新事务状态
                tx_data['state'] = ParticipantState.COMMITTED
                tx_data['committed_at'] = datetime.utcnow()
                
                return True
                
            except Exception as e:
                print(f"Commit failed for transaction {transaction_id}: {e}")
                self.abort(transaction_id)
                return False
    
    def abort(self, transaction_id: str) -> bool:
        """中止阶段"""
        with self.lock:
            if transaction_id not in self.local_transactions:
                return False
            
            try:
                tx_data = self.local_transactions[transaction_id]
                tx_data['state'] = ParticipantState.ABORTED
                tx_data['aborted_at'] = datetime.utcnow()
                
                # 清理资源
                # 在实际实现中，这里会回滚所有已准备的操作
                
                return True
                
            except Exception as e:
                print(f"Abort failed for transaction {transaction_id}: {e}")
                return False
    
    def _validate_operation(self, operation: Dict[str, Any]) -> bool:
        """验证操作"""
        # 简化验证逻辑
        resource_id = operation['resource_id']
        operation_type = operation['operation_type']
        
        if operation_type == 'write':
            # 检查资源是否存在冲突
            return True
        elif operation_type == 'delete':
            # 检查资源是否存在
            return resource_id in self.data_store
        
        return True
    
    def _apply_operation(self, operation: Dict[str, Any]):
        """应用操作"""
        resource_id = operation['resource_id']
        operation_type = operation['operation_type']
        
        if operation_type == 'write':
            self.data_store[resource_id] = operation['new_value']
        elif operation_type == 'delete':
            self.data_store.pop(resource_id, None)
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """获取事务状态"""
        with self.lock:
            if transaction_id not in self.local_transactions:
                return None
            
            tx_data = self.local_transactions[transaction_id]
            return {
                'transaction_id': transaction_id,
                'participant_id': self.participant_id,
                'state': tx_data['state'].value,
                'operation_count': len(tx_data['operations']),
                'prepared_at': tx_data.get('prepared_at'),
                'committed_at': tx_data.get('committed_at'),
                'aborted_at': tx_data.get('aborted_at')
            }


class TransactionCoordinator:
    """事务协调器"""
    
    def __init__(self, coordinator_id: str):
        self.coordinator_id = coordinator_id
        self.participants: Dict[str, TransactionParticipant] = {}
        self.transactions: Dict[str, DistributedTransaction] = {}
        self.lock = threading.RLock()
    
    def register_participant(self, participant_id: str, participant: TransactionParticipant):
        """注册参与者"""
        with self.lock:
            self.participants[participant_id] = participant
    
    def begin_transaction(self, participant_ids: List[str]) -> str:
        """开始分布式事务"""
        with self.lock:
            transaction_id = str(uuid.uuid4())
            
            # 验证所有参与者都已注册
            for participant_id in participant_ids:
                if participant_id not in self.participants:
                    raise ValueError(f"Participant {participant_id} not registered")
            
            transaction = DistributedTransaction(
                transaction_id=transaction_id,
                coordinator_id=self.coordinator_id,
                participants=participant_ids
            )
            
            self.transactions[transaction_id] = transaction
            return transaction_id
    
    def add_operation(self, transaction_id: str, participant_id: str, 
                     operation_type: str, resource_id: str, 
                     old_value: Any = None, new_value: Any = None) -> bool:
        """添加操作"""
        with self.lock:
            transaction = self.transactions.get(transaction_id)
            if not transaction:
                return False
            
            if transaction.state != ParticipantState.ACTIVE:
                return False
            
            transaction.add_operation(participant_id, operation_type, resource_id, old_value, new_value)
            return True
    
    def commit_transaction(self, transaction_id: str) -> bool:
        """提交事务（两阶段提交）"""
        with self.lock:
            transaction = self.transactions.get(transaction_id)
            if not transaction:
                return False
            
            if transaction.state != ParticipantState.ACTIVE:
                return False
            
            # 检查超时
            if transaction.is_expired():
                self.abort_transaction(transaction_id)
                return False
            
            # 第一阶段：准备
            if not self._phase1_prepare(transaction):
                self.abort_transaction(transaction_id)
                return False
            
            # 第二阶段：提交
            if not self._phase2_commit(transaction):
                self.abort_transaction(transaction_id)
                return False
            
            transaction.state = ParticipantState.COMMITTED
            return True
    
    def abort_transaction(self, transaction_id: str) -> bool:
        """中止事务"""
        with self.lock:
            transaction = self.transactions.get(transaction_id)
            if not transaction:
                return False
            
            # 通知所有参与者中止
            for participant_id in transaction.participants:
                participant = self.participants.get(participant_id)
                if participant:
                    participant.abort(transaction_id)
            
            transaction.state = ParticipantState.ABORTED
            return True
    
    def _phase1_prepare(self, transaction: DistributedTransaction) -> bool:
        """第一阶段：准备"""
        # 按参与者分组操作
        participant_operations = defaultdict(list)
        for operation in transaction.operations:
            participant_operations[operation['participant_id']].append(operation)
        
        # 向所有参与者发送准备请求
        for participant_id in transaction.participants:
            participant = self.participants.get(participant_id)
            if not participant:
                return False
            
            operations = participant_operations.get(participant_id, [])
            if not participant.prepare(transaction.transaction_id, operations):
                return False
        
        return True
    
    def _phase2_commit(self, transaction: DistributedTransaction) -> bool:
        """第二阶段：提交"""
        # 向所有参与者发送提交请求
        for participant_id in transaction.participants:
            participant = self.participants.get(participant_id)
            if not participant:
                return False
            
            if not participant.commit(transaction.transaction_id):
                return False
        
        return True
    
    def get_transaction_status(self, transaction_id: str) -> Optional[Dict[str, Any]]:
        """获取事务状态"""
        with self.lock:
            transaction = self.transactions.get(transaction_id)
            if not transaction:
                return None
            
            # 获取所有参与者状态
            participant_statuses = []
            for participant_id in transaction.participants:
                participant = self.participants.get(participant_id)
                if participant:
                    status = participant.get_transaction_status(transaction_id)
                    if status:
                        participant_statuses.append(status)
            
            return {
                'transaction_id': transaction_id,
                'coordinator_id': self.coordinator_id,
                'state': transaction.state.value,
                'participants': transaction.participants,
                'operation_count': len(transaction.operations),
                'created_at': transaction.created_at.isoformat(),
                'is_expired': transaction.is_expired(),
                'participant_statuses': participant_statuses
            }
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        with self.lock:
            state_counts = defaultdict(int)
            for transaction in self.transactions.values():
                state_counts[transaction.state.value] += 1
            
            return {
                'total_transactions': len(self.transactions),
                'registered_participants': len(self.participants),
                'transaction_states': dict(state_counts),
                'coordinator_id': self.coordinator_id
            }


class TwoPhaseCommit:
    """两阶段提交协议"""
    
    def __init__(self):
        self.coordinators: Dict[str, TransactionCoordinator] = {}
        self.participants: Dict[str, TransactionParticipant] = {}
        self.lock = threading.RLock()
    
    def create_coordinator(self, coordinator_id: str) -> TransactionCoordinator:
        """创建协调器"""
        with self.lock:
            coordinator = TransactionCoordinator(coordinator_id)
            self.coordinators[coordinator_id] = coordinator
            return coordinator
    
    def create_participant(self, participant_id: str) -> TransactionParticipant:
        """创建参与者"""
        with self.lock:
            participant = TransactionParticipant(participant_id)
            self.participants[participant_id] = participant
            return participant
    
    def setup_distributed_transaction(self, coordinator_id: str, participant_ids: List[str]) -> str:
        """设置分布式事务"""
        with self.lock:
            coordinator = self.coordinators.get(coordinator_id)
            if not coordinator:
                raise ValueError(f"Coordinator {coordinator_id} not found")
            
            # 注册参与者
            for participant_id in participant_ids:
                participant = self.participants.get(participant_id)
                if not participant:
                    raise ValueError(f"Participant {participant_id} not found")
                coordinator.register_participant(participant_id, participant)
            
            # 开始事务
            return coordinator.begin_transaction(participant_ids)
    
    def get_system_status(self) -> Dict[str, Any]:
        """获取系统状态"""
        with self.lock:
            coordinator_stats = {}
            for coord_id, coordinator in self.coordinators.items():
                coordinator_stats[coord_id] = coordinator.get_statistics()
            
            return {
                'coordinators': len(self.coordinators),
                'participants': len(self.participants),
                'coordinator_stats': coordinator_stats
            }


# 并发控制相关类
class ConcurrencyControl:
    """并发控制"""
    
    def __init__(self):
        self.active_transactions: Set[str] = set()
        self.waiting_transactions: Dict[str, Set[str]] = defaultdict(set)  # tx_id -> waiting_for_tx_ids
        self.lock = threading.RLock()
    
    def add_transaction(self, transaction_id: str):
        """添加事务"""
        with self.lock:
            self.active_transactions.add(transaction_id)
    
    def remove_transaction(self, transaction_id: str):
        """移除事务"""
        with self.lock:
            self.active_transactions.discard(transaction_id)
            self.waiting_transactions.pop(transaction_id, None)
            
            # 移除对此事务的等待
            for waiting_set in self.waiting_transactions.values():
                waiting_set.discard(transaction_id)
    
    def add_wait_for(self, transaction_id: str, waiting_for_id: str):
        """添加等待关系"""
        with self.lock:
            self.waiting_transactions[transaction_id].add(waiting_for_id)
    
    def remove_wait_for(self, transaction_id: str, waiting_for_id: str):
        """移除等待关系"""
        with self.lock:
            self.waiting_transactions[transaction_id].discard(waiting_for_id)
    
    def detect_deadlock(self) -> List[List[str]]:
        """检测死锁"""
        with self.lock:
            # 使用DFS检测环
            visited = set()
            rec_stack = set()
            cycles = []
            
            def dfs(node: str, path: List[str]):
                if node in rec_stack:
                    # 找到环
                    cycle_start = path.index(node)
                    cycle = path[cycle_start:]
                    cycles.append(cycle)
                    return
                
                if node in visited:
                    return
                
                visited.add(node)
                rec_stack.add(node)
                path.append(node)
                
                # 访问所有等待的事务
                for waiting_for in self.waiting_transactions.get(node, set()):
                    dfs(waiting_for, path)
                
                path.pop()
                rec_stack.remove(node)
            
            # 对所有活跃事务进行DFS
            for tx_id in self.active_transactions:
                if tx_id not in visited:
                    dfs(tx_id, [])
            
            return cycles


class DeadlockDetector:
    """死锁检测器"""
    
    def __init__(self, concurrency_control: ConcurrencyControl):
        self.concurrency_control = concurrency_control
        self.detection_interval = 5.0  # 5秒检测一次
        self.running = False
        self.detection_thread = None
    
    def start(self):
        """启动死锁检测"""
        if self.running:
            return
        
        self.running = True
        self.detection_thread = threading.Thread(target=self._detection_loop, daemon=True)
        self.detection_thread.start()
    
    def stop(self):
        """停止死锁检测"""
        self.running = False
        if self.detection_thread:
            self.detection_thread.join()
    
    def _detection_loop(self):
        """检测循环"""
        while self.running:
            try:
                cycles = self.concurrency_control.detect_deadlock()
                if cycles:
                    self._handle_deadlock(cycles)
                
                time.sleep(self.detection_interval)
                
            except Exception as e:
                print(f"Deadlock detection error: {e}")
                time.sleep(1)
    
    def _handle_deadlock(self, cycles: List[List[str]]):
        """处理死锁"""
        for cycle in cycles:
            print(f"Deadlock detected: {' -> '.join(cycle)}")
            # 简化处理：中止最后一个事务
            if cycle:
                victim_tx = cycle[-1]
                print(f"Aborting transaction {victim_tx} to resolve deadlock")
                self.concurrency_control.remove_transaction(victim_tx)


class WaitForGraph:
    """等待图"""
    
    def __init__(self):
        self.graph: Dict[str, Set[str]] = defaultdict(set)
        self.lock = threading.RLock()
    
    def add_edge(self, from_tx: str, to_tx: str):
        """添加边"""
        with self.lock:
            self.graph[from_tx].add(to_tx)
    
    def remove_edge(self, from_tx: str, to_tx: str):
        """移除边"""
        with self.lock:
            self.graph[from_tx].discard(to_tx)
    
    def remove_node(self, tx_id: str):
        """移除节点"""
        with self.lock:
            # 移除所有指向该节点的边
            for from_tx in self.graph:
                self.graph[from_tx].discard(tx_id)
            
            # 移除该节点的所有边
            self.graph.pop(tx_id, None)
    
    def has_cycle(self) -> bool:
        """检查是否有环"""
        with self.lock:
            visited = set()
            rec_stack = set()
            
            def dfs(node: str) -> bool:
                if node in rec_stack:
                    return True
                if node in visited:
                    return False
                
                visited.add(node)
                rec_stack.add(node)
                
                for neighbor in self.graph.get(node, set()):
                    if dfs(neighbor):
                        return True
                
                rec_stack.remove(node)
                return False
            
            for node in self.graph:
                if node not in visited:
                    if dfs(node):
                        return True
            
            return False
    
    def get_graph_info(self) -> Dict[str, Any]:
        """获取图信息"""
        with self.lock:
            edge_count = sum(len(edges) for edges in self.graph.values())
            return {
                'nodes': len(self.graph),
                'edges': edge_count,
                'has_cycle': self.has_cycle()
            } 