"""
一致性哈希分区系统
展示《数据密集型应用系统设计》中的一致性哈希概念
"""

import hashlib
import bisect
import threading
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class VirtualNode:
    """虚拟节点"""
    node_id: str
    virtual_id: int
    hash_value: int
    data: Dict[str, Any] = field(default_factory=dict)
    stats: Dict[str, int] = field(default_factory=lambda: {
        'record_count': 0,
        'read_count': 0,
        'write_count': 0,
        'storage_size': 0
    })
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def put(self, key: str, value: Any):
        """存储键值对"""
        is_new = key not in self.data
        self.data[key] = value
        
        if is_new:
            self.stats['record_count'] += 1
        self.stats['write_count'] += 1
        self.stats['storage_size'] = len(self.data)
    
    def get(self, key: str) -> Optional[Any]:
        """获取键值对"""
        self.stats['read_count'] += 1
        return self.data.get(key)
    
    def delete(self, key: str) -> bool:
        """删除键值对"""
        if key in self.data:
            del self.data[key]
            self.stats['record_count'] -= 1
            self.stats['storage_size'] = len(self.data)
            return True
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """获取节点统计信息"""
        return {
            'node_id': self.node_id,
            'virtual_id': self.virtual_id,
            'hash_value': self.hash_value,
            'stats': self.stats.copy(),
            'created_at': self.created_at.isoformat()
        }


class ConsistentHashRing:
    """一致性哈希环"""
    
    def __init__(self, virtual_nodes_per_node: int = 256):
        self.virtual_nodes_per_node = virtual_nodes_per_node
        self.ring: Dict[int, VirtualNode] = {}  # hash -> virtual_node
        self.sorted_hashes: List[int] = []
        self.nodes: Dict[str, List[VirtualNode]] = {}  # node_id -> virtual_nodes
        self.lock = threading.RLock()
    
    def _hash(self, key: str) -> int:
        """计算键的哈希值"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**32)
    
    def add_node(self, node_id: str):
        """添加节点"""
        with self.lock:
            if node_id in self.nodes:
                return
            
            virtual_nodes = []
            
            # 为每个物理节点创建多个虚拟节点
            for i in range(self.virtual_nodes_per_node):
                virtual_key = f"{node_id}:{i}"
                hash_value = self._hash(virtual_key)
                
                virtual_node = VirtualNode(
                    node_id=node_id,
                    virtual_id=i,
                    hash_value=hash_value
                )
                
                self.ring[hash_value] = virtual_node
                virtual_nodes.append(virtual_node)
            
            self.nodes[node_id] = virtual_nodes
            self.sorted_hashes = sorted(self.ring.keys())
    
    def remove_node(self, node_id: str):
        """移除节点"""
        with self.lock:
            if node_id not in self.nodes:
                return
            
            # 移除虚拟节点
            virtual_nodes = self.nodes[node_id]
            for virtual_node in virtual_nodes:
                del self.ring[virtual_node.hash_value]
            
            del self.nodes[node_id]
            self.sorted_hashes = sorted(self.ring.keys())
    
    def get_node(self, key: str) -> Optional[VirtualNode]:
        """获取键对应的节点"""
        with self.lock:
            if not self.ring:
                return None
            
            key_hash = self._hash(key)
            
            # 找到第一个大于等于key_hash的虚拟节点
            idx = bisect.bisect_right(self.sorted_hashes, key_hash)
            
            # 如果没有找到，则使用第一个虚拟节点（环形结构）
            if idx == len(self.sorted_hashes):
                idx = 0
            
            chosen_hash = self.sorted_hashes[idx]
            return self.ring[chosen_hash]
    
    def get_nodes(self, key: str, count: int) -> List[VirtualNode]:
        """获取键对应的多个节点（用于复制）"""
        with self.lock:
            if not self.ring or count <= 0:
                return []
            
            key_hash = self._hash(key)
            idx = bisect.bisect_right(self.sorted_hashes, key_hash)
            
            nodes = []
            seen_physical_nodes = set()
            
            for i in range(len(self.sorted_hashes)):
                hash_idx = (idx + i) % len(self.sorted_hashes)
                chosen_hash = self.sorted_hashes[hash_idx]
                virtual_node = self.ring[chosen_hash]
                
                # 确保不选择同一个物理节点的多个虚拟节点
                if virtual_node.node_id not in seen_physical_nodes:
                    nodes.append(virtual_node)
                    seen_physical_nodes.add(virtual_node.node_id)
                    
                    if len(nodes) >= count:
                        break
            
            return nodes
    
    def get_ring_stats(self) -> Dict[str, Any]:
        """获取环的统计信息"""
        with self.lock:
            node_stats = defaultdict(lambda: {
                'virtual_node_count': 0,
                'total_records': 0,
                'total_reads': 0,
                'total_writes': 0,
                'total_storage': 0
            })
            
            for virtual_node in self.ring.values():
                node_id = virtual_node.node_id
                stats = virtual_node.stats
                
                node_stats[node_id]['virtual_node_count'] += 1
                node_stats[node_id]['total_records'] += stats['record_count']
                node_stats[node_id]['total_reads'] += stats['read_count']
                node_stats[node_id]['total_writes'] += stats['write_count']
                node_stats[node_id]['total_storage'] += stats['storage_size']
            
            return {
                'total_virtual_nodes': len(self.ring),
                'physical_nodes': len(self.nodes),
                'virtual_nodes_per_node': self.virtual_nodes_per_node,
                'node_stats': dict(node_stats)
            }


class ConsistentHashPartitioner:
    """一致性哈希分区器"""
    
    def __init__(self, virtual_nodes_per_node: int = 256, replication_factor: int = 3):
        self.ring = ConsistentHashRing(virtual_nodes_per_node)
        self.replication_factor = replication_factor
        self.lock = threading.RLock()
    
    def add_node(self, node_id: str):
        """添加节点"""
        self.ring.add_node(node_id)
    
    def remove_node(self, node_id: str):
        """移除节点"""
        self.ring.remove_node(node_id)
    
    def put(self, key: str, value: Any) -> bool:
        """存储键值对"""
        nodes = self.ring.get_nodes(key, self.replication_factor)
        
        if not nodes:
            return False
        
        # 写入到所有副本节点
        for node in nodes:
            node.put(key, value)
        
        return True
    
    def get(self, key: str) -> Optional[Any]:
        """获取键值对"""
        nodes = self.ring.get_nodes(key, self.replication_factor)
        
        if not nodes:
            return None
        
        # 从第一个可用节点读取
        for node in nodes:
            value = node.get(key)
            if value is not None:
                return value
        
        return None
    
    def delete(self, key: str) -> bool:
        """删除键值对"""
        nodes = self.ring.get_nodes(key, self.replication_factor)
        
        if not nodes:
            return False
        
        success = False
        # 从所有副本节点删除
        for node in nodes:
            if node.delete(key):
                success = True
        
        return success
    
    def get_stats(self) -> Dict[str, Any]:
        """获取分区器统计信息"""
        return self.ring.get_ring_stats()
    
    def rebalance(self) -> Dict[str, Any]:
        """重新平衡数据"""
        # 在实际实现中，这里会处理数据迁移
        # 简化版本只返回建议
        stats = self.get_stats()
        
        if not stats['node_stats']:
            return {'action': 'none', 'reason': 'No nodes available'}
        
        # 计算负载分布
        node_loads = {}
        total_records = 0
        
        for node_id, node_stat in stats['node_stats'].items():
            node_loads[node_id] = node_stat['total_records']
            total_records += node_stat['total_records']
        
        if total_records == 0:
            return {'action': 'none', 'reason': 'No data to rebalance'}
        
        # 计算平均负载
        avg_load = total_records / len(node_loads)
        
        # 找出负载不均衡的节点
        overloaded_nodes = []
        underloaded_nodes = []
        
        for node_id, load in node_loads.items():
            if load > avg_load * 1.5:  # 超过平均负载50%
                overloaded_nodes.append({'node_id': node_id, 'load': load})
            elif load < avg_load * 0.5:  # 低于平均负载50%
                underloaded_nodes.append({'node_id': node_id, 'load': load})
        
        rebalance_info = {
            'action': 'rebalance' if overloaded_nodes or underloaded_nodes else 'none',
            'total_records': total_records,
            'avg_load': avg_load,
            'overloaded_nodes': overloaded_nodes,
            'underloaded_nodes': underloaded_nodes
        }
        
        return rebalance_info


class PartitionManager:
    """分区管理器"""
    
    def __init__(self, partitioner: ConsistentHashPartitioner):
        self.partitioner = partitioner
        self.migration_log: List[Dict[str, Any]] = []
        self.lock = threading.RLock()
    
    def add_node(self, node_id: str) -> Dict[str, Any]:
        """添加新节点"""
        with self.lock:
            old_stats = self.partitioner.get_stats()
            
            self.partitioner.add_node(node_id)
            
            new_stats = self.partitioner.get_stats()
            
            migration_info = {
                'operation': 'add_node',
                'node_id': node_id,
                'timestamp': datetime.utcnow().isoformat(),
                'old_node_count': old_stats['physical_nodes'],
                'new_node_count': new_stats['physical_nodes']
            }
            
            self.migration_log.append(migration_info)
            
            return migration_info
    
    def remove_node(self, node_id: str) -> Dict[str, Any]:
        """移除节点"""
        with self.lock:
            old_stats = self.partitioner.get_stats()
            
            self.partitioner.remove_node(node_id)
            
            new_stats = self.partitioner.get_stats()
            
            migration_info = {
                'operation': 'remove_node',
                'node_id': node_id,
                'timestamp': datetime.utcnow().isoformat(),
                'old_node_count': old_stats['physical_nodes'],
                'new_node_count': new_stats['physical_nodes']
            }
            
            self.migration_log.append(migration_info)
            
            return migration_info
    
    def get_migration_log(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取迁移日志"""
        with self.lock:
            return self.migration_log[-limit:]
    
    def suggest_scaling(self) -> Dict[str, Any]:
        """建议扩缩容"""
        stats = self.partitioner.get_stats()
        
        if not stats['node_stats']:
            return {'action': 'none', 'reason': 'No nodes available'}
        
        # 计算总负载
        total_load = sum(node_stat['total_records'] for node_stat in stats['node_stats'].values())
        node_count = stats['physical_nodes']
        avg_load_per_node = total_load / node_count if node_count > 0 else 0
        
        # 负载阈值
        high_load_threshold = 10000  # 每个节点超过10000条记录
        low_load_threshold = 1000   # 每个节点少于1000条记录
        
        suggestions = []
        
        if avg_load_per_node > high_load_threshold:
            suggestions.append({
                'action': 'scale_out',
                'reason': f'Average load per node ({avg_load_per_node:.0f}) exceeds threshold ({high_load_threshold})',
                'recommended_nodes': max(1, int(total_load / high_load_threshold) - node_count)
            })
        
        elif avg_load_per_node < low_load_threshold and node_count > 1:
            suggestions.append({
                'action': 'scale_in',
                'reason': f'Average load per node ({avg_load_per_node:.0f}) is below threshold ({low_load_threshold})',
                'recommended_nodes': max(1, int(total_load / low_load_threshold))
            })
        
        return {
            'current_nodes': node_count,
            'total_load': total_load,
            'avg_load_per_node': avg_load_per_node,
            'suggestions': suggestions
        }


class PartitionRebalancer:
    """分区重平衡器"""
    
    def __init__(self, partitioner: ConsistentHashPartitioner):
        self.partitioner = partitioner
        self.rebalance_history: List[Dict[str, Any]] = []
        self.lock = threading.RLock()
    
    def analyze_balance(self) -> Dict[str, Any]:
        """分析负载均衡状况"""
        stats = self.partitioner.get_stats()
        
        if not stats['node_stats']:
            return {'balanced': True, 'reason': 'No nodes available'}
        
        # 计算负载统计
        loads = [node_stat['total_records'] for node_stat in stats['node_stats'].values()]
        
        if not loads:
            return {'balanced': True, 'reason': 'No data available'}
        
        total_load = sum(loads)
        avg_load = total_load / len(loads)
        max_load = max(loads)
        min_load = min(loads)
        
        # 计算负载不均衡系数
        if avg_load > 0:
            imbalance_ratio = (max_load - min_load) / avg_load
        else:
            imbalance_ratio = 0
        
        # 认为不均衡系数大于0.5就需要重平衡
        needs_rebalance = imbalance_ratio > 0.5
        
        analysis = {
            'balanced': not needs_rebalance,
            'total_load': total_load,
            'avg_load': avg_load,
            'max_load': max_load,
            'min_load': min_load,
            'imbalance_ratio': imbalance_ratio,
            'threshold': 0.5
        }
        
        return analysis
    
    def plan_rebalance(self) -> Dict[str, Any]:
        """规划重平衡操作"""
        analysis = self.analyze_balance()
        
        if analysis['balanced']:
            return {
                'needed': False,
                'reason': 'System is already balanced',
                'analysis': analysis
            }
        
        # 创建重平衡计划
        plan = {
            'needed': True,
            'analysis': analysis,
            'operations': [],
            'estimated_migration_keys': 0
        }
        
        # 在实际实现中，这里会计算具体的数据迁移操作
        # 简化版本只提供建议
        plan['operations'].append({
            'type': 'redistribute_data',
            'description': 'Redistribute data across nodes to achieve better balance',
            'estimated_time': '5-10 minutes'
        })
        
        return plan
    
    def execute_rebalance(self) -> Dict[str, Any]:
        """执行重平衡操作"""
        with self.lock:
            plan = self.plan_rebalance()
            
            if not plan['needed']:
                return plan
            
            # 在实际实现中，这里会执行真正的数据迁移
            # 简化版本只记录重平衡事件
            rebalance_event = {
                'timestamp': datetime.utcnow().isoformat(),
                'plan': plan,
                'status': 'completed',
                'duration': '30 seconds'  # 模拟执行时间
            }
            
            self.rebalance_history.append(rebalance_event)
            
            return rebalance_event
    
    def get_rebalance_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取重平衡历史"""
        with self.lock:
            return self.rebalance_history[-limit:] 