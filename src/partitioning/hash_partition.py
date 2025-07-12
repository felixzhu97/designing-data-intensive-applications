"""
哈希分区系统
展示《数据密集型应用系统设计》中的哈希分区概念
"""

import hashlib
import json
import threading
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
from collections import defaultdict
from dataclasses import dataclass, field


@dataclass
class HashPartition:
    """哈希分区"""
    partition_id: int
    node_id: str
    hash_range: Tuple[int, int]  # (start, end)
    data: Dict[str, Any] = field(default_factory=dict)
    stats: Dict[str, int] = field(default_factory=lambda: {
        'record_count': 0,
        'read_count': 0,
        'write_count': 0,
        'storage_size': 0
    })
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def contains_key(self, key: str) -> bool:
        """检查键是否属于此分区"""
        key_hash = self._hash_key(key)
        return self.hash_range[0] <= key_hash <= self.hash_range[1]
    
    def _hash_key(self, key: str) -> int:
        """计算键的哈希值"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16) % (2**32)
    
    def put(self, key: str, value: Any) -> bool:
        """存储键值对"""
        if not self.contains_key(key):
            return False
        
        is_new = key not in self.data
        self.data[key] = value
        
        # 更新统计信息
        if is_new:
            self.stats['record_count'] += 1
        self.stats['write_count'] += 1
        self.stats['storage_size'] = len(json.dumps(self.data))
        
        return True
    
    def get(self, key: str) -> Optional[Any]:
        """获取键值对"""
        if not self.contains_key(key):
            return None
        
        self.stats['read_count'] += 1
        return self.data.get(key)
    
    def delete(self, key: str) -> bool:
        """删除键值对"""
        if not self.contains_key(key):
            return False
        
        if key in self.data:
            del self.data[key]
            self.stats['record_count'] -= 1
            self.stats['storage_size'] = len(json.dumps(self.data))
            return True
        
        return False
    
    def get_stats(self) -> Dict[str, Any]:
        """获取分区统计信息"""
        return {
            'partition_id': self.partition_id,
            'node_id': self.node_id,
            'hash_range': self.hash_range,
            'stats': self.stats.copy(),
            'created_at': self.created_at.isoformat()
        }


class HashPartitioner:
    """哈希分区器"""
    
    def __init__(self, num_partitions: int = 16):
        self.num_partitions = num_partitions
        self.partitions: Dict[int, HashPartition] = {}
        self.nodes: List[str] = []
        self.lock = threading.RLock()
        
        # 创建分区
        self._create_partitions()
    
    def _create_partitions(self):
        """创建分区"""
        hash_space = 2**32
        partition_size = hash_space // self.num_partitions
        
        for i in range(self.num_partitions):
            start_hash = i * partition_size
            end_hash = (i + 1) * partition_size - 1 if i < self.num_partitions - 1 else hash_space - 1
            
            partition = HashPartition(
                partition_id=i,
                node_id=f"node_{i % max(1, len(self.nodes))}",
                hash_range=(start_hash, end_hash)
            )
            
            self.partitions[i] = partition
    
    def add_node(self, node_id: str):
        """添加节点"""
        with self.lock:
            if node_id not in self.nodes:
                self.nodes.append(node_id)
                self._redistribute_partitions()
    
    def remove_node(self, node_id: str):
        """移除节点"""
        with self.lock:
            if node_id in self.nodes:
                self.nodes.remove(node_id)
                self._redistribute_partitions()
    
    def _redistribute_partitions(self):
        """重新分布分区"""
        if not self.nodes:
            return
        
        for i, partition in self.partitions.items():
            new_node = self.nodes[i % len(self.nodes)]
            partition.node_id = new_node
    
    def get_partition(self, key: str) -> HashPartition:
        """获取键对应的分区"""
        partition_id = self._hash_key(key) % self.num_partitions
        return self.partitions[partition_id]
    
    def _hash_key(self, key: str) -> int:
        """计算键的哈希值"""
        return int(hashlib.md5(key.encode()).hexdigest(), 16)
    
    def put(self, key: str, value: Any) -> bool:
        """存储键值对"""
        partition = self.get_partition(key)
        return partition.put(key, value)
    
    def get(self, key: str) -> Optional[Any]:
        """获取键值对"""
        partition = self.get_partition(key)
        return partition.get(key)
    
    def delete(self, key: str) -> bool:
        """删除键值对"""
        partition = self.get_partition(key)
        return partition.delete(key)
    
    def get_partition_stats(self) -> List[Dict[str, Any]]:
        """获取所有分区统计信息"""
        return [partition.get_stats() for partition in self.partitions.values()]
    
    def get_node_stats(self) -> Dict[str, Dict[str, Any]]:
        """获取节点统计信息"""
        node_stats = defaultdict(lambda: {
            'partition_count': 0,
            'total_records': 0,
            'total_reads': 0,
            'total_writes': 0,
            'total_storage': 0
        })
        
        for partition in self.partitions.values():
            node_id = partition.node_id
            stats = partition.stats
            
            node_stats[node_id]['partition_count'] += 1
            node_stats[node_id]['total_records'] += stats['record_count']
            node_stats[node_id]['total_reads'] += stats['read_count']
            node_stats[node_id]['total_writes'] += stats['write_count']
            node_stats[node_id]['total_storage'] += stats['storage_size']
        
        return dict(node_stats)


class PartitionRouter:
    """分区路由器"""
    
    def __init__(self, partitioner: HashPartitioner):
        self.partitioner = partitioner
        self.routing_table: Dict[str, int] = {}
        self.lock = threading.RLock()
    
    def route_request(self, key: str, operation: str) -> Dict[str, Any]:
        """路由请求到正确的分区"""
        partition = self.partitioner.get_partition(key)
        
        routing_info = {
            'partition_id': partition.partition_id,
            'node_id': partition.node_id,
            'operation': operation,
            'key': key,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        # 缓存路由信息
        with self.lock:
            self.routing_table[key] = partition.partition_id
        
        return routing_info
    
    def get_routing_stats(self) -> Dict[str, Any]:
        """获取路由统计信息"""
        with self.lock:
            partition_distribution = defaultdict(int)
            for partition_id in self.routing_table.values():
                partition_distribution[partition_id] += 1
            
            return {
                'total_routes': len(self.routing_table),
                'partition_distribution': dict(partition_distribution),
                'routing_table_size': len(self.routing_table)
            }
    
    def clear_routing_cache(self):
        """清空路由缓存"""
        with self.lock:
            self.routing_table.clear()


class HotspotDetector:
    """热点检测器"""
    
    def __init__(self, partitioner: HashPartitioner, 
                 read_threshold: int = 1000, 
                 write_threshold: int = 500):
        self.partitioner = partitioner
        self.read_threshold = read_threshold
        self.write_threshold = write_threshold
        self.hotspot_history: List[Dict[str, Any]] = []
    
    def detect_hotspots(self) -> List[Dict[str, Any]]:
        """检测热点分区"""
        hotspots = []
        
        for partition in self.partitioner.partitions.values():
            stats = partition.stats
            
            is_read_hotspot = stats['read_count'] > self.read_threshold
            is_write_hotspot = stats['write_count'] > self.write_threshold
            
            if is_read_hotspot or is_write_hotspot:
                hotspot_info = {
                    'partition_id': partition.partition_id,
                    'node_id': partition.node_id,
                    'type': [],
                    'stats': stats.copy(),
                    'detected_at': datetime.utcnow().isoformat()
                }
                
                if is_read_hotspot:
                    hotspot_info['type'].append('read')
                if is_write_hotspot:
                    hotspot_info['type'].append('write')
                
                hotspots.append(hotspot_info)
        
        # 记录历史
        if hotspots:
            self.hotspot_history.append({
                'timestamp': datetime.utcnow().isoformat(),
                'hotspots': hotspots
            })
        
        return hotspots
    
    def get_hotspot_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """获取热点历史记录"""
        return self.hotspot_history[-limit:]
    
    def suggest_rebalancing(self) -> Dict[str, Any]:
        """建议重新平衡策略"""
        hotspots = self.detect_hotspots()
        
        if not hotspots:
            return {'action': 'none', 'reason': 'No hotspots detected'}
        
        # 分析热点类型
        read_hotspots = [h for h in hotspots if 'read' in h['type']]
        write_hotspots = [h for h in hotspots if 'write' in h['type']]
        
        suggestions = {
            'action': 'rebalance',
            'hotspots_count': len(hotspots),
            'suggestions': []
        }
        
        if read_hotspots:
            suggestions['suggestions'].append({
                'type': 'read_scaling',
                'description': 'Consider adding read replicas for read-heavy partitions',
                'affected_partitions': [h['partition_id'] for h in read_hotspots]
            })
        
        if write_hotspots:
            suggestions['suggestions'].append({
                'type': 'write_scaling',
                'description': 'Consider splitting write-heavy partitions',
                'affected_partitions': [h['partition_id'] for h in write_hotspots]
            })
        
        return suggestions 