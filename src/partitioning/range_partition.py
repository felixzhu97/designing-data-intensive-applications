"""
范围分区系统
展示《数据密集型应用系统设计》中的范围分区概念
"""

import threading
from typing import Dict, List, Any, Optional, Tuple, Union
from datetime import datetime, date
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
import json


@dataclass
class PartitionKey:
    """分区键"""
    value: Any
    key_type: str  # 'string', 'int', 'float', 'datetime', 'date'
    
    def __post_init__(self):
        self.comparable_value = self._get_comparable_value()
    
    def _get_comparable_value(self) -> Any:
        """获取可比较的值"""
        if self.key_type == 'string':
            return str(self.value)
        elif self.key_type == 'int':
            return int(self.value)
        elif self.key_type == 'float':
            return float(self.value)
        elif self.key_type == 'datetime':
            if isinstance(self.value, datetime):
                return self.value
            return datetime.fromisoformat(str(self.value))
        elif self.key_type == 'date':
            if isinstance(self.value, date):
                return self.value
            return date.fromisoformat(str(self.value))
        else:
            return self.value
    
    def __lt__(self, other):
        if not isinstance(other, PartitionKey):
            return NotImplemented
        return self.comparable_value < other.comparable_value
    
    def __le__(self, other):
        if not isinstance(other, PartitionKey):
            return NotImplemented
        return self.comparable_value <= other.comparable_value
    
    def __gt__(self, other):
        if not isinstance(other, PartitionKey):
            return NotImplemented
        return self.comparable_value > other.comparable_value
    
    def __ge__(self, other):
        if not isinstance(other, PartitionKey):
            return NotImplemented
        return self.comparable_value >= other.comparable_value
    
    def __eq__(self, other):
        if not isinstance(other, PartitionKey):
            return NotImplemented
        return self.comparable_value == other.comparable_value
    
    def __str__(self):
        return f"PartitionKey({self.value}, {self.key_type})"


@dataclass
class RangePartition:
    """范围分区"""
    partition_id: int
    node_id: str
    start_key: Optional[PartitionKey]  # None表示负无穷
    end_key: Optional[PartitionKey]    # None表示正无穷
    data: Dict[str, Any] = field(default_factory=dict)
    stats: Dict[str, int] = field(default_factory=lambda: {
        'record_count': 0,
        'read_count': 0,
        'write_count': 0,
        'storage_size': 0
    })
    created_at: datetime = field(default_factory=datetime.utcnow)
    
    def contains_key(self, key: PartitionKey) -> bool:
        """检查键是否属于此分区"""
        # 检查下界
        if self.start_key is not None and key < self.start_key:
            return False
        
        # 检查上界
        if self.end_key is not None and key >= self.end_key:
            return False
        
        return True
    
    def put(self, key: str, value: Any, partition_key: PartitionKey) -> bool:
        """存储键值对"""
        if not self.contains_key(partition_key):
            return False
        
        is_new = key not in self.data
        self.data[key] = value
        
        # 更新统计信息
        if is_new:
            self.stats['record_count'] += 1
        self.stats['write_count'] += 1
        self.stats['storage_size'] = len(json.dumps(self.data))
        
        return True
    
    def get(self, key: str, partition_key: PartitionKey) -> Optional[Any]:
        """获取键值对"""
        if not self.contains_key(partition_key):
            return None
        
        self.stats['read_count'] += 1
        return self.data.get(key)
    
    def delete(self, key: str, partition_key: PartitionKey) -> bool:
        """删除键值对"""
        if not self.contains_key(partition_key):
            return False
        
        if key in self.data:
            del self.data[key]
            self.stats['record_count'] -= 1
            self.stats['storage_size'] = len(json.dumps(self.data))
            return True
        
        return False
    
    def get_range_keys(self, start_key: Optional[PartitionKey], 
                      end_key: Optional[PartitionKey]) -> List[str]:
        """获取范围内的键"""
        # 简化实现：返回所有键（实际实现中会根据键的分区键过滤）
        result = []
        for key in self.data.keys():
            # 这里需要根据实际的分区键逻辑来过滤
            # 简化版本直接返回所有键
            result.append(key)
        
        return result
    
    def get_stats(self) -> Dict[str, Any]:
        """获取分区统计信息"""
        return {
            'partition_id': self.partition_id,
            'node_id': self.node_id,
            'start_key': str(self.start_key) if self.start_key else None,
            'end_key': str(self.end_key) if self.end_key else None,
            'stats': self.stats.copy(),
            'created_at': self.created_at.isoformat()
        }


class RangePartitioner:
    """范围分区器"""
    
    def __init__(self, key_type: str = 'string'):
        self.key_type = key_type
        self.partitions: Dict[int, RangePartition] = {}
        self.partition_counter = 0
        self.lock = threading.RLock()
        
        # 创建默认分区（覆盖所有范围）
        self._create_default_partition()
    
    def _create_default_partition(self):
        """创建默认分区"""
        partition = RangePartition(
            partition_id=self.partition_counter,
            node_id="default_node",
            start_key=None,  # 负无穷
            end_key=None     # 正无穷
        )
        self.partitions[self.partition_counter] = partition
        self.partition_counter += 1
    
    def add_partition(self, split_key: Any, node_id: str) -> Tuple[int, int]:
        """添加新分区（在指定键处分割）"""
        with self.lock:
            partition_key = PartitionKey(split_key, self.key_type)
            
            # 找到包含split_key的分区
            target_partition = None
            for partition in self.partitions.values():
                if partition.contains_key(partition_key):
                    target_partition = partition
                    break
            
            if target_partition is None:
                raise ValueError(f"No partition found for key: {split_key}")
            
            # 创建新分区
            new_partition_id = self.partition_counter
            self.partition_counter += 1
            
            # 原分区的范围变为 [start_key, split_key)
            # 新分区的范围变为 [split_key, end_key)
            new_partition = RangePartition(
                partition_id=new_partition_id,
                node_id=node_id,
                start_key=partition_key,
                end_key=target_partition.end_key
            )
            
            # 更新原分区的结束键
            target_partition.end_key = partition_key
            
            # 添加新分区
            self.partitions[new_partition_id] = new_partition
            
            # 迁移数据（简化实现）
            self._migrate_data(target_partition, new_partition)
            
            return target_partition.partition_id, new_partition_id
    
    def _migrate_data(self, old_partition: RangePartition, new_partition: RangePartition):
        """迁移数据到新分区"""
        # 简化实现：实际中需要根据键的分区键来决定数据归属
        # 这里假设所有数据都留在原分区
        pass
    
    def get_partition(self, key: Any) -> Optional[RangePartition]:
        """获取键对应的分区"""
        partition_key = PartitionKey(key, self.key_type)
        
        with self.lock:
            for partition in self.partitions.values():
                if partition.contains_key(partition_key):
                    return partition
        
        return None
    
    def put(self, key: str, value: Any, partition_key: Any) -> bool:
        """存储键值对"""
        partition = self.get_partition(partition_key)
        if partition is None:
            return False
        
        pk = PartitionKey(partition_key, self.key_type)
        return partition.put(key, value, pk)
    
    def get(self, key: str, partition_key: Any) -> Optional[Any]:
        """获取键值对"""
        partition = self.get_partition(partition_key)
        if partition is None:
            return None
        
        pk = PartitionKey(partition_key, self.key_type)
        return partition.get(key, pk)
    
    def delete(self, key: str, partition_key: Any) -> bool:
        """删除键值对"""
        partition = self.get_partition(partition_key)
        if partition is None:
            return False
        
        pk = PartitionKey(partition_key, self.key_type)
        return partition.delete(key, pk)
    
    def range_query(self, start_key: Any, end_key: Any) -> List[Tuple[str, Any]]:
        """范围查询"""
        start_pk = PartitionKey(start_key, self.key_type) if start_key is not None else None
        end_pk = PartitionKey(end_key, self.key_type) if end_key is not None else None
        
        results = []
        
        with self.lock:
            for partition in self.partitions.values():
                # 检查分区是否与查询范围重叠
                if self._partition_overlaps_range(partition, start_pk, end_pk):
                    # 获取分区内的相关数据
                    for key, value in partition.data.items():
                        # 简化实现：返回所有数据
                        # 实际实现中需要根据分区键过滤
                        results.append((key, value))
        
        return results
    
    def _partition_overlaps_range(self, partition: RangePartition, 
                                 start_key: Optional[PartitionKey], 
                                 end_key: Optional[PartitionKey]) -> bool:
        """检查分区是否与查询范围重叠"""
        # 分区范围：[partition.start_key, partition.end_key)
        # 查询范围：[start_key, end_key)
        
        # 如果分区结束键 <= 查询开始键，则不重叠
        if (partition.end_key is not None and 
            start_key is not None and 
            partition.end_key <= start_key):
            return False
        
        # 如果分区开始键 >= 查询结束键，则不重叠
        if (partition.start_key is not None and 
            end_key is not None and 
            partition.start_key >= end_key):
            return False
        
        return True
    
    def get_partition_stats(self) -> List[Dict[str, Any]]:
        """获取所有分区统计信息"""
        with self.lock:
            return [partition.get_stats() for partition in self.partitions.values()]
    
    def get_partition_distribution(self) -> Dict[str, Any]:
        """获取分区分布信息"""
        with self.lock:
            node_stats = defaultdict(lambda: {
                'partition_count': 0,
                'total_records': 0,
                'total_storage': 0
            })
            
            for partition in self.partitions.values():
                node_id = partition.node_id
                stats = partition.stats
                
                node_stats[node_id]['partition_count'] += 1
                node_stats[node_id]['total_records'] += stats['record_count']
                node_stats[node_id]['total_storage'] += stats['storage_size']
            
            return {
                'total_partitions': len(self.partitions),
                'key_type': self.key_type,
                'node_distribution': dict(node_stats)
            }
    
    def suggest_split(self) -> List[Dict[str, Any]]:
        """建议分区分割"""
        suggestions = []
        
        with self.lock:
            for partition in self.partitions.values():
                stats = partition.stats
                
                # 如果分区太大，建议分割
                if stats['record_count'] > 10000:  # 超过10000条记录
                    suggestions.append({
                        'partition_id': partition.partition_id,
                        'node_id': partition.node_id,
                        'current_records': stats['record_count'],
                        'suggestion': 'split',
                        'reason': f'Partition has {stats["record_count"]} records, exceeding threshold of 10000'
                    })
                
                # 如果分区负载过高，建议分割
                elif stats['read_count'] > 5000:  # 读取次数过多
                    suggestions.append({
                        'partition_id': partition.partition_id,
                        'node_id': partition.node_id,
                        'current_reads': stats['read_count'],
                        'suggestion': 'split',
                        'reason': f'Partition has {stats["read_count"]} reads, indicating high load'
                    })
        
        return suggestions 