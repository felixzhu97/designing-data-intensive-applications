"""
分区系统包
展示《数据密集型应用系统设计》中的数据分区概念
"""

from .hash_partition import *
from .range_partition import *
from .consistent_hash import *

__all__ = [
    # 哈希分区
    "HashPartitioner",
    "HashPartition",
    "PartitionRouter",
    
    # 范围分区
    "RangePartitioner",
    "RangePartition",
    "PartitionKey",
    
    # 一致性哈希
    "ConsistentHashPartitioner",
    "ConsistentHashRing",
    "VirtualNode",
    
    # 分区管理
    "PartitionManager",
    "PartitionRebalancer",
    "HotspotDetector",
] 