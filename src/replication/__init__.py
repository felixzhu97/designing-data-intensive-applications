"""
复制系统包
展示《数据密集型应用系统设计》中的数据复制概念
"""

from .master_slave import *
from .multi_master import *
from .leaderless import *
from .conflict_resolution import *

__all__ = [
    # 主从复制
    "MasterSlaveReplication",
    "ReplicationMaster",
    "ReplicationSlave",
    
    # 多主复制
    "MultiMasterReplication",
    "ReplicationNode",
    "ConflictDetector",
    
    # 无主复制
    "LeaderlessReplication",
    "QuorumManager",
    "VectorClock",
    
    # 冲突解决
    "ConflictResolver",
    "LastWriteWins",
    "MergeResolver",
    "CRDT",
] 