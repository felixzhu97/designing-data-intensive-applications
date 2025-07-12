"""
数据模型包
展示《数据密集型应用系统设计》中的不同数据模型
"""

from .relational import *
from .document import *
from .graph import *
from .timeseries import *
from .base import *

__all__ = [
    # 基础模型
    "BaseModel",
    "TimestampMixin",
    
    # 关系型模型
    "User",
    "Post", 
    "Follow",
    "Like",
    "Comment",
    
    # 文档型模型
    "UserProfile",
    "PostDocument",
    "ActivityLog",
    
    # 图型模型
    "SocialGraphNode",
    "SocialGraphRelation",
    
    # 时序模型
    "UserActivity",
    "SystemMetrics",
    "EventLog",
] 