"""
流处理系统包
展示《数据密集型应用系统设计》中的流处理概念
"""

from .event_stream import *
from .windowing import *
from .state_management import *
from .cep import *

__all__ = [
    # 事件流
    "EventStream",
    "StreamProcessor",
    "EventPublisher",
    "EventConsumer",
    
    # 窗口操作
    "WindowManager",
    "TumblingWindow",
    "SlidingWindow",
    "SessionWindow",
    
    # 状态管理
    "StreamState",
    "KeyedState",
    "OperatorState",
    "Checkpoint",
    
    # 复杂事件处理
    "CEPEngine",
    "EventPattern",
    "PatternMatcher",
    "EventSequence",
] 