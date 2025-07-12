"""
事件流处理系统
展示事件驱动架构和流式数据处理
"""
import time
import asyncio
import threading
import json
from typing import Dict, List, Optional, Any, Callable, AsyncGenerator, Union
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
import uuid
from collections import deque, defaultdict
import heapq

from ..data_models.base import BaseModel, Event


class EventType(str, Enum):
    """事件类型"""
    USER_ACTION = "user_action"
    SYSTEM_EVENT = "system_event"
    BUSINESS_EVENT = "business_event"
    ERROR_EVENT = "error_event"


class ProcessingGuarantee(str, Enum):
    """处理保证"""
    AT_MOST_ONCE = "at_most_once"      # 最多一次
    AT_LEAST_ONCE = "at_least_once"    # 最少一次
    EXACTLY_ONCE = "exactly_once"      # 恰好一次


@dataclass
class StreamEvent(Event):
    """流事件"""
    partition_key: Optional[str] = None
    correlation_id: Optional[str] = None
    headers: Dict[str, str] = field(default_factory=dict)
    
    def __post_init__(self):
        if not self.partition_key:
            self.partition_key = str(hash(self.event_id) % 16)  # 默认16个分区


class WaterMark:
    """水印 - 用于处理乱序事件"""
    
    def __init__(self, timestamp: datetime, max_delay: timedelta = timedelta(seconds=10)):
        self.timestamp = timestamp
        self.max_delay = max_delay
    
    @property
    def watermark_time(self) -> datetime:
        """水印时间"""
        return self.timestamp - self.max_delay
    
    def is_late(self, event_time: datetime) -> bool:
        """判断事件是否迟到"""
        return event_time < self.watermark_time


class EventStream:
    """事件流"""
    
    def __init__(self, stream_name: str, partitions: int = 16):
        self.stream_name = stream_name
        self.partitions = partitions
        self.events: Dict[int, deque] = {i: deque() for i in range(partitions)}
        self.subscribers: List[Callable] = []
        self.watermarks: Dict[int, WaterMark] = {}
        self.lock = threading.RLock()
    
    def publish(self, event: StreamEvent):
        """发布事件"""
        partition = int(event.partition_key) % self.partitions
        
        with self.lock:
            self.events[partition].append(event)
            
            # 更新水印
            event_time = datetime.fromtimestamp(event.timestamp)
            if partition not in self.watermarks or event_time > self.watermarks[partition].timestamp:
                self.watermarks[partition] = WaterMark(event_time)
        
        # 通知订阅者
        self._notify_subscribers(event)
    
    def subscribe(self, callback: Callable[[StreamEvent], None]):
        """订阅事件"""
        self.subscribers.append(callback)
    
    def consume(self, partition: int, max_events: int = 100) -> List[StreamEvent]:
        """消费事件"""
        with self.lock:
            events = []
            partition_queue = self.events[partition]
            
            for _ in range(min(max_events, len(partition_queue))):
                if partition_queue:
                    events.append(partition_queue.popleft())
            
            return events
    
    def _notify_subscribers(self, event: StreamEvent):
        """通知订阅者"""
        for callback in self.subscribers:
            try:
                callback(event)
            except Exception as e:
                print(f"Subscriber callback error: {e}")
    
    def get_watermark(self, partition: int) -> Optional[WaterMark]:
        """获取分区水印"""
        return self.watermarks.get(partition)


class StreamOperator:
    """流操作符基类"""
    
    def __init__(self, name: str):
        self.name = name
        self.parallelism = 1
        self.state = {}
    
    def process(self, event: StreamEvent) -> Union[StreamEvent, List[StreamEvent], None]:
        """处理事件"""
        raise NotImplementedError
    
    def on_timer(self, timestamp: datetime):
        """定时器回调"""
        pass


class MapOperator(StreamOperator):
    """映射操作符"""
    
    def __init__(self, name: str, mapper_func: Callable[[StreamEvent], StreamEvent]):
        super().__init__(name)
        self.mapper_func = mapper_func
    
    def process(self, event: StreamEvent) -> StreamEvent:
        """映射事件"""
        return self.mapper_func(event)


class FilterOperator(StreamOperator):
    """过滤操作符"""
    
    def __init__(self, name: str, predicate: Callable[[StreamEvent], bool]):
        super().__init__(name)
        self.predicate = predicate
    
    def process(self, event: StreamEvent) -> Optional[StreamEvent]:
        """过滤事件"""
        return event if self.predicate(event) else None


class AggregateOperator(StreamOperator):
    """聚合操作符"""
    
    def __init__(self, name: str, key_extractor: Callable[[StreamEvent], str],
                 aggregate_func: Callable[[Any, StreamEvent], Any],
                 window_size: timedelta = timedelta(minutes=5)):
        super().__init__(name)
        self.key_extractor = key_extractor
        self.aggregate_func = aggregate_func
        self.window_size = window_size
        self.windows: Dict[str, Dict[datetime, Any]] = defaultdict(dict)
    
    def process(self, event: StreamEvent) -> Optional[StreamEvent]:
        """聚合事件"""
        key = self.key_extractor(event)
        event_time = datetime.fromtimestamp(event.timestamp)
        
        # 计算窗口开始时间
        window_start = self._get_window_start(event_time)
        
        # 更新聚合状态
        if window_start not in self.windows[key]:
            self.windows[key][window_start] = None
        
        self.windows[key][window_start] = self.aggregate_func(
            self.windows[key][window_start], event
        )
        
        # 检查窗口是否可以输出
        return self._check_window_complete(key, window_start)
    
    def _get_window_start(self, event_time: datetime) -> datetime:
        """计算窗口开始时间"""
        window_size_seconds = self.window_size.total_seconds()
        timestamp = event_time.timestamp()
        window_timestamp = int(timestamp // window_size_seconds) * window_size_seconds
        return datetime.fromtimestamp(window_timestamp)
    
    def _check_window_complete(self, key: str, window_start: datetime) -> Optional[StreamEvent]:
        """检查窗口是否完成"""
        # 简化版本：立即输出结果
        result = self.windows[key][window_start]
        
        return StreamEvent(
            event_id=str(uuid.uuid4()),
            event_type="aggregation_result",
            timestamp=time.time(),
            source=self.name,
            data={
                "key": key,
                "window_start": window_start.isoformat(),
                "window_end": (window_start + self.window_size).isoformat(),
                "result": result
            }
        )


class JoinOperator(StreamOperator):
    """连接操作符"""
    
    def __init__(self, name: str, other_stream: str,
                 left_key_extractor: Callable[[StreamEvent], str],
                 right_key_extractor: Callable[[StreamEvent], str],
                 join_func: Callable[[StreamEvent, StreamEvent], StreamEvent],
                 window_size: timedelta = timedelta(minutes=1)):
        super().__init__(name)
        self.other_stream = other_stream
        self.left_key_extractor = left_key_extractor
        self.right_key_extractor = right_key_extractor
        self.join_func = join_func
        self.window_size = window_size
        
        # 存储两个流的事件
        self.left_buffer: Dict[str, List[StreamEvent]] = defaultdict(list)
        self.right_buffer: Dict[str, List[StreamEvent]] = defaultdict(list)
    
    def process_left(self, event: StreamEvent) -> List[StreamEvent]:
        """处理左流事件"""
        key = self.left_key_extractor(event)
        self.left_buffer[key].append(event)
        
        # 与右流中相同key的事件进行连接
        results = []
        for right_event in self.right_buffer.get(key, []):
            if self._in_window(event, right_event):
                results.append(self.join_func(event, right_event))
        
        return results
    
    def process_right(self, event: StreamEvent) -> List[StreamEvent]:
        """处理右流事件"""
        key = self.right_key_extractor(event)
        self.right_buffer[key].append(event)
        
        # 与左流中相同key的事件进行连接
        results = []
        for left_event in self.left_buffer.get(key, []):
            if self._in_window(left_event, event):
                results.append(self.join_func(left_event, event))
        
        return results
    
    def _in_window(self, event1: StreamEvent, event2: StreamEvent) -> bool:
        """检查两个事件是否在连接窗口内"""
        time_diff = abs(event1.timestamp - event2.timestamp)
        return time_diff <= self.window_size.total_seconds()


class StreamProcessor:
    """流处理器"""
    
    def __init__(self, name: str, parallelism: int = 1):
        self.name = name
        self.parallelism = parallelism
        self.operators: List[StreamOperator] = []
        self.input_streams: List[EventStream] = []
        self.output_streams: List[EventStream] = []
        self.is_running = False
        self.processing_threads: List[threading.Thread] = []
        
        # 状态管理
        self.checkpoints: Dict[str, Any] = {}
        self.processing_guarantee = ProcessingGuarantee.AT_LEAST_ONCE
    
    def add_operator(self, operator: StreamOperator):
        """添加操作符"""
        self.operators.append(operator)
    
    def add_input_stream(self, stream: EventStream):
        """添加输入流"""
        self.input_streams.append(stream)
    
    def add_output_stream(self, stream: EventStream):
        """添加输出流"""
        self.output_streams.append(stream)
    
    def start(self):
        """启动流处理"""
        self.is_running = True
        
        # 为每个并行度启动处理线程
        for i in range(self.parallelism):
            thread = threading.Thread(
                target=self._processing_loop,
                args=(i,),
                daemon=True
            )
            thread.start()
            self.processing_threads.append(thread)
        
        print(f"Stream processor {self.name} started with parallelism {self.parallelism}")
    
    def stop(self):
        """停止流处理"""
        self.is_running = False
        print(f"Stream processor {self.name} stopped")
    
    def _processing_loop(self, partition_id: int):
        """处理循环"""
        while self.is_running:
            try:
                # 从输入流消费事件
                for input_stream in self.input_streams:
                    events = input_stream.consume(partition_id % input_stream.partitions, max_events=10)
                    
                    for event in events:
                        # 通过操作符链处理事件
                        result = self._process_event(event)
                        
                        # 输出到下游
                        if result and self.output_streams:
                            if isinstance(result, list):
                                for r in result:
                                    self._emit_event(r)
                            else:
                                self._emit_event(result)
                
                time.sleep(0.01)  # 10ms处理间隔
                
            except Exception as e:
                print(f"Processing error in {self.name}: {e}")
                time.sleep(0.1)
    
    def _process_event(self, event: StreamEvent) -> Union[StreamEvent, List[StreamEvent], None]:
        """处理单个事件"""
        current_event = event
        
        # 通过操作符链处理
        for operator in self.operators:
            if current_event is None:
                break
            
            if isinstance(current_event, list):
                # 处理多个事件
                results = []
                for e in current_event:
                    result = operator.process(e)
                    if result:
                        if isinstance(result, list):
                            results.extend(result)
                        else:
                            results.append(result)
                current_event = results if results else None
            else:
                # 处理单个事件
                current_event = operator.process(current_event)
        
        return current_event
    
    def _emit_event(self, event: StreamEvent):
        """发送事件到输出流"""
        for output_stream in self.output_streams:
            output_stream.publish(event)
    
    def checkpoint(self) -> str:
        """创建检查点"""
        checkpoint_id = str(uuid.uuid4())
        
        # 保存操作符状态
        operator_states = {}
        for i, operator in enumerate(self.operators):
            operator_states[f"operator_{i}"] = operator.state.copy()
        
        self.checkpoints[checkpoint_id] = {
            "timestamp": time.time(),
            "operator_states": operator_states
        }
        
        print(f"Checkpoint created: {checkpoint_id}")
        return checkpoint_id
    
    def restore_from_checkpoint(self, checkpoint_id: str) -> bool:
        """从检查点恢复"""
        if checkpoint_id not in self.checkpoints:
            return False
        
        checkpoint = self.checkpoints[checkpoint_id]
        operator_states = checkpoint["operator_states"]
        
        # 恢复操作符状态
        for i, operator in enumerate(self.operators):
            operator_key = f"operator_{i}"
            if operator_key in operator_states:
                operator.state = operator_states[operator_key].copy()
        
        print(f"Restored from checkpoint: {checkpoint_id}")
        return True


class EventPublisher:
    """事件发布器"""
    
    def __init__(self, stream: EventStream):
        self.stream = stream
        self.published_count = 0
    
    def publish(self, event_type: str, data: Dict[str, Any], 
                partition_key: str = None, headers: Dict[str, str] = None):
        """发布事件"""
        event = StreamEvent(
            event_id=str(uuid.uuid4()),
            event_type=event_type,
            timestamp=time.time(),
            source="publisher",
            data=data,
            partition_key=partition_key,
            headers=headers or {}
        )
        
        self.stream.publish(event)
        self.published_count += 1
        
        return event.event_id
    
    def publish_user_action(self, user_id: str, action: str, details: Dict[str, Any]):
        """发布用户行为事件"""
        return self.publish(
            event_type="user_action",
            data={
                "user_id": user_id,
                "action": action,
                "details": details,
                "timestamp": datetime.utcnow().isoformat()
            },
            partition_key=user_id
        )
    
    def publish_system_event(self, component: str, event_name: str, metrics: Dict[str, Any]):
        """发布系统事件"""
        return self.publish(
            event_type="system_event",
            data={
                "component": component,
                "event_name": event_name,
                "metrics": metrics,
                "timestamp": datetime.utcnow().isoformat()
            },
            partition_key=component
        )


class EventConsumer:
    """事件消费器"""
    
    def __init__(self, name: str, stream: EventStream, consumer_group: str = "default"):
        self.name = name
        self.stream = stream
        self.consumer_group = consumer_group
        self.consumed_count = 0
        self.is_running = False
        self.partitions = list(range(stream.partitions))
        self.handlers: Dict[str, Callable] = {}
    
    def register_handler(self, event_type: str, handler: Callable[[StreamEvent], None]):
        """注册事件处理器"""
        self.handlers[event_type] = handler
    
    def start(self):
        """启动消费"""
        self.is_running = True
        self.consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.consumer_thread.start()
        print(f"Consumer {self.name} started")
    
    def stop(self):
        """停止消费"""
        self.is_running = False
        print(f"Consumer {self.name} stopped")
    
    def _consume_loop(self):
        """消费循环"""
        while self.is_running:
            try:
                for partition in self.partitions:
                    events = self.stream.consume(partition, max_events=5)
                    
                    for event in events:
                        self._handle_event(event)
                        self.consumed_count += 1
                
                time.sleep(0.05)  # 50ms消费间隔
                
            except Exception as e:
                print(f"Consumer {self.name} error: {e}")
                time.sleep(0.1)
    
    def _handle_event(self, event: StreamEvent):
        """处理事件"""
        if event.event_type in self.handlers:
            try:
                self.handlers[event.event_type](event)
            except Exception as e:
                print(f"Event handler error for {event.event_type}: {e}")
        else:
            # 默认处理器
            self._default_handler(event)
    
    def _default_handler(self, event: StreamEvent):
        """默认事件处理器"""
        print(f"Consumed event: {event.event_type} from {event.source}")


# 流处理工厂类
class StreamProcessingTopology:
    """流处理拓扑"""
    
    def __init__(self, name: str):
        self.name = name
        self.streams: Dict[str, EventStream] = {}
        self.processors: List[StreamProcessor] = []
        self.publishers: Dict[str, EventPublisher] = {}
        self.consumers: Dict[str, EventConsumer] = {}
    
    def create_stream(self, name: str, partitions: int = 16) -> EventStream:
        """创建事件流"""
        stream = EventStream(name, partitions)
        self.streams[name] = stream
        return stream
    
    def create_processor(self, name: str, parallelism: int = 1) -> StreamProcessor:
        """创建流处理器"""
        processor = StreamProcessor(name, parallelism)
        self.processors.append(processor)
        return processor
    
    def create_publisher(self, stream_name: str) -> EventPublisher:
        """创建事件发布器"""
        if stream_name not in self.streams:
            raise ValueError(f"Stream {stream_name} not found")
        
        publisher = EventPublisher(self.streams[stream_name])
        self.publishers[stream_name] = publisher
        return publisher
    
    def create_consumer(self, name: str, stream_name: str, consumer_group: str = "default") -> EventConsumer:
        """创建事件消费器"""
        if stream_name not in self.streams:
            raise ValueError(f"Stream {stream_name} not found")
        
        consumer = EventConsumer(name, self.streams[stream_name], consumer_group)
        self.consumers[name] = consumer
        return consumer
    
    def start_all(self):
        """启动所有组件"""
        for processor in self.processors:
            processor.start()
        
        for consumer in self.consumers.values():
            consumer.start()
        
        print(f"Topology {self.name} started")
    
    def stop_all(self):
        """停止所有组件"""
        for processor in self.processors:
            processor.stop()
        
        for consumer in self.consumers.values():
            consumer.stop()
        
        print(f"Topology {self.name} stopped")
    
    def get_stats(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            "topology_name": self.name,
            "streams": len(self.streams),
            "processors": len(self.processors),
            "publishers": {name: pub.published_count for name, pub in self.publishers.items()},
            "consumers": {name: cons.consumed_count for name, cons in self.consumers.items()}
        } 