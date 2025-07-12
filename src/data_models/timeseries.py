"""
时序数据模型
展示时间序列数据的存储和分析，包括用户行为、系统指标等
"""
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Union
from enum import Enum
from pydantic import Field, validator
import time

from .base import BaseModel, Entity


class MetricType(str, Enum):
    """指标类型"""
    COUNTER = "counter"      # 计数器
    GAUGE = "gauge"          # 仪表盘
    HISTOGRAM = "histogram"  # 直方图
    SUMMARY = "summary"      # 摘要


class EventType(str, Enum):
    """事件类型"""
    USER_ACTION = "user_action"
    SYSTEM_EVENT = "system_event"
    BUSINESS_EVENT = "business_event"
    ERROR_EVENT = "error_event"


class AggregationFunction(str, Enum):
    """聚合函数"""
    SUM = "sum"
    AVG = "avg"
    MIN = "min"
    MAX = "max"
    COUNT = "count"
    PERCENTILE = "percentile"


# 时序数据点
class TimeSeriesPoint(BaseModel):
    """时序数据点基类"""
    timestamp: datetime
    value: Union[float, int, str, Dict[str, Any]]
    tags: Dict[str, str] = Field(default_factory=dict)
    
    @validator('timestamp', pre=True)
    def parse_timestamp(cls, v):
        if isinstance(v, (int, float)):
            return datetime.fromtimestamp(v)
        return v


class UserActivity(Entity):
    """用户活动时序数据"""
    # 用户标识
    user_id: str
    session_id: Optional[str] = None
    
    # 活动信息
    action: str  # login, logout, view_post, like_post, comment, etc.
    target_type: Optional[str] = None  # post, user, comment
    target_id: Optional[str] = None
    
    # 时间信息
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    duration: Optional[int] = None  # 持续时间（秒）
    
    # 上下文信息
    page_url: Optional[str] = None
    referrer: Optional[str] = None
    user_agent: Optional[str] = None
    ip_address: Optional[str] = None
    
    # 设备信息
    device_type: str = "unknown"  # mobile, desktop, tablet
    platform: Optional[str] = None  # ios, android, web
    browser: Optional[str] = None
    
    # 地理位置
    country: Optional[str] = None
    city: Optional[str] = None
    timezone: Optional[str] = None
    
    # 自定义属性
    properties: Dict[str, Any] = Field(default_factory=dict)
    
    # 分区字段（用于时间分区）
    date_partition: str = Field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d"))
    hour_partition: int = Field(default_factory=lambda: datetime.utcnow().hour)


class SystemMetrics(Entity):
    """系统指标时序数据"""
    # 指标标识
    metric_name: str
    metric_type: MetricType
    
    # 时间和值
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    value: float
    
    # 标签（用于分组和过滤）
    service: str
    instance: str
    environment: str = "production"
    tags: Dict[str, str] = Field(default_factory=dict)
    
    # 单位
    unit: Optional[str] = None
    
    # 分区字段
    date_partition: str = Field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d"))
    
    class Config:
        # ClickHouse表名
        table_name = "system_metrics"


class EventLog(Entity):
    """事件日志时序数据"""
    # 事件标识
    event_type: EventType
    event_name: str
    
    # 时间信息
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # 事件来源
    source: str
    source_id: Optional[str] = None
    
    # 事件数据
    data: Dict[str, Any] = Field(default_factory=dict)
    
    # 严重程度
    severity: str = "info"  # debug, info, warning, error, critical
    
    # 分类标签
    category: Optional[str] = None
    subcategory: Optional[str] = None
    
    # 关联信息
    correlation_id: Optional[str] = None
    trace_id: Optional[str] = None
    span_id: Optional[str] = None
    
    # 分区字段
    date_partition: str = Field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d"))


class BusinessMetrics(Entity):
    """业务指标时序数据"""
    # 指标信息
    metric_name: str
    metric_category: str  # user, content, engagement, revenue
    
    # 时间和值
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    value: float
    
    # 维度标签
    dimensions: Dict[str, str] = Field(default_factory=dict)
    
    # 计算方式
    calculation_method: Optional[str] = None
    
    # 分区字段
    date_partition: str = Field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d"))


class PerformanceMetrics(Entity):
    """性能指标时序数据"""
    # 请求信息
    endpoint: str
    method: str = "GET"
    
    # 时间信息
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    
    # 性能指标
    response_time: float  # 响应时间（毫秒）
    cpu_usage: Optional[float] = None
    memory_usage: Optional[float] = None
    
    # 状态信息
    status_code: int
    error_message: Optional[str] = None
    
    # 用户信息
    user_id: Optional[str] = None
    
    # 分区字段
    date_partition: str = Field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d"))


# 时序数据聚合
class TimeSeriesAggregation(BaseModel):
    """时序数据聚合结果"""
    metric_name: str
    start_time: datetime
    end_time: datetime
    interval: str  # 1m, 5m, 1h, 1d
    function: AggregationFunction
    value: float
    count: int
    tags: Dict[str, str] = Field(default_factory=dict)


class TimeSeriesQuery(BaseModel):
    """时序数据查询"""
    metric_name: str
    start_time: datetime
    end_time: datetime
    interval: str = "5m"
    function: AggregationFunction = AggregationFunction.AVG
    filters: Dict[str, str] = Field(default_factory=dict)
    group_by: List[str] = Field(default_factory=list)


# 时序数据分析
class UserBehaviorAnalyzer:
    """用户行为分析器"""
    
    @staticmethod
    def calculate_session_duration(activities: List[UserActivity]) -> float:
        """计算会话持续时间"""
        if not activities:
            return 0.0
        
        activities.sort(key=lambda x: x.timestamp)
        start_time = activities[0].timestamp
        end_time = activities[-1].timestamp
        return (end_time - start_time).total_seconds()
    
    @staticmethod
    def calculate_page_views(activities: List[UserActivity]) -> int:
        """计算页面浏览量"""
        return len([a for a in activities if a.action == "view_page"])
    
    @staticmethod
    def calculate_bounce_rate(sessions: List[List[UserActivity]]) -> float:
        """计算跳出率"""
        if not sessions:
            return 0.0
        
        bounce_sessions = sum(1 for session in sessions if len(session) == 1)
        return bounce_sessions / len(sessions)
    
    @staticmethod
    def find_popular_pages(activities: List[UserActivity]) -> Dict[str, int]:
        """找出热门页面"""
        page_counts = {}
        for activity in activities:
            if activity.action == "view_page" and activity.page_url:
                page_counts[activity.page_url] = page_counts.get(activity.page_url, 0) + 1
        
        return dict(sorted(page_counts.items(), key=lambda x: x[1], reverse=True))
    
    @staticmethod
    def analyze_user_journey(activities: List[UserActivity]) -> List[str]:
        """分析用户路径"""
        activities.sort(key=lambda x: x.timestamp)
        journey = []
        
        for activity in activities:
            if activity.action == "view_page" and activity.page_url:
                journey.append(activity.page_url)
        
        return journey


class SystemMetricsAnalyzer:
    """系统指标分析器"""
    
    @staticmethod
    def calculate_availability(metrics: List[SystemMetrics], threshold: float = 0.99) -> float:
        """计算可用性"""
        if not metrics:
            return 0.0
        
        uptime_count = sum(1 for m in metrics if m.value >= threshold)
        return uptime_count / len(metrics)
    
    @staticmethod
    def detect_anomalies(metrics: List[SystemMetrics], 
                        window_size: int = 10, 
                        threshold: float = 2.0) -> List[SystemMetrics]:
        """检测异常值"""
        if len(metrics) < window_size:
            return []
        
        anomalies = []
        for i in range(window_size, len(metrics)):
            window = metrics[i-window_size:i]
            avg = sum(m.value for m in window) / len(window)
            std = (sum((m.value - avg) ** 2 for m in window) / len(window)) ** 0.5
            
            current_metric = metrics[i]
            if abs(current_metric.value - avg) > threshold * std:
                anomalies.append(current_metric)
        
        return anomalies
    
    @staticmethod
    def calculate_percentiles(values: List[float], percentiles: List[float]) -> Dict[float, float]:
        """计算百分位数"""
        if not values:
            return {}
        
        sorted_values = sorted(values)
        result = {}
        
        for p in percentiles:
            index = int(len(sorted_values) * p / 100)
            if index >= len(sorted_values):
                index = len(sorted_values) - 1
            result[p] = sorted_values[index]
        
        return result


# 时序数据存储接口
class TimeSeriesStorage:
    """时序数据存储接口"""
    
    def write_point(self, point: TimeSeriesPoint) -> bool:
        """写入单个数据点"""
        raise NotImplementedError
    
    def write_batch(self, points: List[TimeSeriesPoint]) -> bool:
        """批量写入数据点"""
        raise NotImplementedError
    
    def query(self, query: TimeSeriesQuery) -> List[TimeSeriesAggregation]:
        """查询时序数据"""
        raise NotImplementedError
    
    def delete_old_data(self, retention_days: int) -> bool:
        """删除过期数据"""
        raise NotImplementedError


class ClickHouseTimeSeriesStorage(TimeSeriesStorage):
    """ClickHouse时序数据存储实现"""
    
    def __init__(self, client):
        self.client = client
    
    def write_point(self, point: TimeSeriesPoint) -> bool:
        """写入单个数据点"""
        try:
            # 实现ClickHouse写入逻辑
            return True
        except Exception as e:
            print(f"Error writing point: {e}")
            return False
    
    def write_batch(self, points: List[TimeSeriesPoint]) -> bool:
        """批量写入数据点"""
        try:
            # 实现ClickHouse批量写入逻辑
            return True
        except Exception as e:
            print(f"Error writing batch: {e}")
            return False
    
    def query(self, query: TimeSeriesQuery) -> List[TimeSeriesAggregation]:
        """查询时序数据"""
        try:
            # 实现ClickHouse查询逻辑
            return []
        except Exception as e:
            print(f"Error querying data: {e}")
            return []


# 实时指标计算
class RealTimeMetricsCalculator:
    """实时指标计算器"""
    
    def __init__(self):
        self.window_data = {}
    
    def add_data_point(self, metric_name: str, value: float, timestamp: datetime = None):
        """添加数据点"""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        if metric_name not in self.window_data:
            self.window_data[metric_name] = []
        
        self.window_data[metric_name].append((timestamp, value))
        
        # 清理过期数据（保留最近5分钟）
        cutoff_time = timestamp - timedelta(minutes=5)
        self.window_data[metric_name] = [
            (ts, val) for ts, val in self.window_data[metric_name] 
            if ts > cutoff_time
        ]
    
    def get_current_average(self, metric_name: str) -> Optional[float]:
        """获取当前平均值"""
        if metric_name not in self.window_data or not self.window_data[metric_name]:
            return None
        
        values = [val for _, val in self.window_data[metric_name]]
        return sum(values) / len(values)
    
    def get_current_rate(self, metric_name: str) -> Optional[float]:
        """获取当前速率（每秒）"""
        if metric_name not in self.window_data or len(self.window_data[metric_name]) < 2:
            return None
        
        data = self.window_data[metric_name]
        time_span = (data[-1][0] - data[0][0]).total_seconds()
        
        if time_span == 0:
            return None
        
        return len(data) / time_span 