"""
监控指标收集系统
展示《数据密集型应用系统设计》中的指标收集和监控概念
"""

import threading
import time
import psutil
from typing import Dict, List, Any, Optional, Callable
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum


class MetricType(Enum):
    """指标类型"""
    COUNTER = "counter"         # 计数器（只增不减）
    GAUGE = "gauge"            # 量规（可增可减）
    HISTOGRAM = "histogram"    # 直方图
    SUMMARY = "summary"        # 摘要


@dataclass
class MetricPoint:
    """指标点"""
    timestamp: datetime
    value: float
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'timestamp': self.timestamp.isoformat(),
            'value': self.value,
            'labels': self.labels
        }


@dataclass
class Metric:
    """指标"""
    name: str
    metric_type: MetricType
    description: str
    unit: str = ""
    labels: Dict[str, str] = field(default_factory=dict)
    points: deque = field(default_factory=lambda: deque(maxlen=1000))
    
    def add_point(self, value: float, labels: Dict[str, str] = None, timestamp: datetime = None):
        """添加指标点"""
        if timestamp is None:
            timestamp = datetime.utcnow()
        
        merged_labels = self.labels.copy()
        if labels:
            merged_labels.update(labels)
        
        point = MetricPoint(timestamp, value, merged_labels)
        self.points.append(point)
    
    def get_latest_value(self) -> Optional[float]:
        """获取最新值"""
        if self.points:
            return self.points[-1].value
        return None
    
    def get_values_in_range(self, start_time: datetime, end_time: datetime) -> List[MetricPoint]:
        """获取时间范围内的值"""
        return [
            point for point in self.points
            if start_time <= point.timestamp <= end_time
        ]
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        if not self.points:
            return {}
        
        values = [point.value for point in self.points]
        return {
            'count': len(values),
            'min': min(values),
            'max': max(values),
            'avg': sum(values) / len(values),
            'latest': values[-1],
            'first': values[0]
        }


class MetricsRegistry:
    """指标注册表"""
    
    def __init__(self):
        self.metrics: Dict[str, Metric] = {}
        self.lock = threading.RLock()
    
    def register(self, name: str, metric_type: MetricType, description: str, 
                unit: str = "", labels: Dict[str, str] = None) -> Metric:
        """注册指标"""
        with self.lock:
            if name in self.metrics:
                return self.metrics[name]
            
            metric = Metric(
                name=name,
                metric_type=metric_type,
                description=description,
                unit=unit,
                labels=labels or {}
            )
            
            self.metrics[name] = metric
            return metric
    
    def get_metric(self, name: str) -> Optional[Metric]:
        """获取指标"""
        with self.lock:
            return self.metrics.get(name)
    
    def get_all_metrics(self) -> List[Metric]:
        """获取所有指标"""
        with self.lock:
            return list(self.metrics.values())
    
    def record_counter(self, name: str, value: float = 1, labels: Dict[str, str] = None):
        """记录计数器"""
        metric = self.get_metric(name)
        if metric and metric.metric_type == MetricType.COUNTER:
            current_value = metric.get_latest_value() or 0
            metric.add_point(current_value + value, labels)
    
    def record_gauge(self, name: str, value: float, labels: Dict[str, str] = None):
        """记录量规"""
        metric = self.get_metric(name)
        if metric and metric.metric_type == MetricType.GAUGE:
            metric.add_point(value, labels)
    
    def record_histogram(self, name: str, value: float, labels: Dict[str, str] = None):
        """记录直方图"""
        metric = self.get_metric(name)
        if metric and metric.metric_type == MetricType.HISTOGRAM:
            metric.add_point(value, labels)
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """获取指标摘要"""
        with self.lock:
            summary = {}
            for name, metric in self.metrics.items():
                summary[name] = {
                    'type': metric.metric_type.value,
                    'description': metric.description,
                    'unit': metric.unit,
                    'latest_value': metric.get_latest_value(),
                    'point_count': len(metric.points)
                }
            return summary


class SystemMetrics:
    """系统指标收集器"""
    
    def __init__(self, registry: MetricsRegistry):
        self.registry = registry
        self.running = False
        self.collection_thread = None
        self.collection_interval = 5.0  # 5秒收集一次
        
        # 注册系统指标
        self._register_system_metrics()
    
    def _register_system_metrics(self):
        """注册系统指标"""
        self.registry.register("system_cpu_usage", MetricType.GAUGE, "CPU使用率", "%")
        self.registry.register("system_memory_usage", MetricType.GAUGE, "内存使用率", "%")
        self.registry.register("system_disk_usage", MetricType.GAUGE, "磁盘使用率", "%")
        self.registry.register("system_network_bytes_sent", MetricType.COUNTER, "网络发送字节数", "bytes")
        self.registry.register("system_network_bytes_recv", MetricType.COUNTER, "网络接收字节数", "bytes")
        self.registry.register("system_load_average", MetricType.GAUGE, "系统负载", "")
        self.registry.register("system_process_count", MetricType.GAUGE, "进程数", "")
        self.registry.register("system_uptime", MetricType.GAUGE, "系统运行时间", "seconds")
    
    def start_collection(self):
        """开始收集指标"""
        if self.running:
            return
        
        self.running = True
        self.collection_thread = threading.Thread(target=self._collection_loop, daemon=True)
        self.collection_thread.start()
    
    def stop_collection(self):
        """停止收集指标"""
        self.running = False
        if self.collection_thread:
            self.collection_thread.join()
    
    def _collection_loop(self):
        """收集循环"""
        while self.running:
            try:
                self._collect_cpu_metrics()
                self._collect_memory_metrics()
                self._collect_disk_metrics()
                self._collect_network_metrics()
                self._collect_system_metrics()
                
                time.sleep(self.collection_interval)
                
            except Exception as e:
                print(f"Error collecting system metrics: {e}")
                time.sleep(1)
    
    def _collect_cpu_metrics(self):
        """收集CPU指标"""
        cpu_percent = psutil.cpu_percent()
        self.registry.record_gauge("system_cpu_usage", cpu_percent)
    
    def _collect_memory_metrics(self):
        """收集内存指标"""
        memory = psutil.virtual_memory()
        self.registry.record_gauge("system_memory_usage", memory.percent)
    
    def _collect_disk_metrics(self):
        """收集磁盘指标"""
        disk = psutil.disk_usage('/')
        usage_percent = (disk.used / disk.total) * 100
        self.registry.record_gauge("system_disk_usage", usage_percent)
    
    def _collect_network_metrics(self):
        """收集网络指标"""
        net_io = psutil.net_io_counters()
        self.registry.record_counter("system_network_bytes_sent", net_io.bytes_sent)
        self.registry.record_counter("system_network_bytes_recv", net_io.bytes_recv)
    
    def _collect_system_metrics(self):
        """收集系统指标"""
        # 系统负载
        load_avg = psutil.getloadavg()[0]  # 1分钟平均负载
        self.registry.record_gauge("system_load_average", load_avg)
        
        # 进程数
        process_count = len(psutil.pids())
        self.registry.record_gauge("system_process_count", process_count)
        
        # 系统运行时间
        uptime = time.time() - psutil.boot_time()
        self.registry.record_gauge("system_uptime", uptime)


class ApplicationMetrics:
    """应用指标收集器"""
    
    def __init__(self, registry: MetricsRegistry):
        self.registry = registry
        self.request_counts: Dict[str, int] = defaultdict(int)
        self.response_times: Dict[str, deque] = defaultdict(lambda: deque(maxlen=100))
        self.error_counts: Dict[str, int] = defaultdict(int)
        self.lock = threading.RLock()
        
        # 注册应用指标
        self._register_application_metrics()
    
    def _register_application_metrics(self):
        """注册应用指标"""
        self.registry.register("app_request_count", MetricType.COUNTER, "请求总数", "requests")
        self.registry.register("app_response_time", MetricType.HISTOGRAM, "响应时间", "ms")
        self.registry.register("app_error_count", MetricType.COUNTER, "错误总数", "errors")
        self.registry.register("app_active_connections", MetricType.GAUGE, "活跃连接数", "connections")
        self.registry.register("app_queue_size", MetricType.GAUGE, "队列大小", "items")
        self.registry.register("app_cache_hit_rate", MetricType.GAUGE, "缓存命中率", "%")
        self.registry.register("app_database_connections", MetricType.GAUGE, "数据库连接数", "connections")
    
    def record_request(self, endpoint: str, method: str, response_time: float, status_code: int):
        """记录请求"""
        with self.lock:
            labels = {'endpoint': endpoint, 'method': method, 'status': str(status_code)}
            
            # 请求计数
            self.registry.record_counter("app_request_count", 1, labels)
            
            # 响应时间
            self.registry.record_histogram("app_response_time", response_time, labels)
            
            # 错误计数
            if status_code >= 400:
                self.registry.record_counter("app_error_count", 1, labels)
    
    def record_connection_count(self, count: int):
        """记录连接数"""
        self.registry.record_gauge("app_active_connections", count)
    
    def record_queue_size(self, size: int):
        """记录队列大小"""
        self.registry.record_gauge("app_queue_size", size)
    
    def record_cache_hit_rate(self, hit_rate: float):
        """记录缓存命中率"""
        self.registry.record_gauge("app_cache_hit_rate", hit_rate)
    
    def record_database_connections(self, count: int):
        """记录数据库连接数"""
        self.registry.record_gauge("app_database_connections", count)


class MetricsCollector:
    """指标收集器"""
    
    def __init__(self):
        self.registry = MetricsRegistry()
        self.system_metrics = SystemMetrics(self.registry)
        self.application_metrics = ApplicationMetrics(self.registry)
        self.custom_collectors: List[Callable] = []
        self.running = False
    
    def start(self):
        """启动指标收集"""
        if self.running:
            return
        
        self.running = True
        self.system_metrics.start_collection()
        
        # 启动自定义收集器
        for collector in self.custom_collectors:
            try:
                collector()
            except Exception as e:
                print(f"Error in custom collector: {e}")
    
    def stop(self):
        """停止指标收集"""
        self.running = False
        self.system_metrics.stop_collection()
    
    def add_custom_collector(self, collector: Callable):
        """添加自定义收集器"""
        self.custom_collectors.append(collector)
    
    def get_metrics(self, names: List[str] = None) -> Dict[str, Any]:
        """获取指标"""
        if names is None:
            return self.registry.get_metrics_summary()
        
        result = {}
        for name in names:
            metric = self.registry.get_metric(name)
            if metric:
                result[name] = {
                    'type': metric.metric_type.value,
                    'description': metric.description,
                    'unit': metric.unit,
                    'latest_value': metric.get_latest_value(),
                    'statistics': metric.get_statistics()
                }
        return result
    
    def get_metrics_for_period(self, start_time: datetime, end_time: datetime) -> Dict[str, List[Dict[str, Any]]]:
        """获取时间段内的指标"""
        result = {}
        for name, metric in self.registry.metrics.items():
            points = metric.get_values_in_range(start_time, end_time)
            result[name] = [point.to_dict() for point in points]
        return result
    
    def export_prometheus_format(self) -> str:
        """导出Prometheus格式"""
        lines = []
        
        for name, metric in self.registry.metrics.items():
            # 指标帮助信息
            lines.append(f"# HELP {name} {metric.description}")
            lines.append(f"# TYPE {name} {metric.metric_type.value}")
            
            # 指标值
            latest_value = metric.get_latest_value()
            if latest_value is not None:
                labels_str = ""
                if metric.labels:
                    label_pairs = [f'{k}="{v}"' for k, v in metric.labels.items()]
                    labels_str = "{" + ",".join(label_pairs) + "}"
                
                lines.append(f"{name}{labels_str} {latest_value}")
        
        return "\n".join(lines)
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """获取仪表板数据"""
        now = datetime.utcnow()
        one_hour_ago = now - timedelta(hours=1)
        
        # 获取最近一小时的数据
        metrics_data = self.get_metrics_for_period(one_hour_ago, now)
        
        # 构建仪表板数据
        dashboard = {
            'timestamp': now.isoformat(),
            'system': {
                'cpu_usage': self.registry.get_metric("system_cpu_usage").get_latest_value(),
                'memory_usage': self.registry.get_metric("system_memory_usage").get_latest_value(),
                'disk_usage': self.registry.get_metric("system_disk_usage").get_latest_value(),
                'load_average': self.registry.get_metric("system_load_average").get_latest_value()
            },
            'application': {
                'request_count': self.registry.get_metric("app_request_count").get_latest_value(),
                'error_count': self.registry.get_metric("app_error_count").get_latest_value(),
                'active_connections': self.registry.get_metric("app_active_connections").get_latest_value(),
                'cache_hit_rate': self.registry.get_metric("app_cache_hit_rate").get_latest_value()
            },
            'trends': metrics_data
        }
        
        return dashboard 