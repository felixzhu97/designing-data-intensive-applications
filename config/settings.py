"""
数据密集型应用系统配置
"""
import os
from typing import Optional
from pydantic import BaseSettings


class DatabaseSettings(BaseSettings):
    """数据库配置"""
    # PostgreSQL
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "ddia_app"
    postgres_user: str = "ddia_user"
    postgres_password: str = "ddia_password"
    
    # Redis
    redis_host: str = "localhost"
    redis_port: int = 6379
    redis_db: int = 0
    
    # MongoDB
    mongodb_host: str = "localhost"
    mongodb_port: int = 27017
    mongodb_db: str = "ddia_app"
    mongodb_user: str = "ddia_user"
    mongodb_password: str = "ddia_password"
    
    # Neo4j
    neo4j_uri: str = "bolt://localhost:7687"
    neo4j_user: str = "neo4j"
    neo4j_password: str = "ddia_password"
    
    # ClickHouse
    clickhouse_host: str = "localhost"
    clickhouse_port: int = 8123
    clickhouse_db: str = "ddia_analytics"
    clickhouse_user: str = "ddia_user"
    clickhouse_password: str = "ddia_password"
    
    # Elasticsearch
    elasticsearch_host: str = "localhost"
    elasticsearch_port: int = 9200
    
    @property
    def postgres_url(self) -> str:
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    @property
    def mongodb_url(self) -> str:
        return f"mongodb://{self.mongodb_user}:{self.mongodb_password}@{self.mongodb_host}:{self.mongodb_port}/{self.mongodb_db}"
    
    @property
    def redis_url(self) -> str:
        return f"redis://{self.redis_host}:{self.redis_port}/{self.redis_db}"


class KafkaSettings(BaseSettings):
    """Kafka配置"""
    bootstrap_servers: str = "localhost:9092"
    
    # 主题配置
    user_events_topic: str = "user-events"
    post_events_topic: str = "post-events"
    analytics_events_topic: str = "analytics-events"
    notification_events_topic: str = "notification-events"
    
    # 消费者组
    analytics_consumer_group: str = "analytics-processor"
    notification_consumer_group: str = "notification-processor"
    recommendation_consumer_group: str = "recommendation-processor"


class FlinkSettings(BaseSettings):
    """Flink配置"""
    jobmanager_host: str = "localhost"
    jobmanager_port: int = 8081
    
    # 检查点配置
    checkpoint_interval: int = 60000  # 60秒
    checkpoint_timeout: int = 600000  # 10分钟
    
    # 并行度配置
    default_parallelism: int = 2
    max_parallelism: int = 8


class ReplicationSettings(BaseSettings):
    """复制配置"""
    # 主从复制
    enable_read_replicas: bool = True
    read_replica_lag_threshold: int = 1000  # 毫秒
    
    # 多主复制
    enable_multi_master: bool = False
    conflict_resolution_strategy: str = "last_write_wins"
    
    # 无主复制
    replication_factor: int = 3
    read_quorum: int = 2
    write_quorum: int = 2


class PartitioningSettings(BaseSettings):
    """分区配置"""
    # 用户数据分区
    user_partition_count: int = 16
    user_partition_strategy: str = "hash"  # hash, range
    
    # 帖子数据分区
    post_partition_count: int = 32
    post_partition_strategy: str = "hash"
    
    # 时序数据分区
    timeseries_partition_strategy: str = "time_range"
    timeseries_partition_interval: str = "1d"  # 1天


class CacheSettings(BaseSettings):
    """缓存配置"""
    # Redis缓存
    cache_ttl: int = 3600  # 1小时
    session_ttl: int = 86400  # 24小时
    
    # 多级缓存
    l1_cache_size: int = 1000  # 内存缓存大小
    l2_cache_enabled: bool = True  # Redis缓存
    
    # 缓存策略
    cache_strategy: str = "write_through"  # write_through, write_back, write_around


class MonitoringSettings(BaseSettings):
    """监控配置"""
    # Prometheus
    prometheus_host: str = "localhost"
    prometheus_port: int = 9090
    metrics_port: int = 8000
    
    # 指标收集
    collect_detailed_metrics: bool = True
    metrics_collection_interval: int = 15  # 秒
    
    # 告警配置
    enable_alerting: bool = True
    alert_thresholds = {
        "cpu_usage": 80,
        "memory_usage": 85,
        "disk_usage": 90,
        "response_time_p99": 1000,  # 毫秒
    }


class SecuritySettings(BaseSettings):
    """安全配置"""
    secret_key: str = "your-secret-key-change-in-production"
    algorithm: str = "HS256"
    access_token_expire_minutes: int = 30
    
    # 加密配置
    password_hash_algorithm: str = "bcrypt"
    encryption_key: Optional[str] = None
    
    # 限流配置
    rate_limit_requests: int = 100
    rate_limit_window: int = 60  # 秒


class BatchProcessingSettings(BaseSettings):
    """批处理配置"""
    # Spark配置
    spark_master: str = "local[*]"
    spark_app_name: str = "DDIA-BatchProcessing"
    
    # 批处理任务配置
    batch_size: int = 10000
    batch_interval: int = 3600  # 1小时
    
    # ETL配置
    etl_input_path: str = "/data/input"
    etl_output_path: str = "/data/output"
    etl_checkpoint_path: str = "/data/checkpoints"


class StreamProcessingSettings(BaseSettings):
    """流处理配置"""
    # 窗口配置
    window_size: int = 300  # 5分钟
    window_slide: int = 60   # 1分钟
    
    # 水印配置
    watermark_delay: int = 10  # 10秒
    
    # 状态后端
    state_backend: str = "rocksdb"
    state_checkpoint_dir: str = "/data/flink/checkpoints"


class Settings(BaseSettings):
    """主配置类"""
    # 应用基础配置
    app_name: str = "数据密集型应用系统"
    app_version: str = "1.0.0"
    debug: bool = False
    
    # 服务配置
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    
    # 组件配置
    database: DatabaseSettings = DatabaseSettings()
    kafka: KafkaSettings = KafkaSettings()
    flink: FlinkSettings = FlinkSettings()
    replication: ReplicationSettings = ReplicationSettings()
    partitioning: PartitioningSettings = PartitioningSettings()
    cache: CacheSettings = CacheSettings()
    monitoring: MonitoringSettings = MonitoringSettings()
    security: SecuritySettings = SecuritySettings()
    batch_processing: BatchProcessingSettings = BatchProcessingSettings()
    stream_processing: StreamProcessingSettings = StreamProcessingSettings()
    
    class Config:
        env_file = ".env"
        env_nested_delimiter = "__"


# 全局配置实例
settings = Settings() 