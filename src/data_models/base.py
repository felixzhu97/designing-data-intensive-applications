"""
基础数据模型
提供所有数据模型的通用功能
"""
import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel as PydanticBaseModel, Field
from sqlalchemy import Column, DateTime, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID


# SQLAlchemy基础类
SQLAlchemyBase = declarative_base()


class TimestampMixin:
    """时间戳混入类"""
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)


class BaseModel(PydanticBaseModel):
    """基础Pydantic模型"""
    
    class Config:
        # 允许从ORM对象创建
        from_attributes = True
        # 使用枚举值
        use_enum_values = True
        # 验证赋值
        validate_assignment = True
        # 任意类型允许
        arbitrary_types_allowed = True


class Entity(BaseModel):
    """实体基类"""
    id: Optional[str] = Field(default_factory=lambda: str(uuid.uuid4()))
    created_at: Optional[datetime] = Field(default_factory=datetime.utcnow)
    updated_at: Optional[datetime] = Field(default_factory=datetime.utcnow)


class Event(BaseModel):
    """事件基类"""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    source: str
    version: str = "1.0"
    data: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        """转换为字典"""
        return self.dict()


class AggregateRoot(Entity):
    """聚合根基类"""
    version: int = 0
    
    def __init__(self, **data):
        super().__init__(**data)
        self._events: list[Event] = []
    
    def add_event(self, event: Event):
        """添加领域事件"""
        self._events.append(event)
    
    def get_events(self) -> list[Event]:
        """获取未提交的事件"""
        return self._events.copy()
    
    def clear_events(self):
        """清除事件"""
        self._events.clear()


class ValueObject(BaseModel):
    """值对象基类"""
    
    class Config(BaseModel.Config):
        # 值对象不可变
        allow_mutation = False
        # 值对象相等性基于值
        frozen = True


class DomainError(Exception):
    """领域错误基类"""
    
    def __init__(self, message: str, error_code: str = None):
        self.message = message
        self.error_code = error_code
        super().__init__(self.message)


class ValidationError(DomainError):
    """验证错误"""
    pass


class BusinessRuleError(DomainError):
    """业务规则错误"""
    pass


class ConcurrencyError(DomainError):
    """并发错误"""
    pass


# 常用值对象
class Email(ValueObject):
    """邮箱值对象"""
    value: str
    
    def __init__(self, value: str):
        import re
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        if not re.match(email_pattern, value):
            raise ValidationError(f"Invalid email format: {value}")
        super().__init__(value=value)


class UserId(ValueObject):
    """用户ID值对象"""
    value: str
    
    def __init__(self, value: str = None):
        if value is None:
            value = str(uuid.uuid4())
        super().__init__(value=value)


class PostId(ValueObject):
    """帖子ID值对象"""
    value: str
    
    def __init__(self, value: str = None):
        if value is None:
            value = str(uuid.uuid4())
        super().__init__(value=value)


class Money(ValueObject):
    """金额值对象"""
    amount: float
    currency: str = "USD"
    
    def __init__(self, amount: float, currency: str = "USD"):
        if amount < 0:
            raise ValidationError("Amount cannot be negative")
        super().__init__(amount=amount, currency=currency)
    
    def add(self, other: 'Money') -> 'Money':
        """加法运算"""
        if self.currency != other.currency:
            raise BusinessRuleError("Cannot add different currencies")
        return Money(self.amount + other.amount, self.currency)
    
    def subtract(self, other: 'Money') -> 'Money':
        """减法运算"""
        if self.currency != other.currency:
            raise BusinessRuleError("Cannot subtract different currencies")
        return Money(self.amount - other.amount, self.currency)


class Location(ValueObject):
    """位置值对象"""
    latitude: float
    longitude: float
    
    def __init__(self, latitude: float, longitude: float):
        if not (-90 <= latitude <= 90):
            raise ValidationError("Latitude must be between -90 and 90")
        if not (-180 <= longitude <= 180):
            raise ValidationError("Longitude must be between -180 and 180")
        super().__init__(latitude=latitude, longitude=longitude)
    
    def distance_to(self, other: 'Location') -> float:
        """计算到另一个位置的距离（公里）"""
        import math
        
        # 使用Haversine公式
        R = 6371  # 地球半径（公里）
        
        lat1_rad = math.radians(self.latitude)
        lat2_rad = math.radians(other.latitude)
        delta_lat = math.radians(other.latitude - self.latitude)
        delta_lon = math.radians(other.longitude - self.longitude)
        
        a = (math.sin(delta_lat / 2) ** 2 + 
             math.cos(lat1_rad) * math.cos(lat2_rad) * 
             math.sin(delta_lon / 2) ** 2)
        c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
        
        return R * c


# 分页相关
class PaginationParams(BaseModel):
    """分页参数"""
    page: int = Field(default=1, ge=1)
    size: int = Field(default=20, ge=1, le=100)
    
    @property
    def offset(self) -> int:
        return (self.page - 1) * self.size


class PaginatedResponse(BaseModel):
    """分页响应"""
    items: list[Any]
    total: int
    page: int
    size: int
    pages: int
    
    @classmethod
    def create(cls, items: list[Any], total: int, params: PaginationParams):
        """创建分页响应"""
        pages = (total + params.size - 1) // params.size
        return cls(
            items=items,
            total=total,
            page=params.page,
            size=params.size,
            pages=pages
        ) 