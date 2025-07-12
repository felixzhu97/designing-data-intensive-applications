"""
文档型数据模型
展示NoSQL文档数据库的设计，包括嵌套文档、数组、灵活模式等
"""
from datetime import datetime
from typing import List, Dict, Any, Optional, Union
from enum import Enum
from pydantic import Field, validator
from pymongo import MongoClient, ASCENDING, DESCENDING, TEXT
from pymongo.collection import Collection

from .base import BaseModel, Entity


class ActivityType(str, Enum):
    """活动类型"""
    POST_CREATE = "post_create"
    POST_LIKE = "post_like"
    POST_COMMENT = "post_comment"
    USER_FOLLOW = "user_follow"
    USER_UNFOLLOW = "user_unfollow"
    LOGIN = "login"
    LOGOUT = "logout"
    PROFILE_UPDATE = "profile_update"


class MediaType(str, Enum):
    """媒体类型"""
    IMAGE = "image"
    VIDEO = "video"
    AUDIO = "audio"
    DOCUMENT = "document"


class NotificationType(str, Enum):
    """通知类型"""
    LIKE = "like"
    COMMENT = "comment"
    FOLLOW = "follow"
    MENTION = "mention"
    SYSTEM = "system"


# 嵌套文档模型
class MediaFile(BaseModel):
    """媒体文件"""
    url: str
    type: MediaType
    size: int  # 字节
    width: Optional[int] = None
    height: Optional[int] = None
    duration: Optional[int] = None  # 秒
    thumbnail_url: Optional[str] = None
    alt_text: Optional[str] = None


class Location(BaseModel):
    """位置信息"""
    name: str
    latitude: float
    longitude: float
    country: Optional[str] = None
    city: Optional[str] = None
    address: Optional[str] = None


class UserMention(BaseModel):
    """用户提及"""
    user_id: str
    username: str
    start_index: int
    end_index: int


class Hashtag(BaseModel):
    """话题标签"""
    tag: str
    start_index: int
    end_index: int


class PostMetrics(BaseModel):
    """帖子指标"""
    views: int = 0
    likes: int = 0
    comments: int = 0
    shares: int = 0
    saves: int = 0
    engagement_rate: float = 0.0
    
    def calculate_engagement_rate(self):
        """计算参与度"""
        if self.views == 0:
            self.engagement_rate = 0.0
        else:
            total_engagements = self.likes + self.comments + self.shares + self.saves
            self.engagement_rate = total_engagements / self.views


class UserProfile(Entity):
    """用户档案文档 - 展示灵活的文档结构"""
    # 基本信息
    username: str
    email: str
    display_name: Optional[str] = None
    bio: Optional[str] = None
    avatar: Optional[MediaFile] = None
    
    # 个人信息
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    birth_date: Optional[datetime] = None
    gender: Optional[str] = None
    
    # 联系信息
    phone: Optional[str] = None
    website: Optional[str] = None
    location: Optional[Location] = None
    
    # 社交链接
    social_links: Dict[str, str] = Field(default_factory=dict)
    
    # 偏好设置
    preferences: Dict[str, Any] = Field(default_factory=dict)
    
    # 隐私设置
    privacy_settings: Dict[str, bool] = Field(default_factory=lambda: {
        "profile_public": True,
        "posts_public": True,
        "location_visible": False,
        "online_status_visible": True,
    })
    
    # 统计信息
    stats: Dict[str, int] = Field(default_factory=lambda: {
        "followers_count": 0,
        "following_count": 0,
        "posts_count": 0,
        "total_likes_received": 0,
    })
    
    # 兴趣标签
    interests: List[str] = Field(default_factory=list)
    
    # 语言设置
    languages: List[str] = Field(default_factory=lambda: ["zh-CN"])
    
    # 验证状态
    verification: Dict[str, Any] = Field(default_factory=lambda: {
        "email_verified": False,
        "phone_verified": False,
        "identity_verified": False,
        "verified_at": None,
    })
    
    # 活动状态
    last_active_at: Optional[datetime] = None
    is_online: bool = False
    
    class Config:
        # MongoDB集合名
        collection_name = "user_profiles"


class PostDocument(Entity):
    """帖子文档 - 展示复杂的嵌套结构"""
    # 作者信息
    author_id: str
    author_username: str  # 冗余存储，避免频繁关联查询
    
    # 内容
    content: str
    content_type: str = "text"
    title: Optional[str] = None
    
    # 媒体文件
    media_files: List[MediaFile] = Field(default_factory=list)
    
    # 位置信息
    location: Optional[Location] = None
    
    # 标签和提及
    hashtags: List[Hashtag] = Field(default_factory=list)
    mentions: List[UserMention] = Field(default_factory=list)
    
    # 分类
    category: Optional[str] = None
    tags: List[str] = Field(default_factory=list)
    
    # 状态
    status: str = "published"  # draft, published, archived, deleted
    visibility: str = "public"  # public, private, friends
    
    # 指标
    metrics: PostMetrics = Field(default_factory=PostMetrics)
    
    # 时间信息
    published_at: Optional[datetime] = None
    scheduled_at: Optional[datetime] = None
    
    # 元数据
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # 评论（嵌套文档）
    comments: List[Dict[str, Any]] = Field(default_factory=list)
    
    class Config:
        collection_name = "posts"


class ActivityLog(Entity):
    """活动日志文档 - 展示时序数据存储"""
    # 用户信息
    user_id: str
    username: Optional[str] = None
    
    # 活动信息
    activity_type: ActivityType
    action: str
    description: Optional[str] = None
    
    # 相关对象
    target_type: Optional[str] = None  # user, post, comment
    target_id: Optional[str] = None
    
    # 上下文数据
    context: Dict[str, Any] = Field(default_factory=dict)
    
    # 设备和位置信息
    device_info: Dict[str, str] = Field(default_factory=dict)
    ip_address: Optional[str] = None
    user_agent: Optional[str] = None
    location: Optional[Location] = None
    
    # 会话信息
    session_id: Optional[str] = None
    
    # 时间分区字段（用于分片）
    date_partition: str = Field(default_factory=lambda: datetime.utcnow().strftime("%Y-%m-%d"))
    
    class Config:
        collection_name = "activity_logs"


class NotificationDocument(Entity):
    """通知文档"""
    # 接收者
    recipient_id: str
    recipient_username: str
    
    # 发送者
    sender_id: Optional[str] = None
    sender_username: Optional[str] = None
    
    # 通知内容
    type: NotificationType
    title: str
    message: str
    
    # 相关对象
    related_object_type: Optional[str] = None
    related_object_id: Optional[str] = None
    
    # 状态
    is_read: bool = False
    read_at: Optional[datetime] = None
    
    # 发送渠道
    channels: List[str] = Field(default_factory=lambda: ["in_app"])  # in_app, email, push
    
    # 优先级
    priority: str = "normal"  # low, normal, high, urgent
    
    # 过期时间
    expires_at: Optional[datetime] = None
    
    class Config:
        collection_name = "notifications"


class UserSession(Entity):
    """用户会话文档"""
    user_id: str
    username: str
    
    # 会话信息
    session_token: str
    refresh_token: Optional[str] = None
    
    # 设备信息
    device_id: Optional[str] = None
    device_type: str = "web"  # web, mobile, desktop
    device_name: Optional[str] = None
    
    # 网络信息
    ip_address: str
    user_agent: str
    location: Optional[Location] = None
    
    # 时间信息
    login_at: datetime = Field(default_factory=datetime.utcnow)
    last_activity_at: datetime = Field(default_factory=datetime.utcnow)
    expires_at: datetime
    
    # 状态
    is_active: bool = True
    
    class Config:
        collection_name = "user_sessions"


class SearchIndex(Entity):
    """搜索索引文档"""
    # 文档类型
    document_type: str  # user, post, hashtag
    document_id: str
    
    # 搜索内容
    title: Optional[str] = None
    content: str
    tags: List[str] = Field(default_factory=list)
    
    # 权重和排序
    search_weight: float = 1.0
    popularity_score: float = 0.0
    
    # 元数据
    metadata: Dict[str, Any] = Field(default_factory=dict)
    
    # 更新时间
    indexed_at: datetime = Field(default_factory=datetime.utcnow)
    
    class Config:
        collection_name = "search_index"


# MongoDB操作类
class DocumentRepository:
    """文档仓储基类"""
    
    def __init__(self, collection: Collection):
        self.collection = collection
    
    async def create(self, document: Entity) -> str:
        """创建文档"""
        doc_dict = document.dict()
        result = await self.collection.insert_one(doc_dict)
        return str(result.inserted_id)
    
    async def find_by_id(self, doc_id: str) -> Optional[Dict[str, Any]]:
        """根据ID查找文档"""
        return await self.collection.find_one({"id": doc_id})
    
    async def find_many(self, filter_dict: Dict[str, Any], 
                       skip: int = 0, limit: int = 20) -> List[Dict[str, Any]]:
        """查找多个文档"""
        cursor = self.collection.find(filter_dict).skip(skip).limit(limit)
        return await cursor.to_list(length=limit)
    
    async def update(self, doc_id: str, update_dict: Dict[str, Any]) -> bool:
        """更新文档"""
        result = await self.collection.update_one(
            {"id": doc_id}, 
            {"$set": update_dict}
        )
        return result.modified_count > 0
    
    async def delete(self, doc_id: str) -> bool:
        """删除文档"""
        result = await self.collection.delete_one({"id": doc_id})
        return result.deleted_count > 0
    
    async def count(self, filter_dict: Dict[str, Any] = None) -> int:
        """统计文档数量"""
        return await self.collection.count_documents(filter_dict or {})


# 索引定义
MONGODB_INDEXES = {
    "user_profiles": [
        [("username", ASCENDING)],
        [("email", ASCENDING)],
        [("location.latitude", ASCENDING), ("location.longitude", ASCENDING)],
        [("interests", ASCENDING)],
        [("last_active_at", DESCENDING)],
    ],
    "posts": [
        [("author_id", ASCENDING), ("created_at", DESCENDING)],
        [("hashtags.tag", ASCENDING)],
        [("location.latitude", ASCENDING), ("location.longitude", ASCENDING)],
        [("status", ASCENDING), ("published_at", DESCENDING)],
        [("metrics.engagement_rate", DESCENDING)],
    ],
    "activity_logs": [
        [("user_id", ASCENDING), ("created_at", DESCENDING)],
        [("activity_type", ASCENDING), ("created_at", DESCENDING)],
        [("date_partition", ASCENDING)],
        [("session_id", ASCENDING)],
    ],
    "notifications": [
        [("recipient_id", ASCENDING), ("created_at", DESCENDING)],
        [("is_read", ASCENDING), ("created_at", DESCENDING)],
        [("expires_at", ASCENDING)],
    ],
    "search_index": [
        [("content", TEXT)],
        [("document_type", ASCENDING), ("popularity_score", DESCENDING)],
        [("tags", ASCENDING)],
    ],
} 