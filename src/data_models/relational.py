"""
关系型数据模型
展示传统的关系数据库设计，包括规范化、外键约束等
"""
from datetime import datetime
from enum import Enum
from typing import List, Optional
from sqlalchemy import (
    Column, String, Integer, DateTime, Boolean, Text, 
    ForeignKey, Index, UniqueConstraint, CheckConstraint,
    Float, JSON
)
from sqlalchemy.dialects.postgresql import UUID, ARRAY
from sqlalchemy.orm import relationship
from sqlalchemy.ext.hybrid import hybrid_property
import uuid

from .base import SQLAlchemyBase, TimestampMixin, BaseModel


class UserStatus(str, Enum):
    """用户状态枚举"""
    ACTIVE = "active"
    INACTIVE = "inactive"
    SUSPENDED = "suspended"
    DELETED = "deleted"


class PostStatus(str, Enum):
    """帖子状态枚举"""
    DRAFT = "draft"
    PUBLISHED = "published"
    ARCHIVED = "archived"
    DELETED = "deleted"


class User(SQLAlchemyBase, TimestampMixin):
    """用户表 - 展示主键、索引、约束的使用"""
    __tablename__ = "users"
    
    # 主键
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # 基本信息
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(255), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    
    # 个人信息
    first_name = Column(String(100))
    last_name = Column(String(100))
    bio = Column(Text)
    avatar_url = Column(String(500))
    
    # 状态和设置
    status = Column(String(20), default=UserStatus.ACTIVE, nullable=False)
    is_verified = Column(Boolean, default=False, nullable=False)
    is_private = Column(Boolean, default=False, nullable=False)
    
    # 统计信息
    followers_count = Column(Integer, default=0, nullable=False)
    following_count = Column(Integer, default=0, nullable=False)
    posts_count = Column(Integer, default=0, nullable=False)
    
    # 位置信息
    location = Column(String(255))
    timezone = Column(String(50))
    
    # 最后活动时间
    last_login_at = Column(DateTime)
    last_active_at = Column(DateTime)
    
    # 关系
    posts = relationship("Post", back_populates="author", cascade="all, delete-orphan")
    followers = relationship("Follow", foreign_keys="Follow.following_id", back_populates="following")
    following = relationship("Follow", foreign_keys="Follow.follower_id", back_populates="follower")
    likes = relationship("Like", back_populates="user", cascade="all, delete-orphan")
    comments = relationship("Comment", back_populates="author", cascade="all, delete-orphan")
    
    # 表级约束
    __table_args__ = (
        CheckConstraint('followers_count >= 0', name='check_followers_count'),
        CheckConstraint('following_count >= 0', name='check_following_count'),
        CheckConstraint('posts_count >= 0', name='check_posts_count'),
        Index('idx_user_status_created', 'status', 'created_at'),
        Index('idx_user_location', 'location'),
    )
    
    @hybrid_property
    def full_name(self):
        """全名"""
        if self.first_name and self.last_name:
            return f"{self.first_name} {self.last_name}"
        return self.first_name or self.last_name or self.username
    
    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}')>"


class Post(SQLAlchemyBase, TimestampMixin):
    """帖子表 - 展示外键关系、JSON字段等"""
    __tablename__ = "posts"
    
    # 主键
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # 外键
    author_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    
    # 内容
    title = Column(String(500))
    content = Column(Text, nullable=False)
    content_type = Column(String(20), default='text', nullable=False)  # text, image, video
    
    # 媒体文件
    media_urls = Column(ARRAY(String), default=list)
    
    # 状态
    status = Column(String(20), default=PostStatus.DRAFT, nullable=False)
    is_pinned = Column(Boolean, default=False, nullable=False)
    
    # 统计信息
    likes_count = Column(Integer, default=0, nullable=False)
    comments_count = Column(Integer, default=0, nullable=False)
    shares_count = Column(Integer, default=0, nullable=False)
    views_count = Column(Integer, default=0, nullable=False)
    
    # 标签和分类
    tags = Column(ARRAY(String), default=list)
    category = Column(String(100))
    
    # 地理位置
    location_name = Column(String(255))
    latitude = Column(Float)
    longitude = Column(Float)
    
    # 元数据
    metadata = Column(JSON)
    
    # 发布时间
    published_at = Column(DateTime)
    
    # 关系
    author = relationship("User", back_populates="posts")
    likes = relationship("Like", back_populates="post", cascade="all, delete-orphan")
    comments = relationship("Comment", back_populates="post", cascade="all, delete-orphan")
    
    # 索引
    __table_args__ = (
        CheckConstraint('likes_count >= 0', name='check_likes_count'),
        CheckConstraint('comments_count >= 0', name='check_comments_count'),
        CheckConstraint('shares_count >= 0', name='check_shares_count'),
        CheckConstraint('views_count >= 0', name='check_views_count'),
        Index('idx_post_author_status', 'author_id', 'status'),
        Index('idx_post_published', 'published_at'),
        Index('idx_post_location', 'latitude', 'longitude'),
        Index('idx_post_tags', 'tags', postgresql_using='gin'),
    )
    
    @hybrid_property
    def is_published(self):
        """是否已发布"""
        return self.status == PostStatus.PUBLISHED
    
    def __repr__(self):
        return f"<Post(id={self.id}, title='{self.title[:50]}...')>"


class Follow(SQLAlchemyBase, TimestampMixin):
    """关注关系表 - 展示多对多关系的实现"""
    __tablename__ = "follows"
    
    # 复合主键
    follower_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), primary_key=True)
    following_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), primary_key=True)
    
    # 关注状态
    is_active = Column(Boolean, default=True, nullable=False)
    
    # 通知设置
    notify_posts = Column(Boolean, default=True, nullable=False)
    notify_stories = Column(Boolean, default=False, nullable=False)
    
    # 关系
    follower = relationship("User", foreign_keys=[follower_id], back_populates="following")
    following = relationship("User", foreign_keys=[following_id], back_populates="followers")
    
    # 约束
    __table_args__ = (
        CheckConstraint('follower_id != following_id', name='check_no_self_follow'),
        Index('idx_follow_follower', 'follower_id', 'created_at'),
        Index('idx_follow_following', 'following_id', 'created_at'),
    )
    
    def __repr__(self):
        return f"<Follow(follower={self.follower_id}, following={self.following_id})>"


class Like(SQLAlchemyBase, TimestampMixin):
    """点赞表 - 展示唯一约束"""
    __tablename__ = "likes"
    
    # 主键
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # 外键
    user_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    post_id = Column(UUID(as_uuid=True), ForeignKey('posts.id'), nullable=False)
    
    # 点赞类型
    like_type = Column(String(20), default='like', nullable=False)  # like, love, laugh, angry
    
    # 关系
    user = relationship("User", back_populates="likes")
    post = relationship("Post", back_populates="likes")
    
    # 唯一约束
    __table_args__ = (
        UniqueConstraint('user_id', 'post_id', name='uq_user_post_like'),
        Index('idx_like_post', 'post_id', 'created_at'),
        Index('idx_like_user', 'user_id', 'created_at'),
    )
    
    def __repr__(self):
        return f"<Like(user={self.user_id}, post={self.post_id}, type={self.like_type})>"


class Comment(SQLAlchemyBase, TimestampMixin):
    """评论表 - 展示自引用关系（树形结构）"""
    __tablename__ = "comments"
    
    # 主键
    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    
    # 外键
    author_id = Column(UUID(as_uuid=True), ForeignKey('users.id'), nullable=False)
    post_id = Column(UUID(as_uuid=True), ForeignKey('posts.id'), nullable=False)
    parent_id = Column(UUID(as_uuid=True), ForeignKey('comments.id'))  # 自引用
    
    # 内容
    content = Column(Text, nullable=False)
    
    # 状态
    is_deleted = Column(Boolean, default=False, nullable=False)
    
    # 统计
    likes_count = Column(Integer, default=0, nullable=False)
    replies_count = Column(Integer, default=0, nullable=False)
    
    # 关系
    author = relationship("User", back_populates="comments")
    post = relationship("Post", back_populates="comments")
    parent = relationship("Comment", remote_side=[id], back_populates="replies")
    replies = relationship("Comment", back_populates="parent", cascade="all, delete-orphan")
    
    # 索引
    __table_args__ = (
        CheckConstraint('likes_count >= 0', name='check_comment_likes_count'),
        CheckConstraint('replies_count >= 0', name='check_replies_count'),
        Index('idx_comment_post', 'post_id', 'created_at'),
        Index('idx_comment_author', 'author_id', 'created_at'),
        Index('idx_comment_parent', 'parent_id', 'created_at'),
    )
    
    @hybrid_property
    def is_reply(self):
        """是否为回复"""
        return self.parent_id is not None
    
    def __repr__(self):
        return f"<Comment(id={self.id}, post={self.post_id}, author={self.author_id})>"


# Pydantic模型用于API
class UserCreate(BaseModel):
    """创建用户请求"""
    username: str
    email: str
    password: str
    first_name: Optional[str] = None
    last_name: Optional[str] = None


class UserResponse(BaseModel):
    """用户响应"""
    id: str
    username: str
    email: str
    first_name: Optional[str]
    last_name: Optional[str]
    bio: Optional[str]
    avatar_url: Optional[str]
    status: UserStatus
    is_verified: bool
    is_private: bool
    followers_count: int
    following_count: int
    posts_count: int
    created_at: datetime
    
    class Config:
        from_attributes = True


class PostCreate(BaseModel):
    """创建帖子请求"""
    title: Optional[str] = None
    content: str
    content_type: str = "text"
    tags: List[str] = []
    category: Optional[str] = None
    location_name: Optional[str] = None
    latitude: Optional[float] = None
    longitude: Optional[float] = None


class PostResponse(BaseModel):
    """帖子响应"""
    id: str
    title: Optional[str]
    content: str
    content_type: str
    status: PostStatus
    likes_count: int
    comments_count: int
    shares_count: int
    views_count: int
    tags: List[str]
    category: Optional[str]
    published_at: Optional[datetime]
    created_at: datetime
    author: UserResponse
    
    class Config:
        from_attributes = True 