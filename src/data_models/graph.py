"""
图型数据模型
展示图数据库的设计，包括节点、关系、图查询等
用于社交网络分析、推荐系统等场景
"""
from datetime import datetime
from typing import List, Dict, Any, Optional, Set
from enum import Enum
from pydantic import Field
from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable

from .base import BaseModel, Entity


class NodeType(str, Enum):
    """节点类型"""
    USER = "User"
    POST = "Post"
    HASHTAG = "Hashtag"
    LOCATION = "Location"
    INTEREST = "Interest"
    COMMUNITY = "Community"


class RelationType(str, Enum):
    """关系类型"""
    FOLLOWS = "FOLLOWS"
    LIKES = "LIKES"
    COMMENTS = "COMMENTS"
    SHARES = "SHARES"
    MENTIONS = "MENTIONS"
    TAGGED_WITH = "TAGGED_WITH"
    LOCATED_AT = "LOCATED_AT"
    INTERESTED_IN = "INTERESTED_IN"
    MEMBER_OF = "MEMBER_OF"
    SIMILAR_TO = "SIMILAR_TO"
    RECOMMENDS = "RECOMMENDS"


class InfluenceLevel(str, Enum):
    """影响力级别"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CELEBRITY = "celebrity"


# 图节点模型
class GraphNode(BaseModel):
    """图节点基类"""
    node_id: str
    node_type: NodeType
    properties: Dict[str, Any] = Field(default_factory=dict)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class UserNode(GraphNode):
    """用户节点"""
    username: str
    display_name: Optional[str] = None
    bio: Optional[str] = None
    verified: bool = False
    influence_level: InfluenceLevel = InfluenceLevel.LOW
    
    # 社交指标
    followers_count: int = 0
    following_count: int = 0
    posts_count: int = 0
    
    # 活跃度指标
    activity_score: float = 0.0
    engagement_rate: float = 0.0
    
    # 位置信息
    location: Optional[str] = None
    timezone: Optional[str] = None
    
    # 兴趣标签
    interests: List[str] = Field(default_factory=list)
    
    def __init__(self, **data):
        data['node_type'] = NodeType.USER
        super().__init__(**data)


class PostNode(GraphNode):
    """帖子节点"""
    title: Optional[str] = None
    content: str
    content_type: str = "text"
    author_id: str
    
    # 互动指标
    likes_count: int = 0
    comments_count: int = 0
    shares_count: int = 0
    views_count: int = 0
    
    # 传播指标
    reach: int = 0  # 触达人数
    impressions: int = 0  # 展示次数
    viral_coefficient: float = 0.0  # 病毒系数
    
    # 内容分析
    sentiment_score: float = 0.0  # 情感分数 (-1到1)
    quality_score: float = 0.0   # 质量分数 (0到1)
    
    # 时间信息
    published_at: Optional[datetime] = None
    
    def __init__(self, **data):
        data['node_type'] = NodeType.POST
        super().__init__(**data)


class HashtagNode(GraphNode):
    """话题标签节点"""
    tag: str
    normalized_tag: str  # 标准化标签
    
    # 热度指标
    usage_count: int = 0
    trending_score: float = 0.0
    
    # 分类
    category: Optional[str] = None
    
    def __init__(self, **data):
        data['node_type'] = NodeType.HASHTAG
        data['normalized_tag'] = data.get('tag', '').lower().strip('#')
        super().__init__(**data)


class LocationNode(GraphNode):
    """位置节点"""
    name: str
    latitude: float
    longitude: float
    country: Optional[str] = None
    city: Optional[str] = None
    
    # 活跃度
    checkin_count: int = 0
    post_count: int = 0
    
    def __init__(self, **data):
        data['node_type'] = NodeType.LOCATION
        super().__init__(**data)


class InterestNode(GraphNode):
    """兴趣节点"""
    name: str
    category: str
    description: Optional[str] = None
    
    # 受欢迎程度
    follower_count: int = 0
    post_count: int = 0
    
    def __init__(self, **data):
        data['node_type'] = NodeType.INTEREST
        super().__init__(**data)


class CommunityNode(GraphNode):
    """社群节点"""
    name: str
    description: Optional[str] = None
    
    # 社群指标
    member_count: int = 0
    activity_level: float = 0.0
    cohesion_score: float = 0.0  # 凝聚度
    
    def __init__(self, **data):
        data['node_type'] = NodeType.COMMUNITY
        super().__init__(**data)


# 图关系模型
class GraphRelation(BaseModel):
    """图关系基类"""
    relation_id: str
    relation_type: RelationType
    source_id: str
    target_id: str
    properties: Dict[str, Any] = Field(default_factory=dict)
    weight: float = 1.0  # 关系权重
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class FollowRelation(GraphRelation):
    """关注关系"""
    # 关注强度
    interaction_frequency: float = 0.0  # 互动频率
    mutual: bool = False  # 是否互关
    
    # 通知设置
    notifications_enabled: bool = True
    
    def __init__(self, **data):
        data['relation_type'] = RelationType.FOLLOWS
        super().__init__(**data)


class LikeRelation(GraphRelation):
    """点赞关系"""
    like_type: str = "like"  # like, love, laugh, angry
    
    def __init__(self, **data):
        data['relation_type'] = RelationType.LIKES
        super().__init__(**data)


class CommentRelation(GraphRelation):
    """评论关系"""
    comment_content: str
    sentiment: Optional[str] = None  # positive, negative, neutral
    
    def __init__(self, **data):
        data['relation_type'] = RelationType.COMMENTS
        super().__init__(**data)


class SimilarityRelation(GraphRelation):
    """相似性关系"""
    similarity_score: float  # 相似度分数 (0到1)
    similarity_type: str  # content, behavior, interest
    
    def __init__(self, **data):
        data['relation_type'] = RelationType.SIMILAR_TO
        super().__init__(**data)


# 图数据库操作类
class GraphDatabase:
    """图数据库操作类"""
    
    def __init__(self, uri: str, username: str, password: str):
        self.driver = GraphDatabase.driver(uri, auth=(username, password))
    
    def close(self):
        """关闭连接"""
        self.driver.close()
    
    def create_node(self, node: GraphNode) -> bool:
        """创建节点"""
        with self.driver.session() as session:
            try:
                query = f"""
                CREATE (n:{node.node_type.value} {{
                    node_id: $node_id,
                    created_at: datetime($created_at)
                }})
                SET n += $properties
                RETURN n
                """
                session.run(query, 
                          node_id=node.node_id,
                          created_at=node.created_at.isoformat(),
                          properties=node.dict(exclude={'node_id', 'node_type', 'created_at', 'updated_at'}))
                return True
            except Exception as e:
                print(f"Error creating node: {e}")
                return False
    
    def create_relation(self, relation: GraphRelation) -> bool:
        """创建关系"""
        with self.driver.session() as session:
            try:
                query = f"""
                MATCH (a), (b)
                WHERE a.node_id = $source_id AND b.node_id = $target_id
                CREATE (a)-[r:{relation.relation_type.value} {{
                    relation_id: $relation_id,
                    weight: $weight,
                    created_at: datetime($created_at)
                }}]->(b)
                SET r += $properties
                RETURN r
                """
                session.run(query,
                          source_id=relation.source_id,
                          target_id=relation.target_id,
                          relation_id=relation.relation_id,
                          weight=relation.weight,
                          created_at=relation.created_at.isoformat(),
                          properties=relation.dict(exclude={'relation_id', 'relation_type', 'source_id', 'target_id', 'weight', 'created_at', 'updated_at'}))
                return True
            except Exception as e:
                print(f"Error creating relation: {e}")
                return False
    
    def find_shortest_path(self, source_id: str, target_id: str, max_length: int = 6) -> List[Dict[str, Any]]:
        """查找最短路径"""
        with self.driver.session() as session:
            query = """
            MATCH path = shortestPath((a)-[*..%d]-(b))
            WHERE a.node_id = $source_id AND b.node_id = $target_id
            RETURN path
            """ % max_length
            
            result = session.run(query, source_id=source_id, target_id=target_id)
            return [record["path"] for record in result]
    
    def find_mutual_connections(self, user1_id: str, user2_id: str) -> List[Dict[str, Any]]:
        """查找共同关注"""
        with self.driver.session() as session:
            query = """
            MATCH (u1:User)-[:FOLLOWS]->(mutual)<-[:FOLLOWS]-(u2:User)
            WHERE u1.node_id = $user1_id AND u2.node_id = $user2_id
            RETURN mutual
            """
            result = session.run(query, user1_id=user1_id, user2_id=user2_id)
            return [record["mutual"] for record in result]
    
    def find_influencers(self, limit: int = 10) -> List[Dict[str, Any]]:
        """查找影响者"""
        with self.driver.session() as session:
            query = """
            MATCH (u:User)
            OPTIONAL MATCH (u)<-[:FOLLOWS]-(follower)
            WITH u, count(follower) as followers_count
            RETURN u, followers_count
            ORDER BY followers_count DESC
            LIMIT $limit
            """
            result = session.run(query, limit=limit)
            return [(record["u"], record["followers_count"]) for record in result]
    
    def find_trending_hashtags(self, days: int = 7, limit: int = 10) -> List[Dict[str, Any]]:
        """查找热门话题"""
        with self.driver.session() as session:
            query = """
            MATCH (p:Post)-[:TAGGED_WITH]->(h:Hashtag)
            WHERE p.published_at > datetime() - duration({days: $days})
            WITH h, count(p) as usage_count
            RETURN h, usage_count
            ORDER BY usage_count DESC
            LIMIT $limit
            """
            result = session.run(query, days=days, limit=limit)
            return [(record["h"], record["usage_count"]) for record in result]
    
    def recommend_users(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        """推荐用户"""
        with self.driver.session() as session:
            query = """
            MATCH (u:User)-[:FOLLOWS]->(followed)-[:FOLLOWS]->(recommended)
            WHERE u.node_id = $user_id 
            AND NOT (u)-[:FOLLOWS]->(recommended)
            AND u <> recommended
            WITH recommended, count(*) as mutual_follows
            RETURN recommended, mutual_follows
            ORDER BY mutual_follows DESC
            LIMIT $limit
            """
            result = session.run(query, user_id=user_id, limit=limit)
            return [(record["recommended"], record["mutual_follows"]) for record in result]
    
    def detect_communities(self, algorithm: str = "louvain") -> List[Dict[str, Any]]:
        """社群检测"""
        with self.driver.session() as session:
            if algorithm == "louvain":
                query = """
                CALL gds.louvain.stream('socialGraph')
                YIELD nodeId, communityId
                RETURN gds.util.asNode(nodeId) as user, communityId
                ORDER BY communityId
                """
            else:
                # 简单的基于连通分量的社群检测
                query = """
                CALL gds.wcc.stream('socialGraph')
                YIELD nodeId, componentId
                RETURN gds.util.asNode(nodeId) as user, componentId as communityId
                ORDER BY componentId
                """
            
            result = session.run(query)
            return [(record["user"], record["communityId"]) for record in result]
    
    def calculate_centrality(self, centrality_type: str = "pagerank") -> List[Dict[str, Any]]:
        """计算中心性"""
        with self.driver.session() as session:
            if centrality_type == "pagerank":
                query = """
                CALL gds.pageRank.stream('socialGraph')
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId) as user, score
                ORDER BY score DESC
                """
            elif centrality_type == "betweenness":
                query = """
                CALL gds.betweenness.stream('socialGraph')
                YIELD nodeId, score
                RETURN gds.util.asNode(nodeId) as user, score
                ORDER BY score DESC
                """
            else:  # degree centrality
                query = """
                MATCH (u:User)
                OPTIONAL MATCH (u)-[:FOLLOWS]-(connected)
                WITH u, count(connected) as degree
                RETURN u as user, degree as score
                ORDER BY degree DESC
                """
            
            result = session.run(query)
            return [(record["user"], record["score"]) for record in result]


# 图分析工具
class SocialNetworkAnalyzer:
    """社交网络分析工具"""
    
    def __init__(self, graph_db: GraphDatabase):
        self.graph_db = graph_db
    
    def analyze_user_influence(self, user_id: str) -> Dict[str, Any]:
        """分析用户影响力"""
        with self.graph_db.driver.session() as session:
            # 获取用户的基本指标
            query = """
            MATCH (u:User {node_id: $user_id})
            OPTIONAL MATCH (u)<-[:FOLLOWS]-(follower)
            OPTIONAL MATCH (u)-[:FOLLOWS]->(following)
            OPTIONAL MATCH (u)-[:AUTHORED]->(post)
            OPTIONAL MATCH (post)<-[:LIKES]-(like)
            RETURN u,
                   count(DISTINCT follower) as followers,
                   count(DISTINCT following) as following,
                   count(DISTINCT post) as posts,
                   count(DISTINCT like) as total_likes
            """
            result = session.run(query, user_id=user_id).single()
            
            if not result:
                return None
            
            followers = result["followers"]
            following = result["following"]
            posts = result["posts"]
            total_likes = result["total_likes"]
            
            # 计算影响力指标
            follower_ratio = followers / (following + 1)  # 粉丝关注比
            engagement_rate = total_likes / (posts + 1)   # 平均每帖点赞数
            influence_score = (followers * 0.4 + follower_ratio * 0.3 + engagement_rate * 0.3)
            
            return {
                "user_id": user_id,
                "followers": followers,
                "following": following,
                "posts": posts,
                "total_likes": total_likes,
                "follower_ratio": follower_ratio,
                "engagement_rate": engagement_rate,
                "influence_score": influence_score
            }
    
    def find_key_opinion_leaders(self, topic: str, limit: int = 10) -> List[Dict[str, Any]]:
        """查找意见领袖"""
        with self.graph_db.driver.session() as session:
            query = """
            MATCH (u:User)-[:AUTHORED]->(p:Post)-[:TAGGED_WITH]->(h:Hashtag {tag: $topic})
            WITH u, count(p) as posts_count, 
                 sum(p.likes_count) as total_likes,
                 sum(p.shares_count) as total_shares
            MATCH (u)<-[:FOLLOWS]-(follower)
            WITH u, posts_count, total_likes, total_shares, count(follower) as followers
            RETURN u, posts_count, total_likes, total_shares, followers,
                   (total_likes + total_shares * 2 + followers * 0.1) as influence_score
            ORDER BY influence_score DESC
            LIMIT $limit
            """
            result = session.run(query, topic=topic, limit=limit)
            return [dict(record) for record in result]
    
    def analyze_content_spread(self, post_id: str) -> Dict[str, Any]:
        """分析内容传播"""
        with self.graph_db.driver.session() as session:
            query = """
            MATCH (p:Post {node_id: $post_id})
            OPTIONAL MATCH path = (p)<-[:SHARES*1..3]-(sharer)
            WITH p, path, length(path) as depth
            RETURN p,
                   count(DISTINCT path) as total_shares,
                   max(depth) as max_depth,
                   avg(depth) as avg_depth
            """
            result = session.run(query, post_id=post_id).single()
            return dict(result) if result else None


# 图数据初始化
def create_graph_schema(graph_db: GraphDatabase):
    """创建图数据库模式"""
    with graph_db.driver.session() as session:
        # 创建索引
        indexes = [
            "CREATE INDEX user_node_id IF NOT EXISTS FOR (u:User) ON (u.node_id)",
            "CREATE INDEX post_node_id IF NOT EXISTS FOR (p:Post) ON (p.node_id)",
            "CREATE INDEX hashtag_tag IF NOT EXISTS FOR (h:Hashtag) ON (h.tag)",
            "CREATE INDEX location_name IF NOT EXISTS FOR (l:Location) ON (l.name)",
        ]
        
        for index in indexes:
            session.run(index)
        
        # 创建约束
        constraints = [
            "CREATE CONSTRAINT user_unique_id IF NOT EXISTS FOR (u:User) REQUIRE u.node_id IS UNIQUE",
            "CREATE CONSTRAINT post_unique_id IF NOT EXISTS FOR (p:Post) REQUIRE p.node_id IS UNIQUE",
        ]
        
        for constraint in constraints:
            session.run(constraint) 