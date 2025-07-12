#!/bin/bash

# 数据密集型应用系统 - 服务启动脚本

set -e

echo "🚀 启动数据密集型应用系统..."

# 检查Docker是否运行
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker未运行，请先启动Docker"
    exit 1
fi

# 检查docker-compose是否存在
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose未安装"
    exit 1
fi

echo "📦 启动基础设施服务..."
docker-compose up -d

echo "⏳ 等待服务启动..."

# 等待PostgreSQL
echo "等待PostgreSQL启动..."
until docker exec ddia_postgres pg_isready -U ddia_user > /dev/null 2>&1; do
    sleep 1
done
echo "✅ PostgreSQL已就绪"

# 等待Redis
echo "等待Redis启动..."
until docker exec ddia_redis redis-cli ping > /dev/null 2>&1; do
    sleep 1
done
echo "✅ Redis已就绪"

# 等待MongoDB
echo "等待MongoDB启动..."
until docker exec ddia_mongodb mongosh --eval "db.adminCommand('ping')" > /dev/null 2>&1; do
    sleep 1
done
echo "✅ MongoDB已就绪"

# 等待Elasticsearch
echo "等待Elasticsearch启动..."
until curl -s http://localhost:9200/_cluster/health > /dev/null 2>&1; do
    sleep 1
done
echo "✅ Elasticsearch已就绪"

# 等待Kafka
echo "等待Kafka启动..."
until docker exec ddia_kafka kafka-topics --bootstrap-server localhost:9092 --list > /dev/null 2>&1; do
    sleep 1
done
echo "✅ Kafka已就绪"

# 等待Neo4j
echo "等待Neo4j启动..."
until curl -s http://localhost:7474 > /dev/null 2>&1; do
    sleep 1
done
echo "✅ Neo4j已就绪"

echo "🎉 所有服务已启动完成！"

echo "📊 服务访问地址："
echo "  - API文档: http://localhost:8000/docs"
echo "  - Grafana监控: http://localhost:3000 (admin/admin)"
echo "  - Prometheus: http://localhost:9090"
echo "  - Kibana日志: http://localhost:5601"
echo "  - Neo4j浏览器: http://localhost:7474 (neo4j/ddia_password)"
echo "  - Flink UI: http://localhost:8081"
echo "  - MinIO控制台: http://localhost:9001 (ddia_user/ddia_password)"
echo "  - Jaeger追踪: http://localhost:16686"

echo ""
echo "🔧 下一步："
echo "  1. 初始化数据库: python src/setup/init_database.py"
echo "  2. 加载示例数据: python src/setup/load_sample_data.py"
echo "  3. 启动API服务: python src/api/main.py"
echo "  4. 运行演示案例: python examples/demo.py" 