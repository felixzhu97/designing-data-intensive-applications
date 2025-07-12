# 🚀 快速入门指南

本指南将帮助您快速启动和体验《数据密集型应用系统设计》完整案例。

## 🔧 环境要求

- **Docker & Docker Compose** - 用于运行基础设施服务
- **Python 3.9+** - 运行应用程序
- **8GB+ RAM** - 推荐内存大小
- **10GB+ 磁盘空间** - 用于存储数据和日志

## 📦 快速启动

### 1. 克隆项目

```bash
git clone <repo-url>
cd designing-data-intensive-applications
```

### 2. 安装 Python 依赖

```bash
pip install -r requirements.txt
```

### 3. 启动基础设施

```bash
# 启动所有服务（PostgreSQL、Redis、MongoDB、Kafka等）
./scripts/start_services.sh
```

等待所有服务启动完成（约 2-3 分钟）。

### 4. 运行演示程序

```bash
# 运行完整演示，展示所有核心概念
python examples/demo.py
```

## 🎯 核心功能演示

### 📊 数据模型

```bash
# 关系型数据模型演示
python examples/relational_demo.py

# 文档型数据模型演示
python examples/document_demo.py

# 图型数据模型演示
python examples/graph_demo.py

# 时序数据模型演示
python examples/timeseries_demo.py
```

### 💾 存储引擎

```bash
# LSM-Tree存储引擎演示
python examples/lsm_tree_demo.py

# B-Tree索引演示
python examples/btree_demo.py

# 列式存储演示
python examples/column_store_demo.py
```

### 🔄 分布式系统

```bash
# 数据复制演示
python examples/replication_demo.py

# 数据分区演示
python examples/partitioning_demo.py

# 一致性演示
python examples/consistency_demo.py
```

### 🌊 流处理

```bash
# 启动流处理任务
python src/stream_processing/main.py

# 发送测试事件
python examples/stream_events.py
```

## 🌐 Web 界面访问

启动成功后，您可以访问以下 Web 界面：

| 服务             | 地址                       | 用户名/密码             | 说明                        |
| ---------------- | -------------------------- | ----------------------- | --------------------------- |
| **API 文档**     | http://localhost:8000/docs | -                       | FastAPI 自动生成的 API 文档 |
| **Grafana 监控** | http://localhost:3000      | admin/admin             | 系统监控面板                |
| **Prometheus**   | http://localhost:9090      | -                       | 指标收集系统                |
| **Kibana 日志**  | http://localhost:5601      | -                       | 日志分析和可视化            |
| **Neo4j 浏览器** | http://localhost:7474      | neo4j/ddia_password     | 图数据库浏览器              |
| **Flink UI**     | http://localhost:8081      | -                       | 流处理任务管理              |
| **MinIO 控制台** | http://localhost:9001      | ddia_user/ddia_password | 对象存储管理                |
| **Jaeger 追踪**  | http://localhost:16686     | -                       | 分布式追踪系统              |

## 📈 性能测试

```bash
# 运行性能基准测试
python tests/performance/benchmark.py

# 生成性能报告
python tests/performance/generate_report.py

# 压力测试
python tests/performance/stress_test.py
```

## 🧪 概念验证

### 1. 数据一致性

```bash
# 演示不同一致性级别
python examples/consistency_levels.py
```

### 2. 故障恢复

```bash
# 模拟节点故障
python examples/fault_tolerance.py
```

### 3. 扩容演示

```bash
# 演示水平扩容
python examples/scaling_demo.py
```

## 🔍 深入探索

### 查看源代码结构

```
src/
├── data_models/          # 数据模型（关系型、文档型、图型、时序）
├── storage/             # 存储引擎（LSM-Tree、B-Tree、列式存储）
├── replication/         # 复制系统（主从、多主、无主）
├── partitioning/        # 分区系统（哈希、范围、一致性哈希）
├── transactions/        # 事务处理（ACID、隔离级别、分布式事务）
├── batch_processing/    # 批处理（MapReduce、ETL、数据管道）
├── stream_processing/   # 流处理（事件流、CEP、窗口操作）
├── api/                # REST API服务
└── monitoring/         # 监控和可观测性
```

### 阅读文档

```
docs/
├── concepts/           # 核心概念说明
├── architecture/       # 系统架构设计
├── deployment/         # 部署指南
└── api/               # API参考文档
```

## 🐛 故障排除

### 常见问题

**1. Docker 服务启动失败**

```bash
# 检查Docker状态
docker info

# 查看服务日志
docker-compose logs <service-name>

# 重启服务
docker-compose restart <service-name>
```

**2. 端口冲突**

```bash
# 检查端口占用
lsof -i :5432  # PostgreSQL
lsof -i :6379  # Redis
lsof -i :9092  # Kafka

# 修改docker-compose.yml中的端口映射
```

**3. 内存不足**

```bash
# 检查内存使用
docker stats

# 减少服务数量或调整内存限制
# 编辑docker-compose.yml，添加memory限制
```

**4. Python 依赖问题**

```bash
# 创建虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/Mac
# 或
venv\Scripts\activate     # Windows

# 重新安装依赖
pip install -r requirements.txt
```

### 获取帮助

- 查看日志：`docker-compose logs -f`
- 检查服务状态：`docker-compose ps`
- 重启所有服务：`docker-compose restart`
- 完全重建：`docker-compose down && docker-compose up -d`

## 📚 学习路径

### 初学者

1. 🏁 **快速体验**：运行 `python examples/demo.py`
2. 📖 **理解概念**：阅读 `docs/concepts/` 下的文档
3. 🔬 **实验功能**：尝试不同的演示程序

### 进阶用户

1. 🔧 **修改配置**：调整 `config/settings.py` 中的参数
2. 📊 **分析性能**：运行性能测试和监控
3. 🏗️ **扩展功能**：添加自定义的数据模型或存储引擎

### 专家用户

1. 🎯 **深度定制**：修改核心算法实现
2. 🌐 **生产部署**：参考 `docs/deployment/` 进行生产环境部署
3. 📈 **性能优化**：基于监控数据进行系统调优

## 🎓 更多资源

- 📚 **原书**：[《数据密集型应用系统设计》](https://dataintensive.net/)
- 🎥 **视频教程**：Martin Kleppmann 的相关演讲
- 💬 **社区讨论**：加入相关技术社区
- 📄 **论文阅读**：深入了解分布式系统理论

---

🎉 **恭喜！** 您已经成功启动了完整的数据密集型应用系统。开始探索现代数据系统的精彩世界吧！
