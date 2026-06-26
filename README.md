# CVPR Paper Display Pod

CVPR 近年论文大数据展示舱，采用 `Crawler -> Kafka -> Flink -> MySQL -> Gin API -> WebSocket -> Vue/ECharts` 的全链路流处理架构。

## Modules

- `crawler/`: Python 论文采集与 Kafka Producer。
- `flink-job/`: Flink 流处理作业，消费 Kafka、清洗数据并写入 MySQL。
- `backend/`: Go + Gin RESTful API、MySQL binlog 变更监听和 WebSocket 推送。
- `frontend/`: Vue 3 + ECharts 可视化大屏。
- `deploy/mysql/`: MySQL 初始化脚本。
- `docs/`: 架构与开发文档。

## Quick Start

```bash
cp .env.example .env
docker compose up --build
```

启动后访问：

- Frontend: http://localhost:5173
- Backend API: http://localhost:8080/api/health
- Flink UI: http://localhost:18082
- Kafka external listener: `localhost:29092`

## Local Demo Mode

如果 Docker Hub 网络暂时拉不下 Flink、Maven、Node 等镜像，可以先用本地 Go/npm 跑展示舱，只用 Docker 启动 MySQL：

```bash
make demo-db
make seed-demo
make dev-backend
```

另开一个终端：

```bash
make dev-frontend
```

访问 http://localhost:5173 即可看到带演示数据的看板。这个模式用于前后端和数据库展示联调；完整生产链路仍使用 `docker compose up --build` 启动 Kafka/Flink。

发送一批示例论文：

```bash
docker compose --profile tools run --rm crawler
```

爬虫 dry-run，不写 Kafka：

```bash
docker compose --profile tools run --rm crawler python producer.py --sample --dry-run --pretty
```

采集真实 CVPR OpenAccess 页面：

```bash
docker compose --profile tools run --rm crawler python producer.py --years 2024 2023 --fetch-details --limit-per-year 50
```

从本地 JSON / JSONL 导入 Kafka：

```bash
docker compose --profile tools run --rm crawler python producer.py --input-json /app/data/papers.jsonl
```

后端默认优先使用 MySQL row-based binlog 监听 `papers` 表变化，并通过 WebSocket 推送到前端；如果 binlog 权限或连接不可用，会自动退回轮询模式。

## Data Flow

```text
CVPR crawler
  -> Kafka topic cvpr.raw.papers
  -> Flink cleaning and normalization
  -> MySQL tables
  -> Gin REST APIs
  -> WebSocket push
  -> ECharts dashboard
```

Flink 作业会在单条论文维度内用 MySQL 事务写入主表、维表和物化统计表，并通过 `paper_ingest_events` 去重，避免 Kafka 重放导致年度趋势、词云、作者排行等统计重复累计。

如需根据当前明细表重建统计表：

```bash
make rebuild-stats
```

如果 Docker Hub 网络慢，可在 `.env` 中覆盖基础镜像，例如 `MYSQL_IMAGE=docker.m.daocloud.io/library/mysql:8.4`。Compose 已支持 `MYSQL_IMAGE`、`KAFKA_IMAGE`、`FLINK_IMAGE`、`MAVEN_IMAGE`、`NODE_IMAGE`、`NGINX_IMAGE`、`GO_IMAGE`、`ALPINE_IMAGE` 和 `PYTHON_IMAGE`。

## API Snapshot

- `GET /api/papers?q=&year=&limit=&offset=`: 论文检索、年份筛选和分页。
- `GET /api/stats/summary`: 论文、年份、关键词、作者、机构总览。
- `GET /api/stats/years`: 已入库年份列表。
- `GET /api/stats/yearly`: 年度论文趋势。
- `GET /api/stats/keywords`: 关键词词云数据。
- `GET /api/stats/authors`: 作者排行。
- `GET /api/stats/institutions`: 机构排行。
- `GET /api/ws`: 实时论文更新推送。

## Dashboard

前端看板支持关键词检索、年份筛选、结果分页、论文详情抽屉、图表自动刷新和 WebSocket 实时同步。

## Crawler

`crawler/producer.py` 支持三种数据源：内置示例、CVPR OpenAccess 页面、本地 JSON/JSONL。输出统一为 Kafka 消息契约中的结构化论文 JSON，并自动生成稳定 `source_id`、清洗作者列表、从标题和摘要提取关键词。

常用参数：

- `--sample`: 使用内置演示数据。
- `--years 2024 2023`: 指定 CVPR 年份。
- `--fetch-details`: 进入论文详情页抓取摘要和 PDF 地址。
- `--limit-per-year 50`: 每年最多采集 50 篇，便于演示和调试。
- `--input-json papers.jsonl`: 从本地 JSON 数组或 JSONL 文件导入。
- `--dry-run --pretty`: 打印 JSON，不发送 Kafka。

## Next Milestones

1. 增加机构抽取和作者机构关联。
2. 增加 Flink 窗口统计表和 checkpoint。
3. 增加前端图表联动和主题下钻。
4. 增加 CI、单元测试和端到端冒烟测试。
