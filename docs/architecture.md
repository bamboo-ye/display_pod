# Architecture

## Message Contract

Kafka topic: `cvpr.raw.papers`

```json
{
  "source_id": "cvpr-2024-example",
  "year": 2024,
  "title": "Example CVPR Paper",
  "authors": ["Alice Zhang", "Bob Wang"],
  "institutions": ["Example University"],
  "abstract": "Paper abstract...",
  "keywords": ["vision-language", "detection"],
  "paper_url": "https://openaccess.thecvf.com/...",
  "pdf_url": "https://openaccess.thecvf.com/...",
  "crawled_at": "2026-06-25T00:00:00Z"
}
```

## Storage Model

- `papers`: 论文主表。
- `paper_authors`: 作者明细。
- `paper_institutions`: 机构明细。
- `paper_keywords`: 关键词明细。
- `paper_ingest_events`: Flink 去重计数表，保证 Kafka 重放时同一 `source_id` 只累计一次统计。
- `stats_yearly`: Flink 维护的年度论文数量物化统计。
- `stats_keywords`: Flink 维护的关键词热度物化统计。
- `stats_authors`: Flink 维护的作者热度物化统计。
- `stats_institutions`: Flink 维护的机构热度物化统计。

统计接口优先读取 Flink 侧维护的物化统计表；如果统计表为空，则从明细表 SQL 聚合兜底，方便本地开发和历史数据回填。

## Flink Computation

Flink 消费 `cvpr.raw.papers` 后执行：

1. JSON 解析和字段清洗。
2. 作者、机构、关键词去重。
3. 论文主表 upsert。
4. 作者、机构、关键词明细表写入。
5. 通过 `paper_ingest_events` 判断是否首次看到该论文。
6. 首次入库时累加年度、关键词、作者、机构物化统计。

MySQL 写入在单条论文维度内使用事务，避免主表、维表和统计表部分成功导致数据不一致。

## Realtime Sync

MySQL 容器启用 row-based binlog：

- `server-id=101`
- `log-bin=mysql-bin`
- `binlog-format=ROW`
- `binlog-row-image=FULL`

初始化脚本创建 `replicator` 用户并授予复制监听权限。后端启动后读取当前 binlog 位点，从该位点开始监听 `cvpr_display.papers` 的 insert/update 事件，转换成 `paper.updated` WebSocket 消息推送给前端。

如果 binlog 不可用，例如本地单独启动后端但 MySQL 未开启 binlog，后端会自动降级为按 `updated_at` 字段轮询。
