# Druid 数据操作

## 目录

- [数据摄入](#数据摄入)
- [SQL 查询](#sql-查询)
- [原生查询](#原生查询)
- [多维分析](#多维分析)

---

## 数据摄入

### HTTP API 摄入

```bash
# 提交任务
curl -X 'POST' \
  'http://localhost:8081/druid/indexer/v1/task' \
  -H 'Content-Type: application/json' \
  -d '{
    "type": "index_parallel",
    "spec": {
      "ioConfig": {
        "type": "index_parallel",
        "inputSource": {
          "type": "local",
          "baseDir": "/data",
          "files": ["data.json"]
        },
        "inputFormat": {
          "type": "json",
          "keepNullColumns": true
        }
      },
      "tuningConfig": {
        "type": "index_parallel",
        "maxRowsPerSegment": 5000000,
        "maxRowsInMemory": 25000
      },
      "dataSchema": {
        "dataSource": "my_datasource",
        "timestampSpec": {
          "column": "timestamp",
          "format": "auto"
        },
        "dimensionsSpec": {
          "dimensions": [
            {"type": "string", "name": "product"},
            {"type": "string", "name": "category"},
            {"type": "long", "name": "price"}
          ],
          "dimensionExclusions": []
        },
        "metricsSpec": [
          {"type": "count", "name": "count"},
          {"type": "sum", "name": "total_sales", "fieldName": "sales"}
        ],
        "granularitySpec": {
          "type": "uniform",
          "segmentGranularity": "day",
          "queryGranularity": "none"
        }
      }
    }
  }'
```

### SQL 摄入

```sql
-- INSERT INTO (从查询导入)
INSERT INTO "target_datasource"
SELECT
  __time,
  product,
  category,
  SUM(sales) AS sales,
  COUNT(*) AS cnt
FROM "source_table"
WHERE __time >= '2024-01-01'
GROUP BY 1, 2, 3
PARTITIONED BY DAY
```

---

## SQL 查询

### 基本查询

```sql
-- 时间序列查询
SELECT
  TIME_FORMAT(__time, 'YYYY-MM-dd') AS date,
  SUM(sales) AS total_sales,
  COUNT(*) AS cnt
FROM my_datasource
WHERE __time >= '2024-01-01' AND __time < '2024-02-01'
GROUP BY TIME_FORMAT(__time, 'YYYY-MM-dd')
ORDER BY date
LIMIT 100;
```

### 多维分析

```sql
-- 分组查询
SELECT
  product,
  category,
  SUM(sales) AS total_sales,
  AVG(price) AS avg_price
FROM my_datasource
WHERE __time >= '2024-01-01'
GROUP BY product, category
ORDER BY total_sales DESC
LIMIT 50;
```

### 嵌套查询

```sql
-- 子查询
SELECT * FROM (
  SELECT
    product,
    SUM(sales) AS total_sales
  FROM my_datasource
  WHERE __time >= '2024-01-01'
  GROUP BY product
)
WHERE total_sales > 10000
ORDER BY total_sales DESC;
```

---

## 原生查询

### JSON 查询

```json
{
  "queryType": "timeseries",
  "dataSource": "my_datasource",
  "intervals": ["2024-01-01/2024-02-01"],
  "granularity": "day",
  "aggregations": [
    {"type": "sum", "name": "sales", "fieldName": "sales"},
    {"type": "count", "name": "cnt"}
  ],
  "filter": {
    "type": "selector",
    "dimension": "product",
    "value": "iphone"
  }
}
```

### TopN 查询

```json
{
  "queryType": "topN",
  "dataSource": "my_datasource",
  "intervals": ["2024-01-01/2024-02-01"],
  "granularity": "day",
  "threshold": 10,
  "metric": "sales",
  "aggregations": [
    {"type": "sum", "name": "sales", "fieldName": "sales"}
  ],
  "dimension": "product"
}
```

### GroupBy 查询

```json
{
  "queryType": "groupBy",
  "dataSource": "my_datasource",
  "intervals": ["2024-01-01/2024-02-01"],
  "granularity": "day",
  "dimensions": [
    {"type": "default", "outputName": "product", "dimension": "product"},
    {"type": "default", "outputName": "category", "dimension": "category"}
  ],
  "aggregations": [
    {"type": "sum", "name": "sales", "fieldName": "sales"}
  ],
  "limitSpec": {
    "type": "default",
    "columns": [{"dimension": "sales", "direction": "descending"}],
    "limit": 100
  }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [README.md](README.md) | 索引文档 |
