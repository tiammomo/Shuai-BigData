# Apache Kylin 指南

## 目录

- [Cube 构建](#cube-构建)
- [查询优化](#查询优化)
- [Spark 构建](#spark-构建)
- [最佳实践](#最佳实践)

---

## Cube 构建

### 创建 Cube

```sql
-- 1. 创建 Model (数据模型)
CREATE MODEL KYLIN.SALES_MODEL
SET DEFAULT_DS = KYLIN.DEFAULT_CLICKHOUSE_DS

DIMENSIONS (
    KYLIN.SALES_USER.USER_ID,
    KYLIN.SALES_USER.USER_NAME,
    KYLIN.SALES_PRODUCT.PRODUCT_ID,
    KYLIN.SALES_PRODUCT.PRODUCT_NAME,
    KYLIN.SALES_PRODUCT.CATEGORY,
    KYLIN.SALES_REGION.REGION_ID,
    KYLIN.SALES_REGION.REGION_NAME
)

MEASURES (
    KYLIN.SALES_FACT.PRICE,
    KYLIN.SALES_FACT.QUANTITY,
    KYLIN.SALES_FACT.AMOUNT
)

FILTER (
    KYLIN.SALES_FACT.STATUS = 'PAID'
);

-- 2. 创建 Cube
CREATE CUBE KYLIN.SALES_CUBE
(
    CUBE_NAME: SALES_CUBE,
    MODEL_NAME: SALES_MODEL,
    DESCRIPTION: '销售分析立方体'
)

PARTITION BY (
    KYLIN.SALES_FACT.PAY_DATE
)

AGGREGATIONS (
    SUM(KYLIN.SALES_FACT.AMOUNT),
    SUM(KYLIN.SALES_FACT.QUANTITY),
    COUNT(KYLIN.SALES_FACT.ORDER_ID)
)
DIMENSIONS (
    USER_ID: FILTER = ALL,
    USER_NAME: FILTER = ALL,
    PRODUCT_ID: FILTER = ALL,
    PRODUCT_NAME: FILTER = ALL,
    CATEGORY: FILTER = ALL,
    REGION_ID: FILTER = ALL,
    REGION_NAME: FILTER = ALL
);
```

### Cube 配置

```sql
-- 设置聚合组
ALTER CUBE KYLIN.SALES_CUBE
SET AGGR_GROUP = 'GROUP1';

-- 设置 RowKey 顺序
ALTER CUBE KYLIN.SALES_CUBE
SET ROWKEY = (
    PAY_DATE,
    PRODUCT_ID,
    USER_ID,
    REGION_ID
);

-- 设置维度编码
ALTER CUBE KYLIN.SALES_CUBE
SET MAPPING_TYPE = (
    PRODUCT_ID: 'dict',
    USER_ID: 'dict',
    REGION_ID: 'dict'
);
```

### 构建 Cube

```bash
# 触发构建
curl -X PUT \
  'http://localhost:7070/kylin/api/cubes/SALES_CUBE/build' \
  -H 'Content-Type: application/json' \
  -d '{
    "startTime": "1704067200000",
    "endTime": "1706745600000",
    "buildType": "BUILD"
  }'

# 查看构建状态
curl -X GET 'http://localhost:7070/kylin/api/cubes/SALES_CUBE'

# 强制刷新
curl -X PUT 'http://localhost:7070/kylin/api/cubes/SALES_Cube/refresh'
```

---

## 查询优化

### 查询执行

```sql
-- 标准 SQL 查询
SELECT
    product_category,
    region_name,
    SUM(amount) AS total_sales,
    COUNT(DISTINCT user_id) AS user_count
FROM KYLIN.SALES_CUBE
WHERE pay_date >= '2024-01-01' AND pay_date < '2024-02-01'
GROUP BY product_category, region_name
ORDER BY total_sales DESC
LIMIT 100;
```

### 查询重写

```sql
-- 启用查询下压
SET ENABLE_QUERY_PUSH_DOWN = true;

-- 查看执行计划
EXPLAIN SELECT * FROM KYLIN.SALES_CUBE;
```

---

## Spark 构建

### 配置 Spark

```xml
<!-- kylin.properties -->
kylin.engine.spark-conf-spark.driver.memory=4G
kylin.engine.spark-conf-spark.executor.memory=8G
kylin.engine.spark-conf-spark.executor.cores=4
kylin.engine.spark-conf-spark.executor.instances=10
kylin.engine.spark-conf-spark.yarn.queue=default
```

### Spark 构建步骤

```bash
# 1. 分配资源
kylin.sh org.apache.kylin.engine.spark.JobStepAllocation -cube KYLIN.SALES_CUBE

# 2. 分布 Flat Table
spark-submit --class org.apache.kylin.engine.spark.SparkFlatTableJob \
  --master yarn --deploy-mode cluster \
  kylin-job.jar KYLIN.SALES_CUBE

# 3. 构建维度字典
spark-submit --class org.apache.kylin.engine.spark.SparkDictionaryBuildJob \
  kylin-job.jar KYLIN.SALES_CUBE

# 4. 构建 Cube
spark-submit --class org.apache.kylin.engine.spark.SparkCubingJob \
  kylin-job.jar KYLIN.SALES_CUBE
```

---

## 最佳实践

### Cube 设计

```sql
-- 1. 合理设置维度数量
-- 建议: 10-20 个维度

-- 2. 设置必要的聚合组
-- 将高频查询的维度放在同一组

-- 3. 使用字典编码
-- 对于基数 < 100万 的维度

-- 4. 设置强制维度
-- 高基数的 join 维度设为强制维度
```

### 性能优化

```sql
-- 1. 使用预计算
-- 热点查询预计算

-- 2. 分区裁剪
-- 按时间分区

-- 3. 合理设置粒度
-- 根据数据量和查询需求选择

-- 4. 清理历史数据
-- 定期清理旧 segment
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [README.md](README.md) | 索引文档 |
