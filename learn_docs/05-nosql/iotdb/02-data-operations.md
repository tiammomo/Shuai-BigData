# IoTDB 数据操作

## 目录

- [数据写入](#数据写入)
- [数据查询](#数据查询)
- [Schema 管理](#schema-管理)
- [TsFile 操作](#tsfile-操作)
- [JDBC API](#jdbc-api)

---

## 数据写入

### JDBC 写入

```java
import java.sql.*;

Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
Connection connection = DriverManager.getConnection(
    "jdbc:iotdb://localhost:6667/", "root", "root");

Statement statement = connection.createStatement();

// 单条插入
statement.execute(
    "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) " +
    "VALUES(1704880800000, 25.5)"
);

// 批量插入
statement.execute(
    "INSERT INTO root.ln.wf01.wt01(timestamp, temperature, humidity) " +
    "VALUES(1704880800000, 25.5, 60.2), " +
    "      (1704880850000, 26.0, 61.5), " +
    "      (1704880900000, 25.8, 59.8)"
);

// 补齐路径
statement.execute(
    "INSERT INTO root.ln.wf01.wt01(timestamp, temperature) " +
    "VALUES(1704880800000, 25.5), " +
    "      (1704880900000, 26.0)"
);
```

### Session API 写入

```java
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.record.value.TimeValue;
import org.apache.iotdb.tsfile.record.value.Value;

Session session = new Session.Builder()
    .host("localhost")
    .port(6667)
    .username("root")
    .password("root")
    .build();

session.open();

// 单条记录插入
String deviceId = "root.ln.wf01.wt01";
long timestamp = System.currentTimeMillis();
String[] measurements = {"temperature", "humidity"};
TSDataType[] types = {TSDataType.FLOAT, TSDataType.FLOAT};
Object[] values = {25.5f, 60.2f};

session.insertRecord(deviceId, timestamp, measurements, types, values);

// 批量记录插入
List<String> deviceIds = Arrays.asList(
    "root.ln.wf01.wt01",
    "root.ln.wf01.wt02"
);
List<long[]> timestamps = Arrays.asList(
    new long[]{1704880800000L, 1704880900000L},
    new long[]{1704880800000L, 1704880900000L}
);
List<String[][]> measurementsList = ...;

session.insertRecords(deviceIds, timestamps, measurementsList, typesList, valuesList);

// 批量 Tablet 插入 (高效)
Tablet tablet = new Tablet(deviceId, measurements, types, values, timestamps, rowCount);
session.insertTablet(tablet);

session.close();
```

### 异步写入

```java
// 异步写入
session.insertRecordAsync(
    deviceId,
    timestamp,
    measurements,
    types,
    values,
    new Session.InsertCallback() {
        @Override
        public void onComplete(long success, Throwable exception) {
            if (exception != null) {
                System.err.println("Insert failed: " + exception.getMessage());
            } else {
                System.out.println("Insert successful");
            }
        }
    }
);
```

---

## 数据查询

### 时间序列查询

```sql
-- 查询单个测点
SELECT temperature FROM root.ln.wf01.wt01
WHERE time >= 1704880800000 AND time < 1704881000000;

-- 查询多个测点
SELECT temperature, humidity FROM root.ln.wf01.wt01;

-- 查询所有子设备
SELECT * FROM root.ln.wf01.*;

-- 按时间范围
SELECT * FROM root.ln.wf01.wt01
WHERE time BETWEEN 1704880800000 AND 1704881000000;

-- 最近 N 条
SELECT * FROM root.ln.wf01.wt01
ORDER BY time DESC
LIMIT 10;
```

### 聚合查询

```sql
-- 聚合函数
SELECT avg(temperature), max(temperature), min(temperature)
FROM root.ln.wf01.wt01
WHERE time >= now() - 1d;

-- 按时间分组
SELECT avg(temperature)
FROM root.ln.wf01.wt01
WHERE time >= now() - 1d
GROUP BY time(1h);  -- 每小时

-- 按层级分组
SELECT count(*)
FROM root.ln.**
GROUP BY level=2;  -- 按设备分组

-- 滑动窗口
SELECT avg(temperature)
FROM root.ln.wf01.wt01
GROUP BY time(1h), temperature
SLIDING(15m);  -- 15分钟滑动
```

### 表达式查询

```sql
-- 算术运算
SELECT temperature * 1.8 + 32 AS fahrenheit
FROM root.ln.wf01.wt01;

-- 函数
SELECT sin(temperature), cos(temperature)
FROM root.ln.wf01.wt01;

-- 条件表达式
SELECT case when temperature > 30 then 'hot' when temperature < 10 then 'cold' else 'normal' end
FROM root.ln.wf01.wt01;
```

---

## Schema 管理

### 创建 Schema

```sql
-- 创建时间序列
CREATE TIMESERIES root.ln.wf01.wt01.temperature
WITH DATATYPE=FLOAT, ENCODING=RLE;

-- 创建带属性的时间序列
CREATE TIMESERIES root.ln.wf01.wt01.humidity
WITH DATATYPE=FLOAT, ENCODING=RLE, COMPRESSOR=SNAPPY
TAGS(unit=percentage, description=环境湿度);

-- 创建对齐时间序列
CREATE ALIGNED TIMESERIES root.ln.wf01.wt02(
    temperature FLOAT ENCODING=RLE,
    humidity FLOAT ENCODING=RLE
);

-- 设置别名
CREATE TIMESERIES root.ln.wf01.wt01.temp
WITH DATATYPE=FLOAT
AS temperature;
```

### Schema 操作

```sql
-- 查看时间序列
SHOW TIMESERIES;

-- 查看特定路径
SHOW TIMESERIES root.ln.wf01.*;

-- 删除时间序列
DELETE TIMESERIES root.ln.wf01.wt01.temperature;

-- 修改编码
ALTER TIMESERIES root.ln.wf01.wt01.temperature
WITH ENCODING=GORILLA;
```

---

## TsFile 操作

### 写入 TsFile

```java
// 使用 TsFile Writer
File file = new File("data/device_001.tsfile");

try (TsFileWriter writer = new TsFileWriter(file)) {
    // 添加设备 Schema
    Map<String, TSDataType> dataTypes = new HashMap<>();
    dataTypes.put("temperature", TSDataType.FLOAT);
    dataTypes.put("humidity", TSDataType.FLOAT);
    writer.registerDevice("root.ln.wf01.wt01", dataTypes);

    // 写入数据
    for (long timestamp = 0; timestamp < 1000; timestamp++) {
        Map<String, Object> values = new HashMap<>();
        values.put("temperature", 20.0f + (float)(Math.random() * 10));
        values.put("humidity", 50.0f + (float)(Math.random() * 20));
        writer.write("root.ln.wf01.wt01", timestamp, values);
    }

    // 写入元数据
    writer.writeMetadata();
}
```

### 读取 TsFile

```java
// 使用 TsFile Reader
File file = new File("data/device_001.tsfile");

try (TsFileReader reader = new TsFileReader(file)) {
    // 查询数据
    QueryExpression query = QueryExpression.builder()
        .addSelectedPath("root.ln.wf01.wt01.temperature")
        .addSelectedPath("root.ln.wf01.wt01.humidity")
        .setFilterExpression(
            TimeFilter.gtEq(0L).and(TimeFilter.lt(1000L))
        )
        .build();

    QueryResult queryResult = reader.query(query);

    // 遍历结果
    for (Record record : queryResult.getRecords()) {
        long timestamp = record.getTime();
        Map<String, Field> values = record.getFields();
        float temp = values.get("temperature").getFloatValue();
        float hum = values.get("humidity").getFloatValue();
    }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-query-guide.md](03-query-guide.md) | 查询指南 |
| [04-java-api.md](04-java-api.md) | Java API |
| [README.md](README.md) | 索引文档 |
