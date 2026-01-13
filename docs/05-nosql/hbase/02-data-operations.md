# HBase 数据操作

## 目录

- [表管理](#表管理)
- [数据读写](#数据读写)
- [批量操作](#批量操作)
- [过滤器](#过滤器)
- [协处理器](#协处理器)

---

## 表管理

### 创建表

```java
// 创建表
Connection connection = ConnectionFactory.createConnection(config);
Admin admin = connection.getAdmin();

TableName tableName = TableName.valueOf("myTable");

// 定义列族
List<ColumnFamilyDescriptor> columnFamilies = new ArrayList<>();
columnFamilies.add(
    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"))
        .setMaxVersions(3)
        .setTimeToLive(86400 * 7)  // 7天
        .setBlockCacheEnabled(true)
        .build()
);
columnFamilies.add(
    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("data"))
        .setMaxVersions(1)
        .setCompressionType(Compression.Algorithm.SNAPPY)
        .build()
);

TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(tableName)
    .setColumnFamilies(columnFamilies)
    .setValue("table.description", "用户信息表")
    .build();

admin.createTable(tableDescriptor);
```

### 修改表

```java
// 修改列族属性
admin.modifyColumnFamily(tableName,
    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"))
        .setMaxVersions(5)
        .build()
);

// 添加新列族
admin.addColumnFamily(tableName,
    ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("new_cf"))
        .build()
);

// 删除列族
admin.deleteColumnFamily(tableName, Bytes.toBytes("old_cf"));
```

### 删除表

```java
// 禁用表
admin.disableTable(tableName);

// 删除表
admin.deleteTable(tableName);

// 判断表是否存在
boolean exists = admin.tableExists(tableName);
```

---

## 数据读写

### 写入数据

```java
Table table = connection.getTable(TableName.valueOf("myTable"));

// 单条写入
Put put = new Put(Bytes.toBytes("row1"));
put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Alice"));
put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("25"));
put.addColumn(Bytes.toBytes("data"), Bytes.toBytes("score"), Bytes.toBytes("95.5"));
table.put(put);

// 带时间戳写入
Put put2 = new Put(Bytes.toBytes("row2"), 1704880800000L);
put2.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), 1704880800000L,
    Bytes.toBytes("Bob"));
table.put(put2);

// 条件写入 (Check And Put)
Put put3 = new Put(Bytes.toBytes("row3"));
put3.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("Charlie"));

boolean success = table.checkAndPut(
    Bytes.toBytes("row3"),
    Bytes.toBytes("info"),
    Bytes.toBytes("age"),
    null,  // 如果 age 列不存在
    put3,
    true
);
```

### 读取数据

```java
Table table = connection.getTable(TableName.valueOf("myTable"));

// 按 RowKey 读取
Get get = new Get(Bytes.toBytes("row1"));
Result result = table.get(get);

// 获取值
byte[] name = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("name"));
String nameStr = Bytes.toString(name);

// 获取所有列
NavigableMap<byte[], byte[]> infoMap =
    result.getFamilyMap(Bytes.toBytes("info"));

// 获取指定版本
Get getV = new Get(Bytes.toBytes("row1"));
getV.setTimeStamp(1704880800000L);
Result resultV = table.get(getV);

// 获取所有版本
Get getAll = new Get(Bytes.toBytes("row1"));
getAll.readAllVersions();
Result resultAll = table.get(getAll);
```

### 扫描数据

```java
Table table = connection.getTable(TableName.valueOf("myTable"));

// 全表扫描
Scan scan = new Scan();
ResultScanner scanner = table.getScanner(scan);
for (Result r : scanner) {
    System.out.println("RowKey: " + Bytes.toString(r.getRow()));
    // 处理数据
}
scanner.close();

// 范围扫描
Scan scanRange = new Scan();
scanRange.withStartRow(Bytes.toBytes("row1"));
scanRange.withStopRow(Bytes.toBytes("row100"));

// 带过滤的扫描
Scan scanFilter = new Scan();
scanFilter.setFilter(
    new RowFilter(CompareOperator.EQUAL,
        new SubstringComparator("row"))
);
```

---

## 批量操作

### 批量写入

```java
Table table = connection.getTable(TableName.valueOf("myTable"));

// 批量 Put
List<Put> puts = new ArrayList<>();
for (int i = 0; i < 10000; i++) {
    Put put = new Put(Bytes.toBytes("row" + i));
    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes("User" + i));
    puts.add(put);
}
table.put(puts);

// 异步批量写入
CompletableFuture<Void> future = table.putAsync(puts);
future.thenRun(() -> System.out.println("Batch put completed"));
```

### 批量读取

```java
// 批量 Get
List<Get> gets = new ArrayList<>();
for (int i = 0; i < 100; i++) {
    Get get = new Get(Bytes.toBytes("row" + i));
    gets.add(get);
}

Result[] results = table.get(gets);
for (Result r : results) {
    if (r != null && !r.isEmpty()) {
        // 处理结果
    }
}

// 异步批量读取
table.getAsync(gets).thenAccept(results -> {
    for (Result r : results) {
        // 处理结果
    }
});
```

### 批量删除

```java
// 批量 Delete
List<Delete> deletes = new ArrayList<>();
for (int i = 0; i < 100; i++) {
    Delete delete = new Delete(Bytes.toBytes("row" + i));
    deletes.add(delete);
}
table.delete(deletes);
```

---

## 过滤器

### 行键过滤器

```java
// 行键前缀过滤器
Filter prefixFilter = new PrefixFilter(Bytes.toBytes("2024-01"));

// 行键比较过滤器
Filter rowFilter = new RowFilter(
    CompareOperator.GREATER_OR_EQUAL,
    new BinaryComparator(Bytes.toBytes("row100"))
);

// 行键正则过滤器
Filter regexFilter = new RowFilter(
    CompareOperator.EQUAL,
    new RegexStringComparator("row\\d+")
);
```

### 列过滤器

```java
// 列名前缀过滤器
Filter columnPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("name"));

// 多列名过滤器
Filter columnListFilter = new FilterList(
    FilterList.Operator.MUST_PASS_ALL,
    new QualifierFilter(CompareOperator.EQUAL, new BytesComparator(Bytes.toBytes("name"))),
    new QualifierFilter(CompareOperator.EQUAL, new BytesComparator(Bytes.toBytes("age")))
);

// 限制返回列数
scan.setFilter(new ColumnCountGetFilter(10));
```

### 值过滤器

```java
// 值比较过滤器
Filter valueFilter = new ValueFilter(
    CompareOperator.NOT_EQUAL,
    new BinaryComparator(Bytes.toBytes(""))
);

// 值范围过滤器
Filter rangeFilter = new ValueFilter(
    CompareOperator.GREATER_OR_EQUAL,
    new LongComparator(18L)
);

// 单值去重
Filter singleColumnValueFilter = new SingleColumnValueFilter(
    Bytes.toBytes("info"),
    Bytes.toBytes("age"),
    CompareOperator.GREATER_OR_EQUAL,
    new LongComparator(18L)
);
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-java-api.md](03-java-api.md) | Java API |
| [04-optimization.md](04-optimization.md) | 性能优化 |
| [README.md](README.md) | 索引文档 |
