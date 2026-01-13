package com.bigdata.example.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * HBase API Examples - 基础CRUD操作
 */
public class HBaseExample {

    private static final String ZOOKEEPER_QUORUM = "localhost";
    private static final String ZOOKEEPER_PORT = "2181";
    private static final String TABLE_NAME = "test_table";
    private static final String CF_DEFAULT = "cf";
    private static final String CF_INFO = "info";

    public static void main(String[] args) throws Exception {
        // 1. 创建HBase配置
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", ZOOKEEPER_QUORUM);
        config.set("hbase.zookeeper.property.clientPort", ZOOKEEPER_PORT);
        config.set("hbase.master", "localhost:16000");

        // 2. 创建连接
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            Admin admin = connection.getAdmin();

            System.out.println("Connected to HBase!");

            // 3. 创建表
            createTable(admin);

            // 4. CRUD操作
            performCRUD(connection);

            // 5. 批量操作
            batchOperations(connection);

            // 6. 过滤器查询
            filterQueries(connection);

            // 7. 扫描操作
            scanOperations(connection);

            // 8. 计数器操作
            counterOperations(connection);

            // 9. 过滤器示例
            advancedFilters(connection);

            // 10. 协处理器示例
            // coprocessorExample(admin, connection);

            admin.close();
            System.out.println("HBase Examples Completed!");
        }
    }

    /**
     * 创建HBase表
     */
    private static void createTable(Admin admin) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);

        // 检查表是否存在
        if (admin.tableExists(tableName)) {
            System.out.println("Table already exists: " + TABLE_NAME);
            return;
        }

        // 创建表描述符
        TableDescriptorBuilder tableDesc = TableDescriptorBuilder.newBuilder(tableName);

        // 添加列族
        ColumnFamilyDescriptor cfDefault = ColumnFamilyDescriptorBuilder
                .of(CF_DEFAULT)
                .setBlockCacheEnabled(true)
                .setBlocksize(65536)
                .setTimeToLive(86400) // 1天
                .build();

        ColumnFamilyDescriptor cfInfo = ColumnFamilyDescriptorBuilder
                .of(CF_INFO)
                .setBlockCacheEnabled(true)
                .build();

        tableDesc.setColumnFamily(cfDefault);
        tableDesc.setColumnFamily(cfInfo);

        // 设置表属性
        tableDesc.setValue(TableDescriptorBuilder.SPLIT_POLICY,
                "org.apache.hadoop.hbase.regionserver.IncreasingToUpperBoundRegionSplitPolicy");
        tableDesc.setValue("hbase.hstore.blockingStoreFiles", "10");

        admin.createTable(tableDesc.build());
        System.out.println("Created table: " + TABLE_NAME);
    }

    /**
     * CRUD操作
     */
    private static void performCRUD(Connection connection) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        try (Table table = connection.getTable(tableName)) {

            // === CREATE (Put) ===
            String rowKey = "row-001";
            Put put = new Put(Bytes.toBytes(rowKey));

            // 添加列数据
            put.addColumn(Bytes.toBytes(CF_DEFAULT),
                    Bytes.toBytes("name"), Bytes.toBytes("Alice"));
            put.addColumn(Bytes.toBytes(CF_DEFAULT),
                    Bytes.toBytes("age"), Bytes.toBytes("25"));
            put.addColumn(Bytes.toBytes(CF_INFO),
                    Bytes.toBytes("email"), Bytes.toBytes("alice@example.com"));

            table.put(put);
            System.out.println("Inserted row: " + rowKey);

            // === READ (Get) ===
            Get get = new Get(Bytes.toBytes(rowKey));
            Result result = table.get(get);

            if (result.isEmpty()) {
                System.out.println("Row not found: " + rowKey);
            } else {
                System.out.println("Retrieved row: " + rowKey);
                System.out.println("  name: " + Bytes.toString(result.getValue(
                        Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("name"))));
                System.out.println("  age: " + Bytes.toString(result.getValue(
                        Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("age"))));
                System.out.println("  email: " + Bytes.toString(result.getValue(
                        Bytes.toBytes(CF_INFO), Bytes.toBytes("email"))));
            }

            // === UPDATE ===
            // HBase的更新实际上是重新插入
            Put updatePut = new Put(Bytes.toBytes(rowKey));
            updatePut.addColumn(Bytes.toBytes(CF_DEFAULT),
                    Bytes.toBytes("age"), Bytes.toBytes("26"));
            table.put(updatePut);
            System.out.println("Updated row: " + rowKey);

            // === DELETE ===
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            // 删除特定列
            // delete.addColumn(Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("age"));
            // 删除整行
            table.delete(delete);
            System.out.println("Deleted row: " + rowKey);
        }
    }

    /**
     * 批量操作
     */
    private static void batchOperations(Connection connection) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        try (Table table = connection.getTable(tableName)) {

            List<Put> puts = new ArrayList<>();
            Random random = new Random();

            // 批量插入100行
            for (int i = 0; i < 100; i++) {
                String rowKey = String.format("user-%03d", i);
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(Bytes.toBytes(CF_DEFAULT),
                        Bytes.toBytes("name"), Bytes.toBytes("User" + i));
                put.addColumn(Bytes.toBytes(CF_DEFAULT),
                        Bytes.toBytes("age"), Bytes.toBytes(String.valueOf(20 + random.nextInt(50))));
                put.addColumn(Bytes.toBytes(CF_INFO),
                        Bytes.toBytes("score"), Bytes.toBytes(String.valueOf(random.nextDouble() * 100)));
                puts.add(put);
            }

            table.put(puts);
            System.out.println("Batch inserted 100 rows");

            // 批量读取
            List<Get> gets = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Get get = new Get(Bytes.toBytes(String.format("user-%03d", i)));
                gets.add(get);
            }

            Result[] results = table.get(gets);
            for (Result res : results) {
                if (!res.isEmpty()) {
                    String row = Bytes.toString(res.getRow());
                    String name = Bytes.toString(res.getValue(
                            Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("name")));
                    System.out.println("Row: " + row + ", Name: " + name);
                }
            }

            // 批量删除
            List<Delete> deletes = new ArrayList<>();
            for (int i = 0; i < 10; i++) {
                Delete del = new Delete(Bytes.toBytes(String.format("user-%03d", i)));
                deletes.add(del);
            }
            table.delete(deletes);
            System.out.println("Batch deleted 10 rows");
        }
    }

    /**
     * 过滤器查询
     */
    private static void filterQueries(Connection connection) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        try (Table table = connection.getTable(tableName)) {

            // 创建过滤器列表
            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

            // 行键前缀过滤器
            PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes("user-"));
            filterList.addFilter(prefixFilter);

            // 值过滤器
            SingleColumnValueFilter ageFilter = new SingleColumnValueFilter(
                    Bytes.toBytes(CF_DEFAULT),
                    Bytes.toBytes("age"),
                    CompareOperator.GREATER_OR_EQUAL,
                    Bytes.toBytes("30"));
            filterList.addFilter(ageFilter);

            Scan scan = new Scan();
            scan.setFilter(filterList);
            scan.setCaching(50);

            try (ResultScanner scanner = table.getScanner(scan)) {
                int count = 0;
                for (Result result : scanner) {
                    if (count < 5) {
                        String row = Bytes.toString(result.getRow());
                        String age = Bytes.toString(result.getValue(
                                Bytes.toBytes(CF_DEFAULT), Bytes.toBytes("age")));
                        System.out.println("Filtered row: " + row + ", age: " + age);
                    }
                    count++;
                }
                System.out.println("Total matched rows: " + count);
            }
        }
    }

    /**
     * 扫描操作
     */
    private static void scanOperations(Connection connection) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        try (Table table = connection.getTable(tableName)) {

            // 全表扫描
            System.out.println("Scanning all rows...");
            Scan scan = new Scan();
            scan.setCaching(100);

            try (ResultScanner scanner = table.getScanner(scan)) {
                int count = 0;
                for (Result result : scanner) {
                    count++;
                }
                System.out.println("Total rows in table: " + count);
            }

            // 范围扫描
            Scan rangeScan = new Scan();
            rangeScan.withStartRow(Bytes.toBytes("user-000"));
            rangeScan.withStopRow(Bytes.toBytes("user-050"));

            try (ResultScanner scanner = table.getScanner(rangeScan)) {
                int count = 0;
                for (Result result : scanner) {
                    count++;
                }
                System.out.println("Rows in range [user-000, user-050): " + count);
            }
        }
    }

    /**
     * 计数器操作
     */
    private static void counterOperations(Connection connection) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        try (Table table = connection.getTable(tableName)) {

            String counterRow = "counter-row";
            byte[] cf = Bytes.toBytes(CF_INFO);
            byte[] qualifier = Bytes.toBytes("visits");

            // 初始化计数器
            Increment increment = new Increment(Bytes.toBytes(counterRow));
            increment.addColumn(cf, qualifier, 1);
            table.increment(increment);

            // 读取计数器
            Get get = new Get(Bytes.toBytes(counterRow));
            get.addColumn(cf, qualifier);
            Result result = table.get(get);
            long count = Bytes.toLong(result.getValue(cf, qualifier));
            System.out.println("Counter value: " + count);
        }
    }

    /**
     * 高级过滤器
     */
    private static void advancedFilters(Connection connection) throws IOException {
        TableName tableName = TableName.valueOf(TABLE_NAME);
        try (Table table = connection.getTable(tableName)) {

            // 1. 列前缀过滤器
            System.out.println("Column Prefix Filter:");
            Filter colPrefixFilter = new ColumnPrefixFilter(Bytes.toBytes("n"));
            Scan scan1 = new Scan();
            scan1.setFilter(colPrefixFilter);
            scan1.setCaching(20);

            try (ResultScanner scanner = table.getScanner(scan1)) {
                for (Result result : scanner) {
                    System.out.println("  Row: " + Bytes.toString(result.getRow()));
                }
            }

            // 2. 键值过滤器
            System.out.println("\nValue Filter (score > 50):");
            Filter valueFilter = new ValueFilter(
                    CompareOperator.GREATER_OR_EQUAL,
                    Bytes.toBytes("50"));
            Scan scan2 = new Scan();
            scan2.setFilter(valueFilter);
            scan2.setCaching(20);

            try (ResultScanner scanner = table.getScanner(scan2)) {
                for (Result result : scanner) {
                    String value = Bytes.toString(result.getValue(
                            Bytes.toBytes(CF_INFO), Bytes.toBytes("score")));
                    System.out.println("  Score: " + value);
                }
            }

            // 3. 页面过滤器
            System.out.println("\nPage Filter (5 rows):");
            Filter pageFilter = new PageFilter(5);
            Scan scan3 = new Scan();
            scan3.setFilter(pageFilter);
            scan3.setCaching(5);

            try (ResultScanner scanner = table.getScanner(scan3)) {
                for (Result result : scanner) {
                    System.out.println("  Row: " + Bytes.toString(result.getRow()));
                }
            }

            // 4. 多条件组合过滤器
            System.out.println("\nComposite Filter:");
            FilterList composite = new FilterList(
                    FilterList.Operator.MUST_PASS_ALL,
                    new RowFilter(CompareOperator.GREATER_OR_EQUAL,
                            Bytes.toBytes("user-010")),
                    new RowFilter(CompareOperator.LESS,
                            Bytes.toBytes("user-030")),
                    new QualifierFilter(CompareOperator.EQUAL,
                            Bytes.toBytes("name"))
            );
            Scan scan4 = new Scan();
            scan4.setFilter(composite);

            try (ResultScanner scanner = table.getScanner(scan4)) {
                for (Result result : scanner) {
                    System.out.println("  Row: " + Bytes.toString(result.getRow()));
                }
            }
        }
    }
}
