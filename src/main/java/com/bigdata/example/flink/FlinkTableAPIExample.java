package com.bigdata.example.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;

/**
 * Flink Table API Examples - 表处理示例
 */
public class FlinkTableAPIExample {

    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 创建表的多种方式

        // 从DataStream创建
        env.fromElements(1, 2, 3, 4, 5)
                .map(x -> new Order(x, "item" + x, x * 10.0))
                .returns(Order.class);

        // DDL创建表
        tableEnv.executeSql(
                "CREATE TABLE orders (" +
                "  order_id BIGINT," +
                "  item_name STRING," +
                "  amount DOUBLE," +
                "  order_time TIMESTAMP(3)," +
                "  WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'orders'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'format' = 'json'" +
                ")"
        );

        // 3. Table API操作

        Table orders = tableEnv.from("orders");

        // 选择列
        Table selected = orders.select($("order_id"), $("item_name"), $("amount"));

        // 过滤
        Table filtered = orders.filter($("amount").isGreater(100.0));

        // 分组聚合
        Table aggregated = orders
                .groupBy($("item_name"))
                .select(
                        $("item_name"),
                        $("amount").sum().as("total_amount"),
                        $("order_id").count().as("order_count")
                );

        // 窗口聚合 (滚动窗口)
        Table tumblingWindow = orders
                .window(org.apache.flink.table.api.Expressions
                        .tumble($("order_time"), org.apache.flink.table.api.Expressions.lit(10).minutes()))
                .groupBy($("item_name"), $("w"))
                .select(
                        $("item_name"),
                        $("w").start().as("window_start"),
                        $("w").end().as("window_end"),
                        $("amount").sum().as("total_amount")
                );

        // 窗口聚合 (滑动窗口)
        Table slidingWindow = orders
                .window(org.apache.flink.table.api.Expressions
                        .slide($("order_time"), org.apache.flink.table.api.Expressions.lit(10).minutes(),
                               org.apache.flink.table.api.Expressions.lit(5).minutes()))
                .groupBy($("item_name"), $("w"))
                .select($("item_name"), $("w").start(), $("amount").sum());

        // 4. SQL操作

        // 注册临时视图
        tableEnv.createTemporaryView("orders_view", orders);

        // 执行SQL查询
        Table sqlResult = tableEnv.sqlQuery(
                "SELECT item_name, SUM(amount) as total_amount, COUNT(*) as order_count " +
                "FROM orders " +
                "WHERE amount > 100 " +
                "GROUP BY item_name " +
                "ORDER BY total_amount DESC " +
                "LIMIT 10"
        );

        // 5. Join操作

        // 内连接
        Table ordersTable = tableEnv.from("orders");
        Table usersTable = tableEnv.sqlQuery(
                "SELECT * FROM (" +
                "VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')) AS t(user_id, name)"
        );

        Table joined = ordersTable.join(usersTable)
                .where($("user_id").isEqual($("user_id")));

        // 时间窗口连接
        Table shipments = tableEnv.sqlQuery(
                "SELECT * FROM (" +
                "VALUES (1, 'item1', TIMESTAMP '2024-01-01 10:00:00'), " +
                "       (2, 'item2', TIMESTAMP '2024-01-01 10:05:00')) AS t(order_id, item, ship_time)"
        );

        Table windowJoined = ordersTable.join(shipments)
                .where(
                        $("item_name").isEqual($("item"))
                                .and($("order_time").isGreaterOrEqual($("ship_time").minus(lit(4).hours())))
                                .and($("order_time").isLess($("ship_time")))
                );

        // 6. 排序和限制

        Table sorted = orders.orderBy($("amount").desc()).fetch(10);
        Table offset = orders.orderBy($("order_time").asc()).offset(5).fetch(10);

        // 7. DISTINCT

        Table distinct = orders.select($("item_name")).distinct();

        // 8. 集合操作

        Table union = orders.unionAll(filtered);
        Table unionDistinct = orders.union(filtered);

        // 9. 写出到外部系统

        // 写出到Kafka
        tableEnv.executeSql(
                "CREATE TABLE output_orders (" +
                "  item_name STRING," +
                "  total_amount DOUBLE" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'output-orders'," +
                "  'properties.bootstrap.servers' = 'localhost:9092'," +
                "  'format' = 'json'" +
                ")"
        );

        aggregated.insertInto("output_orders");

        // 10. 转换为DataStream

        // 追加模式 (Append Mode)
        DataStream<Row> appendStream = tableEnv.toAppendStream(orders, Row.class);

        // 撤回模式 (Retract Mode)
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnv.toRetractStream(orders, Row.class);

        // 11. 维表查询 (Lookup Join)

        // 假设有一个MySQL维度表
        tableEnv.executeSql(
                "CREATE TABLE dim_user (" +
                "  user_id BIGINT," +
                "  user_name STRING," +
                "  PRIMARY KEY (user_id) NOT ENFORCED" +
                ") WITH (" +
                "  'connector' = 'jdbc'," +
                "  'url' = 'jdbc:mysql://localhost:3306/db'," +
                "  'table-name' = 'users'," +
                "  'username' = 'root'," +
                "  'password' = 'password'" +
                ")"
        );

        Table ordersWithUser = orders.join(dimUser)
                .where($("user_id").isEqual($("id")));

        System.out.println("Flink Table API Examples Completed!");
    }

    // 内部类
    public static class Order {
        public long orderId;
        public String itemName;
        public double amount;
        public LocalDateTime orderTime;

        public Order() {}

        public Order(long orderId, String itemName, double amount) {
            this.orderId = orderId;
            this.itemName = itemName;
            this.amount = amount;
            this.orderTime = LocalDateTime.now();
        }
    }

    public static class User {
        public long userId;
        public String userName;

        public User() {}

        public User(long userId, String userName) {
            this.userId = userId;
            this.userName = userName;
        }
    }
}
