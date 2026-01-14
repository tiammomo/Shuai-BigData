# Flink DataStream API 详解

## 目录

- [环境配置](#环境配置)
- [数据源](#数据源)
- [转换算子](#转换算子)
- [多流操作](#多流操作)
- [窗口计算](#窗口计算)
- [数据接收器](#数据接收器)
- [异步 IO](#异步-io)

---

## 环境配置

### Maven 依赖

```xml
<!-- Flink 核心依赖 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-java</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- DataStream API -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- Flink 运行时 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-runtime</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- Kafka 连接器 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-connector-kafka</artifactId>
    <version>${flink.version}</version>
</dependency>

<!-- 测试依赖 -->
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-test-utils</artifactId>
    <version>${flink.version}</version>
    <scope>test</scope>
</dependency>
```

### 执行环境配置

```java
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.api.common.time.Time;

/**
 * Flink 执行环境配置
 */
public class EnvironmentConfig {

    /**
     * 1. 本地开发环境 (带 Web UI)
     *
     * 适用场景: 开发、调试
     */
    public static StreamExecutionEnvironment localEnvironmentWithWebUI() {
        Configuration config = new Configuration();
        config.setInteger("rest.port", 8081);  // Web UI 端口

        return StreamExecutionEnvironment
            .createLocalEnvironmentWithWebUI(config);
    }

    /**
     * 2. 本地环境 (无 Web UI)
     *
     * 适用场景: 单元测试
     */
    public static StreamExecutionEnvironment localEnvironment() {
        return StreamExecutionEnvironment
            .createLocalEnvironment();
    }

    /**
     * 3. 远程集群环境
     *
     * 适用场景: 连接独立集群
     */
    public static StreamExecutionEnvironment remoteEnvironment() {
        return StreamExecutionEnvironment
            .createRemoteEnvironment(
                "jobmanager-host",  // JobManager 主机
                6123,               // RPC 端口
                2,                  // 并行度
                "path/to/jar"       // JAR 包路径
            );
    }

    /**
     * 4. 生产环境配置
     */
    public static StreamExecutionEnvironment productionEnvironment() {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度
        env.setParallelism(4);

        // 启用 Checkpoint
        env.enableCheckpointing(60000);  // 60秒

        // Checkpoint 模式
        env.getCheckpointConfig()
            .setCheckpointingMode(
                org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);

        // 超时时间
        env.getCheckpointConfig()
            .setCheckpointTimeout(600000);  // 10分钟

        // 最小间隔
        env.getCheckpointConfig()
            .setMinPauseBetweenCheckpoints(5000);

        // 设置状态后端
        env.setStateBackend(
            new org.apache.flink.runtime.state.filesystem.FsStateBackend(
                "hdfs://namenode:8020/flink/checkpoints"
            )
        );

        // 重启策略
        env.setRestartStrategy(
            org.apache.flink.api.common.restartstrategy.RestartStrategies
                .fixedDelayRestart(3, Time.seconds(10))
        );

        // 设置时间语义
        env.setStreamTimeCharacteristic(
            org.apache.flink.streaming.api.TimeCharacteristic.EventTime
        );

        // 水印间隔
        env.getConfig().setAutoWatermarkInterval(5000);

        return env;
    }

    /**
     * 5. YARN 环境
     *
     * 适用场景: YARN 集群
     */
    public static void yarnEnvironment() {
        // 通过 YARN 客户端提交时自动创建
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // YARN 配置
        // - yarn.application.name
        // - yarn.application.container.vcores
        // - yarn.application.memory
    }
}
```

---

## 数据源

### 内置数据源

```java
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.functions.MapFunction;

/**
 * Flink 内置数据源
 */
public class BuiltInSources {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // ============ 1. Collection Source ============
        // 从 Java 集合创建
        DataStreamSource<Integer> collectionSource = env
            .fromCollection(
                () -> java.util.Arrays.asList(1, 2, 3, 4, 5)
            )
            .name("CollectionSource");

        // 从 Iterable 创建
        DataStreamSource<Long> iterableSource = env
            .fromCollection(
                new java.util.Iterator<Long>() {
                    private long current = 0;
                    @Override
                    public boolean hasNext() {
                        return current < 100;
                    }
                    @Override
                    public Long next() {
                        return current++;
                    }
                }
            );

        // ============ 2. Single Element Source ============
        // 从单个元素创建
        DataStreamSource<String> singleSource = env
            .fromElements("a", "b", "c", "d");

        // ============ 3. Sequence Source ============
        // 序列数据
        DataStreamSource<Long> sequenceSource = env
            .generateSequence(1, 1000);

        // ============ 4. Socket Source ============
        // 从网络套接字读取
        DataStreamSource<String> socketSource = env
            .socketTextStream("localhost", 9999, "\n")
            .name("SocketSource");

        // ============ 5. File Source (Flink 1.19+ 推荐) ============
        // 从文件读取
        DataStreamSource<String> fileSource = env
            .readTextFile("hdfs://namenode:8020/data/input.txt")
            .name("FileSource");

        // 监控目录变化
        // env.readFile(
        //     new TextFormat(new Path("/data")),
        //     "/data",
        //     FileProcessingMode.WATCH_CHANGES,
        //     1000
        // );

        // ============ 6. List Source ============
        DataStreamSource<String> listSource = env
            .fromCollection(
                java.util.Arrays.asList("hello", "world", "flink")
            );
    }
}
```

### Kafka 数据源

```java
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

/**
 * Kafka 数据源配置
 */
public class KafkaSourceExample {

    /**
     * 1. 旧版 Kafka Consumer (Flink 1.x)
     */
    public static FlinkKafkaConsumer<String> oldKafkaConsumer() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        props.setProperty("group.id", "flink-consumer-group");

        return new FlinkKafkaConsumer<>(
            "orders",                    // Topic
            new SimpleStringSchema(),    // 反序列化
            props
        );
    }

    /**
     * 2. 新版 Kafka Source (Flink 1.17+ 推荐)
     */
    public static KafkaSource<String> newKafkaSource() {
        return KafkaSource.<String>builder()
            .setBootstrapServers("kafka1:9092,kafka2:9092,kafka3:9092")
            .setTopics("orders")
            .setGroupId("flink-consumer-group")
            .setStartingOffsets(OffsetsInitializer.latest())
            .setValueOnlyDeserializer(new SimpleStringSchema())
            .build();
    }

    /**
     * 3. 完整配置示例
     */
    public static DataStreamSource<String> completeConfig() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
        props.setProperty("group.id", "order-service");
        props.setProperty("auto.offset.reset", "earliest");
        props.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            "orders",
            new SimpleStringSchema(),
            props
        );

        // 手动设置偏移量
        consumer.setStartFromTimestamp(System.currentTimeMillis());

        // 启用自定义水印
        return env
            .addSource(consumer)
            .name("KafkaSource")
            .setParallelism(3);
    }

    /**
     * 4. 事件时间配置
     */
    public static DataStreamSource<String> eventTimeSource() {
        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
            "orders",
            new SimpleStringSchema(),
            getKafkaProps()
        );

        // 设置水印策略
        return env
            .addSource(consumer)
            .assignTimestampsAndWatermarks(
                WatermarkStrategy.<String>forBoundedOutOfOrderness(
                    java.time.Duration.ofSeconds(5)
                )
                .withTimestampAssigner(
                    (event, timestamp) -> {
                        // 从事件中提取时间戳
                        return extractTimestamp(event);
                    }
                )
            );
    }

    private static Properties getKafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "test-group");
        return props;
    }

    private static long extractTimestamp(String event) {
        // 解析 JSON 提取 timestamp
        return System.currentTimeMillis();
    }
}
```

### 自定义数据源

```java
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.configuration.Configuration;

/**
 * 自定义数据源实现
 */
public class CustomSourceExample {

    /**
     * 1. 简单 SourceFunction
     *
     * 适用场景: 单并行度数据源
     */
    public static class NumberSource
            implements SourceFunction<Long> {

        private volatile boolean running = true;
        private long current = 0;

        @Override
        public void run(SourceContext<Long> ctx) {
            while (running && current < 10000) {
                // 发送数据
                ctx.collect(current++);

                // 控制发送速率
                try {
                    Thread.sleep(100);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    /**
     * 2. RichParallelSourceFunction
     *
     * 适用场景: 多并行度数据源
     * 支持 open/close 方法
     */
    public static class JDBCSource
            extends RichParallelSourceFunction<Order> {

        private volatile boolean running = true;
        private Connection connection;
        private PreparedStatement statement;

        @Override
        public void open(Configuration parameters) {
            // 初始化数据库连接
            // 每个并行实例调用一次
            try {
                connection = DriverManager.getConnection(
                    "jdbc:mysql://localhost:3306/orders",
                    "user", "password"
                );
                statement = connection.prepareStatement(
                    "SELECT * FROM orders WHERE id > ?"
                );
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void run(SourceContext<Order> ctx) {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            statement.setLong(1, subtaskIndex * 10000);

            ResultSet rs = statement.executeQuery();
            while (running && rs.next()) {
                Order order = extractOrder(rs);
                ctx.collect(order);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }

        @Override
        public void close() {
            // 清理资源
            // 每个并行实例调用一次
            try {
                if (statement != null) statement.close();
                if (connection != null) connection.close();
            } catch (SQLException e) {
                // log error
            }
        }

        private Order extractOrder(ResultSet rs) throws SQLException {
            return new Order(
                rs.getLong("id"),
                rs.getString("customer_id"),
                rs.getBigDecimal("amount")
            );
        }
    }

    /**
     * 3. 批量读取 Source
     *
     * 适用场景: 大批量数据导入
     */
    public static class BatchSource
            extends RichParallelSourceFunction<String> {

        @Override
        public void run(SourceContext<String> ctx) {
            int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            int parallelism = getRuntimeContext().getNumberOfParallelSubtasks();

            // 分区读取
            for (long i = subtaskIndex; i < 1000000; i += parallelism) {
                ctx.collect("record-" + i);
            }
        }

        @Override
        public void cancel() {}
    }
}
```

---

## 转换算子

### 基础转换

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.common.functions.*;

/**
 * 基础转换算子
 */
public class BasicTransformations {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<String> source = env
            .fromElements("hello world", "flink streaming", "big data");

        // ============ 1. Map ============
        // 一对一转换
        DataStream<Integer> mapped = source
            .map(new MapFunction<String, Integer>() {
                @Override
                public Integer map(String value) throws Exception {
                    return value.length();
                }
            })
            .map(String::length);  // Lambda 简化

        // ============ 2. FlatMap ============
        // 一对多转换
        DataStream<String> flatMapped = source
            .flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out)
                        throws Exception {
                    for (String word : value.split(" ")) {
                        out.collect(word.toLowerCase());
                    }
                }
            })
            .flatMap((value, out) -> {
                for (String word : value.split(" ")) {
                    out.collect(word);
                }
            });

        // ============ 3. Filter ============
        // 过滤
        DataStream<String> filtered = source
            .filter(new FilterFunction<String>() {
                @Override
                public boolean filter(String value) throws Exception {
                    return value.length() > 5;
                }
            })
            .filter(s -> s.length() > 5);

        // ============ 4. KeyBy ============
        // 分组 (返回 KeyedStream)
        DataStream<Integer> keyed = mapped
            .keyBy(value -> value % 2)  // 按奇偶分组
            .sum(0);  // 聚合

        // ============ 5. Reduce ============
        // 归约
        DataStream<Integer> reduced = mapped
            .keyBy(value -> value % 2)
            .reduce(new ReduceFunction<Integer>() {
                @Override
                public Integer reduce(Integer v1, Integer v2)
                        throws Exception {
                    return v1 + v2;
                }
            });

        // ============ 6. Aggregate ============
        // 聚合 (更通用的聚合)
        DataStream<Long> aggregated = source
            .flatMap((value, out) -> out.collect((long) value.length()))
            .keyBy(value -> value % 2)
            .aggregate(new AggregateFunction<Long, Long, Long>() {
                @Override
                public Long createAccumulator() {
                    return 0L;
                }

                @Override
                public Long add(Long value, Long accumulator) {
                    return accumulator + value;
                }

                @Override
                public Long getResult(Long accumulator) {
                    return accumulator;
                }

                @Override
                public Long merge(Long a, Long b) {
                    return a + b;
                }
            });
    }
}
```

### 聚合转换

```java
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.functions.KeySelector;

/**
 * 聚合转换详解
 */
public class AggregationTransformations {

    /**
     * Sum / Min / Max / MinBy / MaxBy
     */
    public static void simpleAggregations() {
        DataStream<Order> orders = env.fromCollection(getOrders());

        // Sum - 求和
        // 按 customerId 分组，求 amount 总和
        orders
            .keyBy(order -> order.getCustomerId())
            .sum("amount")
            .print("sum");

        // Min - 最小值
        orders
            .keyBy(order -> order.getCustomerId())
            .min("amount");

        // Max - 最大值
        orders
            .keyBy(order -> order.getCustomerId())
            .max("amount");

        // MinBy - 最小值对应的完整记录
        orders
            .keyBy(order -> order.getCustomerId())
            .minBy("amount");

        // MaxBy - 最大值对应的完整记录
        orders
            .keyBy(order -> order.getCustomerId())
            .maxBy("amount");
    }

    /**
     * 自定义 Aggregate
     */
    public static class OrderStatsAggregate
            implements AggregateFunction<Order, OrderStats, OrderStats> {

        @Override
        public OrderStats createAccumulator() {
            return new OrderStats();
        }

        @Override
        public OrderStats add(Order order, OrderStats stats) {
            stats.count++;
            stats.totalAmount = stats.totalAmount.add(order.getAmount());
            stats.maxAmount = stats.maxAmount.max(order.getAmount());
            stats.minAmount = stats.minAmount.min(order.getAmount());
            return stats;
        }

        @Override
        public OrderStats getResult(OrderStats stats) {
            stats.avgAmount = stats.totalAmount.divide(
                BigDecimal.valueOf(stats.count), 2, RoundingMode.HALF_UP);
            return stats;
        }

        @Override
        public OrderStats merge(OrderStats a, OrderStats b) {
            a.count += b.count;
            a.totalAmount = a.totalAmount.add(b.totalAmount);
            a.maxAmount = a.maxAmount.max(b.maxAmount);
            a.minAmount = a.minAmount.min(b.minAmount);
            return a;
        }
    }

    public static class OrderStats {
        public int count = 0;
        public BigDecimal totalAmount = BigDecimal.ZERO;
        public BigDecimal maxAmount = BigDecimal.ZERO;
        public BigDecimal minAmount = BigDecimal.ZERO;
        public BigDecimal avgAmount = BigDecimal.ZERO;
    }
}
```

---

## 多流操作

### Union / Connect

```java
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.api.common.functions.CoMapFunction;

/**
 * 多流操作
 */
public class MultiStreamOperations {

    /**
     * 1. Union - 合并多个同类型流
     *
     * 特点:
     * - 合并多个 DataStream
     * - 元素类型必须相同
     * - 简单合并
     */
    public static void unionExample() {
        DataStream<String> stream1 = env.fromElements("a", "b");
        DataStream<String> stream2 = env.fromElements("c", "d");
        DataStream<String> stream3 = env.fromElements("e", "f");

        // 合并多个流
        DataStream<String> union = stream1
            .union(stream2)
            .union(stream3);

        // 输出: a, b, c, d, e, f
    }

    /**
     * 2. Connect - 连接两个不同类型流
     *
     * 特点:
     * - 保留各自的类型
     * - 可以调用 coMap/coFlatMap 统一处理
     * - 可以调用 process 进行更复杂的处理
     */
    public static void connectExample() {
        DataStream<String> stringStream = env.fromElements("1", "2", "3");
        DataStream<Integer> intStream = env.fromElements(10, 20, 30);

        // 连接两个流
        ConnectedStreams<String, Integer> connected =
            stringStream.connect(intStream);

        // 方式1: CoMap - 统一处理两种类型
        DataStream<String> mapped = connected
            .map(new CoMapFunction<String, Integer, String>() {
                @Override
                public String map1(String value) {
                    return "String: " + value;
                }

                @Override
                public String map2(Integer value) {
                    return "Integer: " + value;
                }
            });

        // 方式2: CoFlatMap - 更灵活的处理
        DataStream<String> flatMapped = connected
            .flatMap(new CoFlatMapFunction<String, Integer, String>() {
                @Override
                public void flatMap1(String value, Collector<String> out) {
                    out.collect("String: " + value);
                }

                @Override
                public void flatMap2(Integer value, Collector<String> out) {
                    out.collect("Integer: " + value);
                }
            });
    }

    /**
     * 3. Cross - 笛卡尔积
     *
     * 注意: 大数据量慎用
     */
    public static void crossExample() {
        DataStream<Integer> stream1 = env.fromElements(1, 2);
        DataStream<String> stream2 = env.fromElements("a", "b");

        // 笛卡尔积
        DataStream<String> crossed = stream1
            .cross(stream2)
            .map(tuple -> tuple.f0 + "-" + tuple.f1);

        // 输出: 1-a, 1-b, 2-a, 2-b
    }
}
```

### Join 操作

```java
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 流 Join 操作
 */
public class JoinOperations {

    /**
     * 1. Window Join
     *
     * 在窗口内 Join
     */
    public static void windowJoin() {
        DataStream<Order> orders = env.fromElements(
            new Order(1, "product-1", 100.0),
            new Order(2, "product-2", 200.0)
        );

        DataStream<Shipment> shipments = env.fromElements(
            new Shipment(1, "2024-01-01"),
            new Shipment(2, "2024-01-02")
        );

        // 设置事件时间
        orders = orders.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(
                java.time.Duration.ofSeconds(5)
            ).withTimestampAssigner(
                (order, ts) -> order.getTimestamp()
            )
        );

        shipments = shipments.assignTimestampsAndWatermarks(
            WatermarkStrategy.<Shipment>forBoundedOutOfOrderness(
                java.time.Duration.ofSeconds(5)
            ).withTimestampAssigner(
                (shipment, ts) -> shipment.getTimestamp()
            )
        );

        // Window Join (5秒滚动窗口)
        DataStream<String> joined = orders
            .join(shipments)
            .where(Order::getOrderId)
            .equalTo(Shipment::getOrderId)
            .window(TumblingEventTimeWindows.of(Time.seconds(5)))
            .apply(
                (order, shipment, out) -> {
                    out.collect("Order " + order.getOrderId() +
                        " shipped on " + shipment.getDate());
                }
            );
    }

    /**
     * 2. Interval Join
     *
     * 基于时间间隔 Join
     */
    public static void intervalJoin() {
        DataStream<Order> orders = env.fromElements(
            new Order(1, "product-1", 100.0)
        ).assignTimestampsAndWatermarks(
            WatermarkStrategy.<Order>forBoundedOutOfOrderness(
                java.time.Duration.ofSeconds(5)
            ).withTimestampAssigner(
                (order, ts) -> order.getTimestamp()
            )
        );

        DataStream<Payment> payments = env.fromElements(
            new Payment(1, 100.0)
        ).assignTimestampsAndWatermarks(
            WatermarkStrategy.<Payment>forBoundedOutOfOrderness(
                java.time.Duration.ofSeconds(5)
            ).withTimestampAssigner(
                (payment, ts) -> payment.getTimestamp()
            )
        );

        // Interval Join (订单后 1小时内付款)
        DataStream<String> joined = orders
            .keyBy(Order::getOrderId)
            .intervalJoin(payments.keyBy(Payment::getOrderId))
            .between(
                java.time.Duration.ofSeconds(-3600),  // 下界
                java.time.Duration.ofSeconds(0)       // 上界
            )
            .process(
                new ProcessJoinFunction<Order, Payment, String>() {
                    @Override
                    public void processElement(Order order, Payment payment,
                                               Context ctx, Collector<String> out) {
                        out.collect("Order " + order.getOrderId() +
                            " paid " + payment.getAmount());
                    }
                }
            );
    }
}
```

---

## 窗口计算

### 窗口类型

```java
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.windows.*;

/**
 * Flink 窗口类型详解
 *
 * 窗口分类:
 * 1. Keyed Window - 按 Key 分组后应用窗口
 * 2. Non-Keyed Window - 全局窗口 (并行度为 1)
 *
 * 窗口分配器:
 * 1. Tumbling Window - 滚动窗口 (不重叠)
 * 2. Sliding Window - 滑动窗口 (可重叠)
 * 3. Session Window - 会话窗口 (动态大小)
 * 4. Global Window - 全局窗口 (所有数据)
 */
public class WindowTypes {

    /**
     * 1. Tumbling Window - 滚动窗口
     *
     * 特点: 不重叠，等大小
     * 场景: 定时统计
     */
    public static void tumblingWindow() {
        // 每小时统计
        orders
            .keyBy(order -> order.getCustomerId())
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .sum("amount");

        // 每天统计 (Event Time)
        orders
            .keyBy(order -> order.getCustomerId())
            .window(TumblingEventTimeWindows.of(
                Time.days(1),
                Time.hours(8)  // 偏移量 (解决时区问题)
            ))
            .sum("amount");

        // 每分钟统计 (Processing Time)
        orders
            .keyBy(order -> order.getCustomerId())
            .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
            .sum("amount");
    }

    /**
     * 2. Sliding Window - 滑动窗口
     *
     * 特点: 可重叠，有大小和步长
     * 场景: 移动平均
     */
    public static void slidingWindow() {
        // 窗口大小 1小时，步长 10分钟
        orders
            .keyBy(order -> order.getCustomerId())
            .window(SlidingEventTimeWindows.of(
                Time.hours(1),   // 窗口大小
                Time.minutes(10) // 步长
            ))
            .sum("amount");

        // Count Window (基于数量)
        orders
            .keyBy(order -> order.getCustomerId())
            .countWindow(100, 20);  // 每100个元素窗口，滑动20个
    }

    /**
     * 3. Session Window - 会话窗口
     *
     * 特点: 动态大小，间隔确定
     * 场景: 用户会话分析
     */
    public static void sessionWindow() {
        // 间隙 30分钟的会话
        orders
            .keyBy(order -> order.getCustomerId())
            .window(EventTimeSessionWindows.withGap(Time.minutes(30)))
            .sum("amount");

        // 动态间隙
        orders
            .keyBy(order -> order.getCustomerId())
            .window(EventTimeSessionWindows.withDynamicGap(
                session -> {
                    // 根据用户类型确定间隙
                    return session.getCustomerType().equals("VIP")
                        ? Time.minutes(10)
                        : Time.minutes(30);
                }
            ));
    }

    /**
     * 4. Global Window - 全局窗口
     *
     * 特点: 所有数据一个窗口
     * 场景: 自定义触发
     */
    public static void globalWindow() {
        orders
            .keyBy(order -> order.getCustomerId())
            .window(GlobalWindows.create())
            .trigger(new CustomTrigger())
            .sum("amount");
    }
}
```

### 窗口函数

```java
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.util.Collector;

/**
 * 窗口函数详解
 *
 * 类型:
 * 1. ReduceFunction - 增量聚合
 * 2. AggregateFunction - 增量聚合 (更通用)
 * 3. ProcessWindowFunction - 全窗口处理
 * 4. WindowFunction - 旧版全窗口处理
 */
public class WindowFunctions {

    /**
     * 1. ReduceFunction - 增量聚合
     *
     * 优点: 内存效率高
     * 缺点: 只能返回同类型
     */
    public static void reduceFunction() {
        orders
            .keyBy(order -> order.getCustomerId())
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .reduce(
                // ReduceFunction
                (o1, o2) -> new Order(
                    o1.getOrderId(),
                    o1.getCustomerId(),
                    o1.getAmount().add(o2.getAmount())
                ),
                // WindowFunction (可选，输出结果)
                new OrderWindowFunction()
            );
    }

    /**
     * 2. AggregateFunction - 更通用的聚合
     *
     * 支持不同输入、累加器、输出类型
     */
    public static void aggregateFunction() {
        orders
            .keyBy(order -> order.getCustomerId())
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .aggregate(
                new AggregateFunction<Order, OrderAccumulator, OrderStats>() {
                    @Override
                    public OrderAccumulator createAccumulator() {
                        return new OrderAccumulator();
                    }

                    @Override
                    public OrderAccumulator add(Order order,
                                                 OrderAccumulator acc) {
                        acc.count++;
                        acc.totalAmount = acc.totalAmount.add(order.getAmount());
                        return acc;
                    }

                    @Override
                    public OrderStats getResult(OrderAccumulator acc) {
                        return new OrderStats(
                            acc.count,
                            acc.totalAmount
                        );
                    }

                    @Override
                    public OrderAccumulator merge(OrderAccumulator a,
                                                   OrderAccumulator b) {
                        a.count += b.count;
                        a.totalAmount = a.totalAmount.add(b.totalAmount);
                        return a;
                    }
                }
            );
    }

    /**
     * 3. ProcessWindowFunction - 全窗口处理
     *
     * 优点: 可以访问窗口元信息
     * 缺点: 内存开销大
     */
    public static void processWindowFunction() {
        orders
            .keyBy(order -> order.getCustomerId())
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .process(
                new ProcessWindowFunction<Order, String, String, TimeWindow>() {
                    @Override
                    public void process(String customerId,
                                        Context context,
                                        Iterable<Order> orders,
                                        Collector<String> out) {
                        // 窗口元信息
                        TimeWindow window = context.window();
                        long start = window.getStartTime();
                        long end = window.getEndTime();

                        // 聚合
                        int count = 0;
                        BigDecimal total = BigDecimal.ZERO;
                        for (Order order : orders) {
                            count++;
                            total = total.add(order.getAmount());
                        }

                        // 输出
                        out.collect(String.format(
                            "Customer %s: %d orders, %.2f total (%d - %d)",
                            customerId, count, total, start, end
                        ));
                    }
                }
            );
    }

    // ============ 辅助类 ============

    public static class OrderWindowFunction
            implements WindowFunction<Order, Order, String, TimeWindow> {

        @Override
        public void apply(String customerId, TimeWindow window,
                          Iterable<Order> orders, Collector<Order> out) {
            // 处理逻辑
        }
    }

    public static class OrderAccumulator {
        public int count = 0;
        public BigDecimal totalAmount = BigDecimal.ZERO;
    }

    public static class OrderStats {
        public OrderStats(int count, BigDecimal total) {
            this.count = count;
            this.totalAmount = total;
        }
        public int count;
        public BigDecimal totalAmount;
    }
}
```

---

## 数据接收器

### 内置 Sink

```java
/**
 * Flink 数据接收器
 */
public class SinkExamples {

    /**
     * 1. Print Sink
     *
     * 调试用，生产环境慎用
     */
    public static void printSink() {
        orders
            .map(order -> order.getAmount())
            .print();  // 打印到 TaskManager 日志

        orders
            .map(order -> order.getAmount())
            .printToErr();  // 打印到 stderr
    }

    /**
     * 2. File Sink (Flink 1.19+ 推荐)
     */
    public static void fileSink() {
        // 批处理模式写入文件
        orders
            .map(order -> order.toJson())
            .sinkTo(
                FileSink.forRowFormat(
                    new Path("hdfs://namenode:8020/output"),
                    new SimpleStringEncoder<String>()
                ).build()
            );

        // 分桶策略
        .sinkTo(
            FileSink.forBulkFormat(
                new Path("hdfs://namenode:8020/output"),
                AvroParquetWriter.forGenericRecord(schema)
            ).withBucketAssigner(
                new DateTimeBucketAssigner<>("yyyy/MM/dd")
            ).build()
        );
    }

    /**
     * 3. Kafka Sink
     */
    public static void kafkaSink() {
        orders
            .map(order -> order.toJson())
            .addSink(
                new FlinkKafkaProducer<>(
                    "orders-output",
                    new SimpleStringSchema(),
                    getKafkaProps()
                )
            );

        // 精确一次写入
        orders
            .map(order -> order.toJson())
            .addSink(
                new FlinkKafkaProducer<>(
                    "orders-output",
                    new SimpleStringSchema(),
                    getKafkaProps(),
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE
                )
            );
    }

    /**
     * 4. JDBC Sink
     */
    public static void jdbcSink() {
        orders
            .addSink(
                new JDBCSinkFunction(
                    "INSERT INTO orders (id, customer_id, amount) VALUES (?, ?, ?)",
                    (ps, order) -> {
                        ps.setLong(1, order.getOrderId());
                        ps.setString(2, order.getCustomerId());
                        ps.setBigDecimal(3, order.getAmount());
                    }
                )
            );
    }

    private static Properties getKafkaProps() {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("transaction.timeout.ms", "900000");
        return props;
    }
}
```

---

## 异步 IO

```java
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.configuration.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * 异步 IO 操作
 *
 * 适用场景:
 * - 访问外部服务 (数据库、API)
 * - 提高吞吐量
 */
public class AsyncIOExample {

    /**
     * 1. AsyncFunction - 异步请求
     */
    public static abstract class AsyncDatabaseRequest
            extends RichAsyncFunction<Order, Order> {

        private transient DatabaseClient client;
        private transient ExecutorService executor;

        @Override
        public void open(Configuration parameters) {
            // 初始化连接池
            client = new DatabaseClient("localhost", 3306);
            executor = Executors.newFixedThreadPool(20);
        }

        @Override
        public void close() {
            // 清理资源
            executor.shutdown();
            client.close();
        }

        @Override
        public void asyncInvoke(Order order, ResultFuture<Order> resultFuture) {
            // 异步查询
            CompletableFuture<Customer> customerFuture =
                CompletableFuture.supplyAsync(
                    () -> client.queryCustomer(order.getCustomerId()),
                    executor
                );

            // 回调处理
            customerFuture.thenAccept(customer -> {
                Order enrichedOrder = enrichOrder(order, customer);
                resultFuture.complete(
                    java.util.Collections.singletonList(enrichedOrder)
                );
            });
        }

        @Override
        public void timeout(Order input, ResultFuture<Order> resultFuture) {
            // 超时处理
            resultFuture.completeExceptionally(
                new TimeoutException("Request timeout")
            );
        }

        private Order enrichOrder(Order order, Customer customer) {
            order.setCustomerName(customer.getName());
            return order;
        }
    }

    /**
     * 2. 使用异步 IO
     */
    public static void asyncIOExample() {
        DataStream<Order> orders = env.fromCollection(getOrders());

        // 异步 IO
        DataStream<Order> enriched = AsyncDataStream
            .unorderedWait(
                orders,
                new AsyncDatabaseRequest(),
                5000,  // 超时时间
                java.util.concurrent.TimeUnit.MILLISECONDS,
                100    // 最大并发请求数
            );
    }

    /**
     * 3. 批量异步请求
     */
    public static class BatchAsyncFunction
            extends RichAsyncFunction<Order, Order> {

        private transient BatchDatabaseClient client;

        @Override
        public void open(Configuration parameters) {
            client = new BatchDatabaseClient();
        }

        @Override
        public void asyncInvoke(Order order, ResultFuture<Order> resultFuture) {
            // 批量查询
            // 使用集合收集请求，然后批量处理
        }

        @Override
        public void timeout(Order order, ResultFuture<Order> resultFuture) {
            // 超时处理
        }
    }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Flink 架构详解](01-architecture.md) | Flink 核心架构和概念 |
| [Flink Table API / SQL](03-table-sql.md) | Table API 和 SQL 详解 |
| [Flink 状态管理](04-state-checkpoint.md) | 状态管理和 Checkpoint |
| [Flink CEP](05-cep.md) | 复杂事件处理 |
| [Flink 运维指南](06-operations.md) | 集群部署和运维 |
