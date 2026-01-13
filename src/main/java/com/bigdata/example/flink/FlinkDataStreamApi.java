package com.bigdata.example.flink;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.datastream.api.AsyncDataStream;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.CompletableFuture;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

/**
 * Flink DataStream API 完整学习代码
 * 包含: DataStream基础、转换操作、窗口机制、水位线、异步IO等
 */
public class FlinkDataStreamApi {

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("   Flink DataStream API 完整学习代码");
        System.out.println("========================================\n");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setBufferTimeout(100);

        // ==================== DataStream基础 ====================
        System.out.println("【DataStream基础】");
        demonstrateDataStreamBasics(env);

        // ==================== DataStream转换操作 ====================
        System.out.println("\n【DataStream转换操作】");
        demonstrateTransformations(env);

        // ==================== KeyBy操作 ====================
        System.out.println("\n【KeyBy操作】");
        demonstrateKeyByOperations(env);

        // ==================== 多流操作 ====================
        System.out.println("\n【多流操作】");
        demonstrateMultiStreamOperations(env);

        // ==================== RichFunction详解 ====================
        System.out.println("\n【RichFunction详解】");
        demonstrateRichFunction(env);

        // ==================== 数据序列化机制 ====================
        System.out.println("\n【数据序列化机制】");
        demonstrateSerialization(env);

        // ==================== TypeInformation ====================
        System.out.println("\n【TypeInformation】");
        demonstrateTypeInformation(env);

        // ==================== 数据传输策略 ====================
        System.out.println("\n【数据传输策略】");
        demonstratePartitionStrategies(env);

        // ==================== 异步IO ====================
        System.out.println("\n【异步IO】");
        demonstrateAsyncIO(env);

        // ==================== 窗口机制 ====================
        System.out.println("\n【窗口机制】");
        demonstrateWindowMechanisms(env);

        // ==================== Watermark机制 ====================
        System.out.println("\n【Watermark机制】");
        demonstrateWatermarkMechanisms(env);

        // ==================== 双流Join ====================
        System.out.println("\n【双流Join】");
        demonstrateDualStreamJoin(env);

        // ==================== Flink作业部署 ====================
        System.out.println("\n【Flink作业部署架构】");
        demonstrateJobDeployment(env);

        System.out.println("\n========================================");
        System.out.println("   Flink DataStream API学习代码执行完成!");
        System.out.println("========================================");
    }

    // ==================== DataStream基础 ====================

    /**
     * DataStream API核心概念:
     * 1. Source: 数据源 (Kafka, File, Socket, Collection)
     * 2. Transformation: 转换操作 (map, filter, flatMap等)
     * 3. Sink: 数据输出 (Kafka, File, Print, JDBC)
     *
     * SourceFunction并行度只能是1的原因:
     * - 保证数据全局有序
     * - 保证数据完整性
     * - 需要维护全局状态
     */
    private static void demonstrateDataStreamBasics(StreamExecutionEnvironment env) {
        System.out.println("\n--- DataStream创建方式 ---");

        // 1. 从集合创建
        DataStream<String> collectionStream = env.fromCollection(
                Arrays.asList("a", "b", "c", "d", "e")
        );

        // 2. 从元素创建
        DataStream<Integer> elementStream = env.fromElements(1, 2, 3, 4, 5);

        // 3. 从文件创建 (批处理)
        // DataStream<String> fileStream = env.readTextFile("path/to/file.txt");

        // 4. Socket数据源
        // DataStream<String> socketStream = env.socketTextStream("localhost", 9999);

        // 5. 自定义Source (并行度=1)
        DataStream<Long> customSource = env.addSource(new RichParallelSourceFunction<Long>() {
            private volatile boolean running = true;
            private long count = 0;

            @Override
            public void run(SourceContext<Long> ctx) {
                while (running && count < 100) {
                    ctx.collect(count++);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        break;
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        }).setParallelism(1);  // SourceFunction并行度只能为1

        // 6. Kafka Source
        // DataStream<String> kafkaStream = env.addSource(
        //     new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), props)
        // );

        System.out.println("Source类型:");
        System.out.println("  1. 预定义Source: fromCollection, fromElements, readTextFile");
        System.out.println("  2. 自定义Source: SourceFunction, RichParallelSourceFunction");
        System.out.println("  3. 第三方Source: Kafka, JDBC, ES, Redis");
        System.out.println();
        System.out.println("SourceFunction并行度只能为1的原因:");
        System.out.println("  - 保证数据全局有序性");
        System.out.println("  - 需要维护全局消费offset");
        System.out.println("  - 保证数据不重复不丢失");
    }

    // ==================== DataStream转换操作 ====================

    /**
     * 转换操作分类:
     * 1. Map: 一对一转换
     * 2. FlatMap: 一对多转换
     * 3. Filter: 过滤
     * 4. KeyBy: 分组
     * 5. Reduce: 聚合
     * 6. Aggregate: 聚合
     */
    private static void demonstrateTransformations(StreamExecutionEnvironment env) {
        System.out.println("\n--- DataStream转换操作 ---");

        DataStream<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // 1. Map操作 (一对一转换)
        DataStream<Integer> mapStream = source.map(new MapFunction<Integer, Integer>() {
            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        });

        // 2. Filter操作 (过滤)
        DataStream<Integer> filterStream = source.filter(new FilterFunction<Integer>() {
            @Override
            public boolean filter(Integer value) throws Exception {
                return value % 2 == 0;  // 只保留偶数
            }
        });

        // 3. FlatMap操作 (一对多)
        DataStream<String> flatMapStream = source.flatMap(new FlatMapFunction<Integer, String>() {
            @Override
            public void flatMap(Integer value, Collector<String> out) throws Exception {
                out.collect("number: " + value);
                if (value > 5) {
                    out.collect("large: " + value);
                }
            }
        });

        // 4. ProcessFunction (底层操作)
        DataStream<String> processStream = source.process(new ProcessFunction<Integer, String>() {
            @Override
            public void processElement(Integer value, Context ctx, Collector<String> out) {
                out.collect("processed: " + value);
                // 可以访问Timer, State, 侧输出流等
            }
        });

        System.out.println("转换操作说明:");
        System.out.println("  1. Map: 一对一映射 (1个输入 -> 1个输出)");
        System.out.println("  2. FlatMap: 一对多映射 (1个输入 -> 0~N个输出)");
        System.out.println("  3. Filter: 过滤 (保留/丢弃元素)");
        System.out.println("  4. ProcessFunction: 底层API, 访问状态和Timer");
    }

    // ==================== KeyBy操作 ====================

    /**
     * KeyBy原理:
     * 1. 根据Key对数据分区
     * 2. 相同Key的数据到同一Task
     * 3. 使用KeySelector选择Key
     * 4. 内部使用Shuffle进行分区
     */
    private static void demonstrateKeyByOperations(StreamExecutionEnvironment env) {
        System.out.println("\n--- KeyBy操作 ---");

        DataStream<Tuple2<String, Integer>> source = env.fromElements(
                Tuple2.of("A", 1),
                Tuple2.of("B", 2),
                Tuple2.of("A", 3),
                Tuple2.of("B", 4),
                Tuple2.of("C", 5),
                Tuple2.of("A", 6)
        );

        // KeyBy - 按第一个字段分组
        KeyedStream<Tuple2<String, Integer>, String> keyedStream1 = source
                .keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
                    @Override
                    public String getKey(Tuple2<String, Integer> value) throws Exception {
                        return value.f0;
                    }
                });

        // KeyBy - 使用Lambda表达式 (需要注意序列化问题)
        KeyedStream<Tuple2<String, Integer>, String> keyedStream2 = source
                .keyBy(t -> t.f0);

        // KeyBy后进行Sum操作
        DataStream<Tuple2<String, Integer>> sumResult = source
                .keyBy(t -> t.f0)
                .sum(1);

        // KeyBy后进行Reduce操作
        DataStream<Tuple2<String, Integer>> reduceResult = source
                .keyBy(t -> t.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> v1,
                                                          Tuple2<String, Integer> v2) {
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                });

        System.out.println("KeyBy操作说明:");
        System.out.println("  1. KeySelector: 定义如何从元素中提取Key");
        System.out.println("  2. keyBy(t -> t.field): 快速指定Key");
        System.out.println("  3. sum/first/reduce: KeyBy后的聚合操作");
        System.out.println();
        System.out.println("KeyBy注意事项:");
        System.out.println("  - Key不能为null");
        System.out.println("  - 大量不同Key可能导致数据倾斜");
        System.out.println("  - 使用Tuple作为Key时注意索引越界");
    }

    // ==================== 多流操作 ====================

    /**
     * 多流操作:
     * 1. Union: 合并多个同类型流
     * 2. Connect: 连接两个不同类型流
     * 3. CoFlatMap: 连接后的扁平映射
     */
    private static void demonstrateMultiStreamOperations(StreamExecutionEnvironment env) {
        System.out.println("\n--- 多流操作 ---");

        DataStream<String> stream1 = env.fromElements("A1", "A2", "A3");
        DataStream<String> stream2 = env.fromElements("B1", "B2");
        DataStream<String> stream3 = env.fromElements("C1", "C2", "C3");

        // 1. Union合并多个流
        DataStream<String> unionStream = stream1.union(stream2, stream3);

        // 2. Connect连接两个流 (不同类型)
        DataStream<Integer> intStream = env.fromElements(1, 2, 3);
        DataStream<String> stringStream = env.fromElements("X", "Y", "Z");

        ConnectedStreams<Integer, String> connected = intStream.connect(stringStream);

        // 3. CoMap/CoFlatMap
        DataStream<String> coMapResult = connected
                .coMap(new CoMapFunction<Integer, String, String>() {
                    @Override
                    public String map1(Integer value) {
                        return "Int: " + value;
                    }

                    @Override
                    public String map2(String value) {
                        return "Str: " + value;
                    }
                });

        // 4. CoFlatMap示例
        DataStream<String> coFlatMapResult = connected
                .coFlatMap(new RichCoFlatMapFunction<Integer, String, String>() {
                    @Override
                    public void flatMap1(Integer value, Collector<String> out) {
                        out.collect("Int: " + value);
                    }

                    @Override
                    public void flatMap2(String value, Collector<String> out) {
                        out.collect("Str: " + value);
                    }
                });

        System.out.println("多流操作说明:");
        System.out.println("  1. Union: 合并同类型流, 元素交错输出");
        System.out.println("  2. Connect: 连接两个不同类型流, 保持独立性");
        System.out.println("  3. CoMap/CoFlatMap: 对连接后的流进行转换");
        System.out.println();
        System.out.println("Connect vs Union:");
        System.out.println("  - Connect: 保留两个流独立, 可分别处理");
        System.out.println("  - Union: 合并成一个流, 无法区分来源");
    }

    // ==================== RichFunction详解 ====================

    /**
     * RichFunction家族:
     * 1. RichMapFunction
     * 2. RichFlatMapFunction
     * 3. RichFilterFunction
     * 4. RichReduceFunction
     * 5. RichSourceFunction
     * 6. RichSinkFunction
     *
     * RichFunction提供:
     * - open(): 初始化方法
     * - close(): 清理方法
     * - getRuntimeContext(): 获取运行时上下文
     */
    private static void demonstrateRichFunction(StreamExecutionEnvironment env) {
        System.out.println("\n--- RichFunction示例 ---");

        DataStream<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        // RichMapFunction示例
        DataStream<String> richMapStream = source.map(new RichMapFunction<Integer, String>() {
            private transient LongCounter counter;
            private transient ValueState<Integer> sumState;

            @Override
            public void open(Configuration parameters) {
                // 1. 获取运行时上下文
                // 2. 初始化累加器
                counter = getRuntimeContext().getLongCounter("my-counter");
                // 3. 初始化状态
                ValueStateDescriptor<Integer> descriptor =
                        new ValueStateDescriptor<>("sum-state", Integer.class);
                sumState = getRuntimeContext().getState(descriptor);
            }

            @Override
            public String map(Integer value) throws Exception {
                counter.add(1);  // 记录处理数量
                Integer currentSum = sumState.value();
                if (currentSum == null) currentSum = 0;
                sumState.update(currentSum + value);
                return "value=" + value;
            }

            @Override
            public void close() {
                // 清理资源
                System.out.println("Total processed: " + counter.get());
            }
        });

        // RichSinkFunction示例
        DataStream<String> sinkStream = richMapStream.addSink(new RichSinkFunction<String>() {
            private transient Connection connection;

            @Override
            public void open(Configuration parameters) {
                // 初始化数据库连接
                connection = new Connection();
                System.out.println("Sink opened");
            }

            @Override
            public void invoke(String value, Context context) {
                // 写入数据
                System.out.println("Writing: " + value);
            }

            @Override
            public void close() {
                // 关闭连接
                if (connection != null) connection.close();
                System.out.println("Sink closed");
            }
        });

        System.out.println("RichFunction生命周期:");
        System.out.println("  1. open(): 任务初始化时调用一次");
        System.out.println("  2. map/flatMap/filter...(): 每条数据处理时调用");
        System.out.println("  3. close(): 任务结束时调用一次");
        System.out.println();
        System.out.println("RichFunction可访问:");
        System.out.println("  - RuntimeContext (获取累加器、状态等)");
        System.out.println("  - 分布式缓存");
        System.out.println("  - 广播状态");
    }

    // ==================== 数据序列化机制 ====================

    /**
     * Flink序列化机制:
     * 1. TypeInformation: 类型信息描述
     * 2. TypeSerializer: 类型序列化器
     * 3. 支持Kryo, Avro, Java Serialization
     *
     * 序列化器选择优先级:
     * 1. Flink内置类型 (BasicTypeInfo)
     * 2. Avro类型
     * 3. Kryo序列化
     * 4. Java Serialization
     */
    private static void demonstrateSerialization(StreamExecutionEnvironment env) {
        System.out.println("\n--- 数据序列化机制 ---");

        // 1. Flink内置类型序列化
        DataStream<String> stringStream = env.fromElements("a", "b", "c");
        DataStream<Integer> intStream = env.fromElements(1, 2, 3);
        DataStream<Long> longStream = env.fromElements(1L, 2L, 3L);
        DataStream<Double> doubleStream = env.fromElements(1.0, 2.0, 3.0);

        // 2. Tuple类型序列化
        DataStream<Tuple2<String, Integer>> tupleStream = env.fromElements(
                Tuple2.of("A", 1),
                Tuple2.of("B", 2)
        );

        // 3. POJO类型序列化
        DataStream<UserAction> pojoStream = env.fromElements(
                new UserAction("user1", "login", 1000L),
                new UserAction("user2", "purchase", 2000L)
        );

        // 4. 自定义Kryo序列化器
        ExecutionConfig config = env.getConfig();
        config.registerKryoType(CustomClass.class);
        config.registerKryoSerializer(CustomClass.class, CustomKryoSerializer.class);

        System.out.println("Flink序列化机制:");
        System.out.println("  1. 基本类型: String, Integer, Long, Double等");
        System.out.println("  2. 数组类型: int[], String[]等");
        System.out.println("  3. 复合类型: Tuple, POJO");
        System.out.println();
        System.out.println("序列化器类型:");
        System.out.println("  - StringSerializer: 字符串");
        System.out.println("  - IntSerializer: 整型");
        System.out.println("  - TupleSerializer: 元组");
        System.out.println("  - AvroSerializer: Avro类型");
        System.out.println("  - KryoSerializer: 通用类型");
        System.out.println();
        System.out.println("Lambda表达式序列化问题:");
        System.out.println("  - 使用Lambda时需要显式指定类型信息");
        System.out.println("  - 或使用returns()方法指定返回类型");
    }

    // ==================== TypeInformation ====================

    private static void demonstrateTypeInformation(StreamExecutionEnvironment env) {
        System.out.println("\n--- TypeInformation详解 ---");

        // 获取TypeInformation
        TypeInformation<String> stringInfo = TypeInformation.of(String.class);
        TypeInformation<Integer> intInfo = TypeInformation.of(Integer.class);
        TypeInformation<Tuple2<String, Integer>> tupleInfo =
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {});
        TypeInformation<UserAction> pojoInfo =
                TypeInformation.of(UserAction.class);

        System.out.println("TypeInformation类型:");
        System.out.println("  1. BasicTypeInfo: 基本类型 (String, Integer, Long...)");
        System.out.println("  2. TupleTypeInfo: 元组类型 (Tuple2, Tuple3...)");
        System.out.println("  3. CaseClassTypeInfo: Scala Case Class");
        System.out.println("  4. PojoTypeInfo: POJO类型");
        System.out.println("  5. GenericTypeInfo: 通用类型");
        System.out.println();
        System.out.println("TypeInformation作用:");
        System.out.println("  - 自动推断数据类型");
        System.out.println("  - 创建合适的序列化器");
        System.out.println("  - 确定Key分组策略");
        System.out.println("  - 生成TypeSerializer");

        // 自定义TypeInformation
        TypeInformation<CustomClass> customInfo = new CustomClassTypeInfo();
    }

    // ==================== 数据传输策略 ====================

    /**
     * 数据传输策略 (Partition Strategies):
     * 1. Forward: Forward策略, 上下游并行度相同时一对一传递
     * 2. Rebalance: 轮询分发
     * 3. Random: 随机分发
     * 4. Hash: 按Key的Hash值分发
     * 5. Broadcast: 广播到所有分区
     * 6. Global: 所有数据发送到第一个分区
     * 7. Rescale: 本地轮询分发
     * 8. Custom: 自定义分发策略
     */
    private static void demonstratePartitionStrategies(StreamExecutionEnvironment env) {
        System.out.println("\n--- 数据传输策略 ---");

        DataStream<Integer> source = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        source.setParallelism(4);

        // 1. Forward策略 (默认, 上下游并行度相同时)
        // 上游一个分区 -> 下游对应分区
        DataStream<Integer> forwardStream = source
                .map(x -> x)
                .setParallelism(4)
                .forward();

        // 2. Rebalance策略 (轮询分发)
        DataStream<Integer> rebalanceStream = source
                .map(x -> x)
                .rebalance();

        // 3. Rescale策略 (本地轮询, 比Rebalance更高效)
        // 上下游在同一机器上的分区轮询
        DataStream<Integer> rescaleStream = source
                .map(x -> x)
                .rescale();

        // 4. Hash策略 (按Key Hash分发)
        DataStream<Tuple2<String, Integer>> keyedSource = env.fromElements(
                Tuple2.of("A", 1),
                Tuple2.of("B", 2),
                Tuple2.of("A", 3),
                Tuple2.of("B", 4)
        );
        DataStream<String> hashStream = keyedSource
                .keyBy(t -> t.f0)
                .map(x -> x.f0)
                .setParallelism(4);

        // 5. Broadcast策略 (广播到所有分区)
        DataStream<Integer> broadcastStream = source
                .broadcast()
                .map(x -> x);

        // 6. Global策略 (所有数据发送到第一个分区)
        DataStream<Integer> globalStream = source
                .global()
                .map(x -> x);

        // 7. Custom策略 (自定义分发)
        DataStream<Integer> customStream = source
                .partitionCustom(new Partitioner<Integer>() {
                    @Override
                    public int partition(Integer key, int numPartitions) {
                        return key % numPartitions;  // 按key % partitions分发
                    }
                }, t -> t)
                .map(x -> x);

        System.out.println("数据传输策略说明:");
        System.out.println("  1. Forward: 上游->下游对应分区, 需要相同并行度");
        System.out.println("  2. Rebalance: 轮询所有下游分区");
        System.out.println("  3. Rescale: 本地轮询, 减少网络传输");
        System.out.println("  4. Hash: 按Key Hash分发, 相同Key到相同分区");
        System.out.println("  5. Broadcast: 广播到所有分区");
        System.out.println("  6. Global: 所有数据到第一个分区");
        System.out.println("  7. Custom: 用户自定义分区逻辑");
    }

    // ==================== 异步IO ====================

    /**
     * 异步IO目的:
     * - 提升同步IO的吞吐量
     * - 减少等待时间
     *
     * 实现方式:
     * 1. AsyncDataStream.unorderedWait(): 无序返回
     * 2. AsyncDataStream.orderedWait(): 有序返回
     */
    private static void demonstrateAsyncIO(StreamExecutionEnvironment env) {
        System.out.println("\n--- 异步IO示例 ---");

        DataStream<Integer> source = env.fromElements(1, 2, 3, 4, 5);

        // 模拟异步请求
        DataStream<String> asyncResult = AsyncDataStream.unorderedWait(
                source,
                new RichAsyncFunction<Integer, String>() {
                    private transient java.net.http.HttpClient client;

                    @Override
                    public void open(Configuration parameters) {
                        client = java.net.http.HttpClient.newHttpClient();
                    }

                    @Override
                    public void asyncInvoke(Integer input,
                                           ResultFuture<String> resultFuture) {
                        // 异步发起请求
                        CompletableFuture.supplyAsync(() -> {
                            try {
                                // 模拟外部API调用
                                Thread.sleep(100);
                                return "Result-" + input;
                            } catch (Exception e) {
                                return "Error-" + input;
                            }
                        }).thenAccept(resultFuture::complete);
                    }

                    @Override
                    public void timeout(Integer input, ResultFuture<String> resultFuture) {
                        resultFuture.complete(Collections.singletonList("Timeout-" + input));
                    }
                },
                3000,  // 超时时间
                TimeUnit.MILLISECONDS,
                100    // 最大并发请求数
        );

        System.out.println("异步IO说明:");
        System.out.println("  1. unorderedWait: 无序返回, 性能更好");
        System.out.println("  2. orderedWait: 有序返回, 保持原始顺序");
        System.out.println();
        System.out.println("异步IO参数:");
        System.out.println("  - timeout: 请求超时时间");
        System.out.println("  - capacity: 最大并发请求数");
        System.out.println();
        System.out.println("使用场景:");
        System.out.println("  - 访问外部REST API");
        System.out.println("  - 访问数据库 (异步JDBC)");
        System.out.println("  - 访问缓存 (Redis异步客户端)");
    }

    // ==================== 窗口机制 ====================

    /**
     * 窗口类型:
     * 1. TimeWindow: 时间窗口
     *    - TumblingEventTimeWindow: 滚动时间窗口
     *    - SlidingEventTimeWindow: 滑动时间窗口
     *    - EventTimeSessionWindow: 会话窗口
     *
     * 2. CountWindow: 计数窗口
     *    - TumblingCountWindow: 滚动计数窗口
     *    - SlidingCountWindow: 滑动计数窗口
     *
     * 3. GlobalWindow: 全局窗口
     *    - 配合Trigger使用
     */
    private static void demonstrateWindowMechanisms(StreamExecutionEnvironment env) {
        System.out.println("\n--- 窗口机制 ---");

        // 模拟带时间戳的数据流
        DataStream<Tuple2<String, Integer>> source = env.fromElements(
                Tuple2.of("A", 1),
                Tuple2.of("A", 2),
                Tuple2.of("B", 3),
                Tuple2.of("A", 4),
                Tuple2.of("B", 5)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, ts) -> System.currentTimeMillis())
        );

        // 1. 滚动时间窗口 (Tumbling Window)
        // 每5秒一个窗口, 窗口之间不重叠
        DataStream<Integer> tumblingWindow = source
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                .sum(1);

        // 2. 滑动时间窗口 (Sliding Window)
        // 窗口大小10秒, 滑动步长5秒
        DataStream<Integer> slidingWindow = source
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .sum(1);

        // 3. 会话窗口 (Session Window)
        // 基于数据间隔, 间隔超过gap则开启新窗口
        DataStream<Integer> sessionWindow = source
                .keyBy(t -> t.f0)
                .window(EventTimeSessionWindows.withGap(Duration.ofSeconds(5)))
                .sum(1);

        // 4. 滚动计数窗口 (每3条数据一个窗口)
        DataStream<Integer> tumblingCountWindow = source
                .keyBy(t -> t.f0)
                .countWindow(3)
                .sum(1);

        // 5. 滑动计数窗口 (窗口3条, 滑动2条)
        DataStream<Integer> slidingCountWindow = source
                .keyBy(t -> t.f0)
                .countWindow(3, 2)
                .sum(1);

        // 6. 全局窗口 (Global Window) + 自定义Trigger
        DataStream<Integer> globalWindow = source
                .keyBy(t -> t.f0)
                .window(GlobalWindows.create())
                .trigger(new Trigger<Tuple2<String, Integer>, GlobalWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple2<String, Integer> element,
                                                   long timestamp,
                                                   GlobalWindow window,
                                                   TriggerContext ctx) {
                        // 每累积3条数据触发一次
                        return TriggerResult.FIRE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time,
                                                          GlobalWindow window,
                                                          TriggerContext ctx) {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time,
                                                     GlobalWindow window,
                                                     TriggerContext ctx) {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(GlobalWindow window, TriggerContext ctx) {
                    }
                })
                .sum(1);

        System.out.println("窗口类型说明:");
        System.out.println("  1. Tumbling Window: 滚动窗口, 无重叠");
        System.out.println("  2. Sliding Window: 滑动窗口, 有重叠");
        System.out.println("  3. Session Window: 会话窗口, 基于间隔");
        System.out.println("  4. Count Window: 计数窗口, 基于数量");
        System.out.println("  5. Global Window: 全局窗口, 需配合Trigger");
        System.out.println();
        System.out.println("窗口函数类型:");
        System.out.println("  1. 增量聚合: ReduceFunction, AggregateFunction");
        System.out.println("  2. 全量聚合: ProcessWindowFunction");
    }

    // ==================== Watermark机制 ====================

    /**
     * Watermark (水位线):
     * - 衡量事件时间进度的机制
     * - 用于处理乱序数据和迟到数据
     *
     * Watermark策略:
     * 1. forBoundedOutOfOrderness: 容忍最大乱序时间
     * 2. forMonotonousTimestamps: 单调递增时间戳
     * 3. NoWatermarks: 不生成Watermark
     */
    private static void demonstrateWatermarkMechanisms(StreamExecutionEnvironment env) {
        System.out.println("\n--- Watermark机制 ---");

        // 模拟乱序数据
        DataStream<Tuple2<String, Long>> source = env.fromElements(
                Tuple2.of("event1", 1000L),   // 正常
                Tuple2.of("event2", 2500L),   // 正常
                Tuple2.of("event3", 1500L),   // 乱序: 应该比2500早
                Tuple2.of("event4", 3000L),   // 正常
                Tuple2.of("event5", 2000L)    // 乱序
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, ts) -> event.f1)
        );

        // 1. Periodic Watermark (周期性生成)
        // 默认每200ms生成一次
        WatermarkStrategy<Tuple2<String, Long>> periodicStrategy =
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((e, ts) -> e.f1)
                        .withIdleness(Duration.ofSeconds(10));  // 空闲超时

        // 2. Punctuated Watermark (事件驱动生成)
        WatermarkStrategy<Tuple2<String, Long>> punctuatedStrategy =
                WatermarkStrategy.<Tuple2<String, Long>>forGenerator(ctx -> new WatermarkGenerator<Tuple2<String, Long>>() {
                    private long maxTimestamp = Long.MIN_VALUE;

                    @Override
                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp,
                                        WatermarkOutput output) {
                        // 每个事件都检查是否需要生成Watermark
                        if (event.f1 > maxTimestamp) {
                            maxTimestamp = event.f1;
                        }
                        // 当遇到特定标记时生成Watermark
                        if (event.f0.contains("marker")) {
                            output.emitWatermark(
                                    new Watermark(maxTimestamp - Duration.ofSeconds(5).toMillis())
                            );
                        }
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        // 周期性检查
                        output.emitWatermark(
                                new Watermark(maxTimestamp - Duration.ofSeconds(5).toMillis())
                        );
                    }
                }).withTimestampAssigner((e, ts) -> e.f1);

        // 3. 自定义Watermark Generator
        WatermarkStrategy<Tuple2<String, Long>> customStrategy =
                WatermarkStrategy.<Tuple2<String, Long>>forGenerator(ctx -> new WatermarkGenerator<Tuple2<String, Long>>() {
                    private long maxOutOfOrderness = Duration.ofSeconds(5).toMillis();
                    private long currentMaxTimestamp = Long.MIN_VALUE;

                    @Override
                    public void onEvent(Tuple2<String, Long> event, long eventTimestamp,
                                        WatermarkOutput output) {
                        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
                    }

                    @Override
                    public void onPeriodicEmit(WatermarkOutput output) {
                        long watermark = currentMaxTimestamp - maxOutOfOrderness;
                        output.emitWatermark(new Watermark(watermark));
                    }
                }).withTimestampAssigner((e, ts) -> e.f1);

        System.out.println("Watermark说明:");
        System.out.println("  1. Watermark = maxEventTime - maxOutOfOrderness");
        System.out.println("  2. Watermark表示系统认为不会再有更早的数据");
        System.out.println("  3. 当Watermark >= windowEndTime时触发窗口计算");
        System.out.println();
        System.out.println("Watermark生成策略:");
        System.out.println("  1. Periodic: 周期性生成 (默认200ms)");
        System.out.println("  2. Punctuated: 事件驱动生成");
        System.out.println();
        System.out.println("Watermark问题排查:");
        System.out.println("  1. 水位线不推进: 检查时间戳分配");
        System.out.println("  2. 窗口不触发: 检查Watermark是否 >= windowEnd");
        System.out.println("  3. 数据迟到: 调整outOfOrderness或使用侧输出流");
    }

    // ==================== 双流Join ====================

    /**
     * 双流Join方式:
     * 1. Window Join: 基于窗口的Join
     * 2. Interval Join: 基于时间区间的Join
     * 3. CoGroup: 类似Window Join但更灵活
     * 4. Connect + CoProcess: 最灵活的Join方式
     */
    private static void demonstrateDualStreamJoin(StreamExecutionEnvironment env) {
        System.out.println("\n--- 双流Join方式 ---");

        // 订单流
        DataStream<Tuple2<String, Double>> orders = env.fromElements(
                Tuple2.of("user1", 100.0),
                Tuple2.of("user1", 200.0),
                Tuple2.of("user2", 150.0)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Double>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, ts) -> System.currentTimeMillis())
        );

        // 支付流
        DataStream<Tuple2<String, Double>> payments = env.fromElements(
                Tuple2.of("user1", 100.0),
                Tuple2.of("user1", 200.0),
                Tuple2.of("user3", 300.0)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Double>>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((e, ts) -> System.currentTimeMillis())
        );

        // 1. Window Join (Inner Join)
        DataStream<String> windowJoinResult = orders
                .join(payments)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply((order, payment, out) -> {
                    out.collect("Order: " + order + " <-> Payment: " + payment);
                });

        // 2. Full Outer Join
        DataStream<String> fullOuterJoinResult = orders
                .fullOuterJoin(payments)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply((order, payment) -> {
                    if (order != null && payment != null) {
                        return "BOTH: " + order + " <-> " + payment;
                    } else if (order != null) {
                        return "ORDER_ONLY: " + order;
                    } else {
                        return "PAYMENT_ONLY: " + payment;
                    }
                });

        // 3. Interval Join
        DataStream<String> intervalJoinResult = orders
                .keyBy(t -> t.f0)
                .intervalJoin(payments.keyBy(t -> t.f0))
                .between(Duration.ofSeconds(-5), Duration.ofSeconds(5))
                .process((left, right, out) -> {
                    out.collect("Interval Join: " + left + " with " + right);
                });

        // 4. CoGroup
        DataStream<String> coGroupResult = orders
                .coGroup(payments)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply((orderIter, paymentIter, out) -> {
                    for (Tuple2<String, Double> order : orderIter) {
                        for (Tuple2<String, Double> payment : paymentIter) {
                            out.collect("CoGroup: " + order + " with " + payment);
                        }
                    }
                });

        // 5. Connect + CoProcess (最灵活)
        DataStream<String> connectResult = orders
                .connect(payments)
                .keyBy(t -> t.f0, t -> t.f0)
                .process(new CoProcessFunction<Tuple2<String, Double>, Tuple2<String, Double>, String>() {
                    private transient ValueState<Tuple2<String, Double>> pendingOrder;
                    private transient ValueState<Tuple2<String, Double>> pendingPayment;

                    @Override
                    public void processElement1(Tuple2<String, Double> order,
                                                Context ctx, Collector<String> out) {
                        Tuple2<String, Double> payment = pendingPayment.value();
                        if (payment != null) {
                            out.collect("Matched: " + order + " with " + payment);
                            pendingPayment.clear();
                        } else {
                            pendingOrder.update(order);
                        }
                    }

                    @Override
                    public void processElement2(Tuple2<String, Double> payment,
                                                Context ctx, Collector<String> out) {
                        Tuple2<String, Double> order = pendingOrder.value();
                        if (order != null) {
                            out.collect("Matched: " + order + " with " + payment);
                            pendingOrder.clear();
                        } else {
                            pendingPayment.update(payment);
                        }
                    }
                });

        System.out.println("双流Join方式说明:");
        System.out.println("  1. Window Join: 基于窗口的Inner/Outer Join");
        System.out.println("  2. Interval Join: 基于时间区间的Join, 更灵活");
        System.out.println("  3. CoGroup: 类似Window Join, 更灵活的数据遍历");
        System.out.println("  4. Connect+CoProcess: 最灵活, 可完全控制匹配逻辑");
        System.out.println();
        System.out.println("双流Join痛点:");
        System.out.println("  1. 数据倾斜: 某个Key数据量过大");
        System.out.println("  2. 状态膨胀: 未匹配数据积压");
        System.out.println("  3. 延迟高: 等待时间长");
    }

    // ==================== Flink作业部署架构 ====================

    private static void demonstrateJobDeployment(StreamExecutionEnvironment env) {
        System.out.println("\n--- Flink作业部署架构 ---");

        System.out.println("Flink运行模式:");
        System.out.println("  1. Session模式: 共享TM, 适合短作业");
        System.out.println("  2. Per-Job模式: 独享TM, 适合长作业");
        System.out.println("  3. Application模式: 在集群编译, 适合大型作业");
        System.out.println();

        System.out.println("Flink核心组件:");
        System.out.println("  1. JobManager: 作业调度器");
        System.out.println("  2. TaskManager: 任务执行器");
        System.out.println("  3. Slot: 资源调度单元");
        System.out.println("  4. Operator Chain: 算子链");
        System.out.println();

        System.out.println("并行度概念:");
        System.out.println("  1. Operator Parallelism: 单个算子的并行度");
        System.out.println("  2. Slot Parallelism: 每个Slot的并行度");
        System.out.println("  3. Max Parallelism: 最大并行度 (KeyGroup数量)");
        System.out.println();

        System.out.println("Operator Chain:");
        System.out.println("  - 减少数据序列化/反序列化");
        System.out.println("  - 减少网络传输");
        System.out.println("  - 提升吞吐量");
        System.out.println("  - 可通过disableOperatorChaining()禁用");
    }

    // ==================== 辅助类 ====================

    // POJO类 (用于演示序列化)
    public static class UserAction {
        public String userId;
        public String action;
        public long timestamp;

        public UserAction() {}

        public UserAction(String userId, String action, long timestamp) {
            this.userId = userId;
            this.action = action;
            this.timestamp = timestamp;
        }
    }

    // 自定义类 (用于演示Kryo序列化)
    public static class CustomClass {
        public String name;
        public int value;
    }

    // 自定义Kryo序列化器
    public static class CustomKryoSerializer extends org.apache.flink.api.common.typeutils.base.StringSerializer {
        @Override
        public void serialize(String value, DataOutputView target) throws java.io.IOException {
            super.serialize(value, target);
        }

        @Override
        public String deserialize(DataInputView source) throws java.io.IOException {
            return super.deserialize(source);
        }
    }

    // 自定义TypeInformation
    public static class CustomClassTypeInfo extends TypeInformation<CustomClass> {
        @Override
        public boolean isBasicType() { return false; }
        @Override
        public boolean isTupleType() { return false; }
        @Override
        public int getArity() { return 2; }
        @Override
        public int getTotalFields() { return 2; }
        @Override
        public Class<CustomClass> getTypeClass() { return CustomClass.class; }
        @Override
        public boolean isKeyType() { return false; }
        @Override
        public org.apache.flink.api.common.typeutils.TypeSerializer<CustomClass> createSerializer(ExecutionConfig config) {
            return new KryoSerializer<>(CustomClass.class, config);
        }
    }

    // 模拟连接类
    public static class Connection {
        public void close() {}
    }

    // Watermark Generator
    public static class CustomWatermarkGenerator<T> implements WatermarkGenerator<T> {
        private long maxTimestamp = Long.MIN_VALUE;

        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            output.emitWatermark(new Watermark(maxTimestamp - 5000));
        }
    }
}
