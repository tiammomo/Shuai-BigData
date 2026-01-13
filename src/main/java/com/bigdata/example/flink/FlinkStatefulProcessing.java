package com.bigdata.example.flink;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.CloseableIterator;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;

import java.time.Duration;
import java.util.*;

/**
 * Flink状态计算完整学习代码
 * 包含：状态分类、算子状态、键值状态、广播状态、状态TTL、时间语义、水位线等
 */
public class FlinkStatefulProcessing {

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("   Flink有状态计算完整学习代码");
        System.out.println("========================================\n");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setBufferTimeout(100);

        // 开启Checkpoint
        // env.enableCheckpointing(60000);
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // ==================== 状态分类 ====================
        System.out.println("【Flink状态的分类】");
        demonstrateStateCategories(env);

        // ==================== 算子状态 ====================
        System.out.println("\n【算子状态使用方法】");
        operatorStateExample(env);

        // ==================== 键值状态 ====================
        System.out.println("\n【键值状态使用方法】");
        keyedStateExample(env);

        // ==================== 键值状态API设计原理 ====================
        System.out.println("\n【键值状态API设计原理】");
        keyedStateAPIDesign(env);

        // ==================== UnionListState案例 ====================
        System.out.println("\n【UnionListState案例场景】");
        unionListStateExample(env);

        // ==================== 键值状态重分布机制 ====================
        System.out.println("\n【键值状态重分布机制】");
        keyedStateRedistribution(env);

        // ==================== 广播状态 ====================
        System.out.println("\n【广播状态使用方法】");
        broadcastStateExample(env);

        // ==================== 状态TTL ====================
        System.out.println("\n【状态TTL配置详解】");
        stateTTLExample(env);

        // ==================== 有状态计算案例 ====================
        System.out.println("\n【Flink有状态计算案例】");
        statefulComputationCase(env);

        // ==================== 时间语义总结 ====================
        System.out.println("\n【时间语义和时间窗口总结】");
        timeSemanticsSummary(env);

        // ==================== Watermark相关 ====================
        System.out.println("\n【Watermark详解】");
        watermarkExamples(env);

        // ==================== 数据乱序处理 ====================
        System.out.println("\n【数据乱序处理方案】");
        outOfOrderDataHandling(env);

        // ==================== 迟到数据处理 ====================
        System.out.println("\n【迟到数据处理方案】");
        lateDataSolutions(env);

        // ==================== 计数窗口 ====================
        System.out.println("\n【Flink计数窗口CountWindow】");
        countWindowExample(env);

        System.out.println("\n========================================");
        System.out.println("   Flink状态计算学习代码执行完成!");
        System.out.println("========================================");
    }

    // ==================== Flink状态的分类 ====================

    /**
     * Flink状态分类详解
     * 1. 算子状态 (Operator State) - 作用于算子任务
     *    - 类型: ListState, UnionListState, BroadcastState
     *    - 使用场景: Kafka Source偏移量、算子内部状态
     *
     * 2. 键值状态 (Keyed State) - 作用于KeyedStream
     *    - 类型: ValueState, ListState, MapState, ReducingState, AggregatingState
     *    - 使用场景: 按Key聚合、窗口状态、历史数据记录
     */
    private static void demonstrateStateCategories(StreamExecutionEnvironment env) {
        System.out.println("\n--- 状态分类演示 ---");

        // 1. 算子状态 - 通常在Source算子中使用
        // 例如: Kafka Source记录分区偏移量

        // 2. 键值状态 - 在KeyedProcessFunction中使用
        DataStream<Tuple2<String, Integer>> sourceStream = env.fromElements(
                Tuple2.of("user1", 100),
                Tuple2.of("user2", 200),
                Tuple2.of("user1", 150),
                Tuple2.of("user2", 300)
        );

        // 键值状态示例: 按用户累加金额
        DataStream<Tuple2<String, Long>> resultStream = sourceStream
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Long>>() {
                    // 键值状态: 存储每个Key的累计值
                    private transient ValueState<Long> sumState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Long> descriptor =
                                new ValueStateDescriptor<>("sum-state", Long.class);
                        sumState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx,
                                              Collector<Tuple2<String, Long>> out) throws Exception {
                        Long currentSum = sumState.value();
                        if (currentSum == null) currentSum = 0L;
                        currentSum += value.f1;
                        sumState.update(currentSum);
                        out.collect(Tuple2.of(value.f0, currentSum));
                    }
                });

        System.out.println("键值状态示例 - 按Key累加结果:");
        resultStream.print();

        // 算子状态示例: 并行度变化时的状态重分布
        // 使用ListState记录每个分区的处理位置
        System.out.println("\n算子状态类型:");
        System.out.println("  - ListState: 列表状态，并行度恢复时各任务取列表的一部分");
        System.out.println("  - UnionListState: 联合列表状态，并行度恢复时各任务获取完整列表");
        System.out.println("  - BroadcastState: 广播状态，并行度恢复时所有任务获取完整状态");
    }

    // ==================== 算子状态使用方法 ====================

    /**
     * 算子状态使用场景:
     * 1. Kafka Consumer记录消费偏移量
     * 2. 自定义Source记录数据位置
     * 3. 算子内部需要持久化的临时数据
     */
    private static void operatorStateExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- 算子状态示例 ---");

        // 模拟数据源
        DataStream<String> sourceStream = env.fromElements(
                "data1", "data2", "data3", "data4", "data5"
        );

        // 使用ListState实现算子状态
        DataStream<String> processedStream = sourceStream
                .map(new RichMapFunction<String, String>() {
                    // 算子状态: 每个并行实例独立维护
                    private transient ListState<String> operatorState;

                    @Override
                    public void open(Configuration parameters) {
                        // 创建算子状态描述符
                        ListStateDescriptor<String> descriptor =
                                new ListStateDescriptor<>("my-operator-state", String.class);
                        operatorState = getRuntimeContext().getListState(descriptor);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        // 模拟处理时记录状态
                        operatorState.add(value);
                        return value.toUpperCase();
                    }
                });

        // 算子状态恢复测试场景
        System.out.println("算子状态应用场景:");
        System.out.println("  1. Kafka Source: 记录分区消费偏移量");
        System.out.println("  2. 自定义Source: 记录数据读取位置");
        System.out.println("  3. 算子内部: 需要持久化的中间计算结果");
    }

    // ==================== 键值状态使用方法 ====================

    /**
     * 键值状态5种类型:
     * 1. ValueState<T> - 存储单个值
     * 2. ListState<T> - 存储列表
     * 3. MapState<K, V> - 存储键值对
     * 4. ReducingState<T> - 自动聚合的状态
     * 5. AggregatingState<IN, OUT> - 自定义聚合的状态
     */
    private static void keyedStateExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- 键值状态5种类型示例 ---");

        DataStream<Tuple3<String, String, Integer>> sourceStream = env.fromElements(
                Tuple3.of("user1", "order", 100),
                Tuple3.of("user1", "browse", 50),
                Tuple3.of("user2", "order", 200),
                Tuple3.of("user1", "order", 150),
                Tuple3.of("user2", "browse", 100)
        );

        // 1. ValueState - 记录上次操作
        DataStream<String> valueStateStream = sourceStream
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, String, Integer>, String>() {
                    private transient ValueState<String> lastActionState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<String> descriptor =
                                new ValueStateDescriptor<>("last-action", String.class);
                        lastActionState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple3<String, String, Integer> value, Context ctx,
                                              Collector<String> out) throws Exception {
                        String lastAction = lastActionState.value();
                        String currentAction = value.f1;
                        int amount = value.f2;

                        StringBuilder sb = new StringBuilder();
                        sb.append("User: ").append(value.f0);
                        sb.append(", LastAction: ").append(lastAction == null ? "null" : lastAction);
                        sb.append(", CurrentAction: ").append(currentAction);
                        sb.append(", Amount: ").append(amount);

                        // 更新状态
                        lastActionState.update(currentAction);
                        out.collect(sb.toString());
                    }
                });

        // 2. ListState - 记录操作历史
        DataStream<String> listStateStream = sourceStream
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, String, Integer>, String>() {
                    private transient ListState<Tuple3<String, String, Integer>> historyState;

                    @Override
                    public void open(Configuration parameters) {
                        ListStateDescriptor<Tuple3<String, String, Integer>> descriptor =
                                new ListStateDescriptor<>("action-history", Tuple3.class);
                        historyState = getRuntimeContext().getListState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple3<String, String, Integer> value, Context ctx,
                                              Collector<String> out) throws Exception {
                        historyState.add(value);
                        Iterable<Tuple3<String, String, Integer>> history = historyState.get();

                        StringBuilder sb = new StringBuilder();
                        sb.append("User: ").append(value.f0).append(", History: [");
                        for (Tuple3<String, String, Integer> item : history) {
                            sb.append(item.f1).append("(").append(item.f2).append("), ");
                        }
                        sb.append("]");
                        out.collect(sb.toString());
                    }
                });

        // 3. MapState - 记录每个Action的总金额
        DataStream<String> mapStateStream = sourceStream
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, String, Integer>, String>() {
                    private transient MapState<String, Long> actionSumState;

                    @Override
                    public void open(Configuration parameters) {
                        MapStateDescriptor<String, Long> descriptor =
                                new MapStateDescriptor<>("action-sum", String.class, Long.class);
                        actionSumState = getRuntimeContext().getMapState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple3<String, String, Integer> value, Context ctx,
                                              Collector<String> out) throws Exception {
                        String action = value.f1;
                        Long currentSum = actionSumState.get(action);
                        if (currentSum == null) currentSum = 0L;
                        currentSum += value.f2;
                        actionSumState.put(action, currentSum);

                        StringBuilder sb = new StringBuilder();
                        sb.append("User: ").append(value.f0);
                        sb.append(", Action: ").append(action);
                        sb.append(", Total: ").append(currentSum);
                        out.collect(sb.toString());
                    }
                });

        // 4. ReducingState - 自动累加
        DataStream<String> reducingStateStream = sourceStream
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, String, Integer>, String>() {
                    private transient ReducingState<Integer> totalAmountState;

                    @Override
                    public void open(Configuration parameters) {
                        ReducingStateDescriptor<Integer> descriptor =
                                new ReducingStateDescriptor<>(
                                        "total-amount",
                                        (a, b) -> a + b,  // 累加函数
                                        Integer.class
                                );
                        totalAmountState = getRuntimeContext().getReducingState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple3<String, String, Integer> value, Context ctx,
                                              Collector<String> out) throws Exception {
                        totalAmountState.add(value.f2);
                        out.collect("User: " + value.f0 + ", Total Amount: " + totalAmountState.get());
                    }
                });

        System.out.println("键值状态5种类型:");
        System.out.println("  1. ValueState<T> - 存储单个值");
        System.out.println("  2. ListState<T> - 存储列表");
        System.out.println("  3. MapState<K,V> - 存储键值对");
        System.out.println("  4. ReducingState<T> - 自动聚合");
        System.out.println("  5. AggregatingState<IN,OUT> - 自定义聚合");
    }

    // ==================== 键值状态API设计原理 ====================

    private static void keyedStateAPIDesign(StreamExecutionEnvironment env) {
        System.out.println("\n--- 键值状态API设计原理 ---");

        System.out.println("API设计原则:");
        System.out.println("  1. 状态描述符模式 (StateDescriptor)");
        System.out.println("     - 统一定义状态的名称和类型");
        System.out.println("     - 支持默认值配置");
        System.out.println("     - 支持TTL配置");
        System.out.println();
        System.out.println("  2. 运行时上下文获取 (RuntimeContext)");
        System.out.println("     - 根据Key自动隔离状态");
        System.out.println("     - 自动管理状态生命周期");
        System.out.println();
        System.out.println("  3. 懒加载机制");
        System.out.println("     - 状态在open()方法中初始化");
        System.out.println("     - 首次访问时创建实际状态存储");
        System.out.println();
        System.out.println("  代码示例模式:");
        System.out.println("  ```java");
        System.out.println("  // 1. 定义状态描述符");
        System.out.println("  ValueStateDescriptor<Long> descriptor =");
        System.out.println("      new ValueStateDescriptor<>(\"state-name\", Long.class);");
        System.out.println();
        System.out.println("  // 2. 在open()中获取状态");
        System.out.println("  myState = getRuntimeContext().getState(descriptor);");
        System.out.println();
        System.out.println("  // 3. 在processElement中使用");
        System.out.println("  Long value = myState.value();");
        System.out.println("  myState.update(newValue);");
        System.out.println("  ```");
    }

    // ==================== UnionListState案例场景 ====================

    /**
     * UnionListState特点:
     * - 所有并行实例在恢复时都会获取完整的状态列表
     * - 适用于需要全局数据参与计算的场景
     */
    private static void unionListStateExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- UnionListState案例场景 ---");

        System.out.println("适用场景:");
        System.out.println("  1. 全局配置分发 - 所有算子实例需要完整配置");
        System.out.println("  2. 历史数据回放 - 所有实例需要处理完整历史");
        System.out.println("  3. 分布式协调 - 需要全局信息的场景");
        System.out.println();

        // 示例: 全局黑名单分发
        DataStream<Tuple2<String, Integer>> sourceStream = env.fromElements(
                Tuple2.of("user1", 100),
                Tuple2.of("user2", 200),
                Tuple2.of("blocked_user", 300),
                Tuple2.of("user1", 150)
        );

        // UnionListState使用示例
        DataStream<String> resultStream = sourceStream
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {
                    private transient UnionListState<String> blacklistState;

                    @Override
                    public void open(Configuration parameters) {
                        UnionListStateDescriptor<String> descriptor =
                                new UnionListStateDescriptor<>("global-blacklist", String.class);
                        blacklistState = getRuntimeContext().getUnionListState(descriptor);

                        // 初始化黑名单
                        blacklistState.add("blocked_user");
                        blacklistState.add("spam_user");
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx,
                                              Collector<String> out) throws Exception {
                        String userId = value.f0;
                        boolean isBlocked = false;

                        for (String blocked : blacklistState.get()) {
                            if (userId.equals(blocked)) {
                                isBlocked = true;
                                break;
                            }
                        }

                        if (isBlocked) {
                            out.collect("BLOCKED: " + userId);
                        } else {
                            out.collect("ALLOWED: " + userId + ", Amount: " + value.f1);
                        }
                    }
                });

        System.out.println("ListState vs UnionListState:");
        System.out.println("  ListState: 并行度变化时，各任务分配原状态的子集");
        System.out.println("  UnionListState: 并行度变化时，各任务获取完整状态列表");
    }

    // ==================== 键值状态重分布机制 ====================

    private static void keyedStateRedistribution(StreamExecutionEnvironment env) {
        System.out.println("\n--- 键值状态重分布机制 ---");

        System.out.println("键值状态重分布发生场景:");
        System.out.println("  1. 作业横向扩展 (Scale Up) - 增加并行度");
        System.out.println("  2. 作业升级 - 重启算子");
        System.out.println("  3. 故障恢复 - 从Checkpoint恢复");
        System.out.println("  4. 状态重新分配 - Key分布变化");
        System.out.println();

        System.out.println("重分布策略:");
        System.out.println("  1. Union redistribution (默认)");
        System.out.println("     - 所有并行实例获取所有Key的状态");
        System.out.println("     - 适用于: 广播状态、全局配置");
        System.out.println();
        System.out.println("  2. Split redistribution");
        System.out.println("     - 状态按Key分组分配到不同实例");
        System.out.println("     - 适用于: 键值状态的普通场景");
        System.out.println();
        System.out.println("  3. Broadcast redistribution");
        System.out.println("     - 所有状态复制到所有实例");
        System.out.println("     - 适用于: 小体积全局状态");
        System.out.println();

        System.out.println("状态后端选择建议:");
        System.out.println("  - HashMapStateBackend: 状态小、性能高、内存消耗大");
        System.out.println("  - EmbeddedRocksDBStateBackend: 状态大、持久化、磁盘存储");
    }

    // ==================== 广播状态使用方法 ====================

    /**
     * 广播状态特点:
     * 1. 算子级别状态，所有并行实例共享相同数据
     * 2. 适用于小体积、频繁更新的配置数据
     * 3. 典型应用: 动态规则更新、实时配置下发
     */
    private static void broadcastStateExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- 广播状态示例 ---");

        // 业务数据流
        DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(
                Tuple2.of("rule1", 100),
                Tuple2.of("rule2", 200),
                Tuple2.of("rule1", 150),
                Tuple2.of("rule3", 300)
        );

        // 规则配置流 (动态更新)
        DataStream<Map<String, Integer>> ruleStream = env.fromElements(
                Map.of("rule1", 50, "rule2", 100),
                Map.of("rule1", 80, "rule2", 150, "rule3", 200)  // 规则更新
        );

        // 使用广播状态处理
        DataStream<String> resultStream = dataStream
                .connect(ruleStream.broadcast())
                .process(new BroadcastProcessFunction<
                        Tuple2<String, Integer>,  // 输入数据类型
                        Map<String, Integer>,    // 广播数据类型
                        String>() {

                    // 广播状态描述符
                    private transient MapState<String, Integer> broadcastState;

                    @Override
                    public void open(Configuration parameters) {
                        MapStateDescriptor<String, Integer> descriptor =
                                new MapStateDescriptor<>(
                                        "rules-broadcast-state",
                                        String.class,
                                        Integer.class
                                );
                        broadcastState = getRuntimeContext().getBroadcastState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, ReadOnlyContext ctx,
                                              Collector<String> out) throws Exception {
                        // 读取广播状态
                        ReadOnlyBroadcastState<String, Integer> state = ctx.getBroadcastState(
                                new MapStateDescriptor<>("rules-broadcast-state", String.class, Integer.class)
                        );

                        Integer threshold = state.get(value.f0);
                        int actualValue = value.f1;

                        if (threshold != null) {
                            if (actualValue >= threshold) {
                                out.collect("ALERT: " + value.f0 + " = " + actualValue +
                                           " (threshold: " + threshold + ")");
                            } else {
                                out.collect("NORMAL: " + value.f0 + " = " + actualValue);
                            }
                        } else {
                            out.collect("NO_RULE: " + value.f0 + " = " + actualValue);
                        }
                    }

                    @Override
                    public void processBroadcastElement(Map<String, Integer> value, Context ctx,
                                                        Collector<String> out) throws Exception {
                        // 更新广播状态
                        for (Map.Entry<String, Integer> entry : value.entrySet()) {
                            ctx.getBroadcastState(
                                    new MapStateDescriptor<>("rules-broadcast-state", String.class, Integer.class)
                            ).put(entry.getKey(), entry.getValue());
                        }
                        System.out.println("Broadcast state updated: " + value);
                    }
                });

        System.out.println("广播状态使用场景:");
        System.out.println("  1. 动态规则引擎 - 实时更新业务规则");
        System.out.println("  2. 配置中心 - 配置热更新");
        System.out.println("  3. 白名单/黑名单 - 实时维护用户列表");
        System.out.println("  4. 字典表 - 小体积维度表关联");
    }

    // ==================== 状态TTL配置详解 ====================

    /**
     * 状态TTL (Time To Live) 配置选项:
     * 1. TTL刷新策略:
     *    - TTL刷新策略 - 处理时间 vs 事件时间
     *    - OnCreateAndWrite: 创建和写入时刷新
     *    - OnReadAndWrite: 读取和写入时都刷新
     *
     * 2. 状态可见性:
     *    - ReturnExpiredIfNotCleanedUp: 过期后返回null还是返回值
     *
     * 3. 清理策略:
     *    - FullSnapshotScanPolicy: 全量扫描
     *    - IncrementalCleanupSize: 按数量增量清理
     *    - TimeBasedCleanupStrategy: 基于时间清理
     */
    private static void stateTTLExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- 状态TTL配置详解 ---");

        DataStream<Tuple2<String, Integer>> sourceStream = env.fromElements(
                Tuple2.of("user1", 100),
                Tuple2.of("user2", 200),
                Tuple2.of("user1", 150),
                Tuple2.of("user3", 300)
        );

        // 配置状态TTL
        StateTtlConfig ttlConfig = StateTtlConfig
                // 1. 设置TTL时间 (10秒)
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // 2. 设置过期策略
                .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                // 3. 设置清理策略
                .cleanupFullSnapshot()
                .cleanupIncrementally(100, true)  // 每100条清理一次
                .build();

        DataStream<String> resultStream = sourceStream
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {
                    private transient ValueState<Integer> sumState;

                    @Override
                    public void open(Configuration parameters) {
                        ValueStateDescriptor<Integer> descriptor =
                                new ValueStateDescriptor<>("user-sum", Integer.class);
                        // 启用TTL
                        descriptor.enableTimeToLive(ttlConfig);
                        sumState = getRuntimeContext().getState(descriptor);
                    }

                    @Override
                    public void processElement(Tuple2<String, Integer> value, Context ctx,
                                              Collector<String> out) throws Exception {
                        Integer currentSum = sumState.value();
                        if (currentSum == null) currentSum = 0;
                        currentSum += value.f1;
                        sumState.update(currentSum);
                        out.collect("User: " + value.f0 + ", Sum: " + currentSum);
                    }
                });

        System.out.println("状态TTL配置项:");
        System.out.println("  1. setUpdateType:");
        System.out.println("     - OnCreateAndWrite: 创建和写入时刷新TTL");
        System.out.println("     - OnReadAndWrite: 读取和写入都刷新TTL");
        System.out.println();
        System.out.println("  2. setStateVisibility:");
        System.out.println("     - NeverReturnExpired: 过期值不可见");
        System.out.println("     - ReturnExpiredIfNotCleanedUp: 过期值仍可读取直到清理");
        System.out.println();
        System.out.println("  3. cleanup策略:");
        System.out.println("     - cleanupFullSnapshot(): Checkpoint时全量清理");
        System.out.println("     - cleanupIncrementally(size, recursive): 按数量增量清理");
        System.out.println("     - cleanupInRocksdbCompactStrategy(1000): RocksDB压缩时清理");
    }

    // ==================== 有状态计算案例 ====================

    /**
     * 实时指标计算案例 - 用户行为分析
     * 场景: 实时统计每个用户的:
     * 1. 最近N分钟的活跃度
     * 2. 行为序列
     * 3. 异常检测
     */
    private static void statefulComputationCase(StreamExecutionEnvironment env) {
        System.out.println("\n--- 有状态计算综合案例: 用户行为分析 ---");

        // 模拟用户行为数据
        DataStream<Tuple4<String, String, Long, String>> behaviorStream = env.fromElements(
                Tuple4.of("user1", "login", 1000L, "192.168.1.1"),
                Tuple4.of("user1", "browse", 2000L, "/home"),
                Tuple4.of("user2", "login", 1500L, "192.168.1.2"),
                Tuple4.of("user1", "purchase", 5000L, "/product/123"),
                Tuple4.of("user2", "browse", 3000L, "/home"),
                Tuple4.of("user1", "logout", 6000L, "192.168.1.1"),
                Tuple4.of("user3", "login", 4000L, "192.168.1.3")
        );

        // 定义迟到数据侧输出流
        final OutputTag<String> lateDataTag = new OutputTag<String>("late-data") {};

        // 用户行为分析
        DataStream<String> analysisResult = behaviorStream
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String,
                        Tuple4<String, String, Long, String>, String>() {

                    // 状态: 记录用户行为历史
                    private transient ListState<Tuple4<String, String, Long, String>> behaviorHistory;
                    // 状态: 记录登录状态
                    private transient ValueState<Boolean> isLoggedIn;
                    // 状态: 登录时间
                    private transient ValueState<Long> loginTime;

                    @Override
                    public void open(Configuration parameters) {
                        behaviorHistory = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("behavior-history", Tuple4.class)
                        );
                        isLoggedIn = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("logged-in", Boolean.class)
                        );
                        loginTime = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("login-time", Long.class)
                        );
                    }

                    @Override
                    public void processElement(Tuple4<String, String, Long, String> value, Context ctx,
                                              Collector<String> out) throws Exception {
                        String userId = value.f0;
                        String action = value.f1;
                        Long timestamp = value.f2;

                        // 检查迟到数据
                        long currentWatermark = ctx.timerService().currentWatermark();
                        if (timestamp < currentWatermark - 60000) {
                            ctx.output(lateDataTag, "Late data: " + value);
                            return;
                        }

                        // 更新行为历史
                        behaviorHistory.add(value);

                        // 状态机逻辑
                        Boolean loggedIn = isLoggedIn.value();
                        Long lastLoginTime = loginTime.value();

                        if ("login".equals(action)) {
                            isLoggedIn.update(true);
                            loginTime.update(timestamp);
                            out.collect("User " + userId + " logged in at " + timestamp);
                        } else if ("logout".equals(action)) {
                            isLoggedIn.update(false);
                            out.collect("User " + userId + " logged out. Session duration: " +
                                       (timestamp - lastLoginTime) + "ms");
                        } else if (loggedIn != null && loggedIn) {
                            out.collect("User " + userId + " performed: " + action);

                            // 注册5秒后的定时器
                            ctx.timerService().registerProcessingTimeTimer(
                                    ctx.timerService().currentProcessingTime() + 5000
                            );
                        }
                    }

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) {
                        // 定时器触发逻辑
                        out.collect("Timer triggered for user session check");
                    }
                });

        // 获取迟到数据
        DataStream<String> lateDataStream = analysisResult.getSideOutput(lateDataTag);

        System.out.println("有状态计算应用场景:");
        System.out.println("  1. 实时指标统计 - PV/UV/GMV");
        System.out.println("  2. 状态机引擎 - 订单状态流转");
        System.out.println("  3. 复杂事件处理 - CEP");
        System.out.println("  4. 实时去重 - UV统计");
        System.out.println("  5. 窗口聚合 - 实时报表");
    }

    // ==================== 时间语义和时间窗口总结 ====================

    /**
     * Flink时间语义:
     * 1. Processing Time (处理时间) - 数据被处理的时间
     *    - 优点: 无需水位线，无延迟
     *    - 缺点: 结果不确定，依赖处理速度
     *
     * 2. Event Time (事件时间) - 数据产生的时间
     *    - 优点: 结果确定，可重放
     *    - 缺点: 需要水位线处理乱序
     *
     * 3. Ingestion Time (摄入时间) - 数据进入Flink的时间
     *    - 介于两者之间
     */
    private static void timeSemanticsSummary(StreamExecutionEnvironment env) {
        System.out.println("\n--- 时间语义详解 ---");

        System.out.println("三种时间语义:");
        System.out.println("  1. Processing Time (处理时间)");
        System.out.println("     - 数据被算子处理的系统时间");
        System.out.println("     - 无需水位线，延迟最低");
        System.out.println("     - 结果受处理速度影响");
        System.out.println();
        System.out.println("  2. Event Time (事件时间)");
        System.out.println("     - 数据本身携带的时间戳");
        System.out.println("     - 结果确定性高，可重放");
        System.out.println("     - 需要水位线处理乱序和迟到数据");
        System.out.println();
        System.out.println("  3. Ingestion Time (摄入时间)");
        System.out.println("     - 数据进入Flink Source的时间");
        System.out.println("     - 自动分配时间戳和生成水位线");
        System.out.println();

        // 设置时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 定义水位线策略
        WatermarkStrategy<Tuple2<String, Long>> watermarkStrategy =
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, timestamp) -> event.f1);

        // 窗口类型总结
        System.out.println("窗口类型:");
        System.out.println("  1. TimeWindow - 时间窗口");
        System.out.println("     - TumblingEventTimeWindow: 滚动窗口");
        System.out.println("     - SlidingEventTimeWindow: 滑动窗口");
        System.out.println("     - EventTimeSessionWindow: 会话窗口");
        System.out.println();
        System.out.println("  2. CountWindow - 计数窗口");
        System.out.println("     - GlobalWindow: 全局窗口(需配合Trigger)");
        System.out.println();
        System.out.println("  3. Custom Window - 自定义窗口");
    }

    // ==================== Watermark详解 ====================

    private static void watermarkExamples(StreamExecutionEnvironment env) {
        System.out.println("\n--- Watermark详解 ---");

        // 1. 周期性水位线生成
        System.out.println("周期性水位线生成策略:");
        System.out.println("  - AssignerWithPeriodicWatermarks: 周期生成(默认200ms)");
        System.out.println("  - PunctuatedWatermarkStrategy: 事件驱动生成");
        System.out.println();

        // 2. 水位线策略
        System.out.println("水位线策略:");
        System.out.println("  1. forBoundedOutOfOrderness: 最大乱序时间");
        System.out.println("  2. forMonotonousTimestamps: 单调递增(无乱序)");
        System.out.println("  3. NoWatermarksStrategy: 不生成水位线");
        System.out.println();

        // 3. 自定义水位线分配器
        DataStream<Tuple2<String, Long>> sourceStream = env.fromElements(
                Tuple2.of("event1", 1000L),
                Tuple2.of("event2", 2500L),
                Tuple2.of("event3", 3000L),
                Tuple2.of("event4", 1500L)  // 乱序数据
        );

        // 使用水位线策略
        WatermarkStrategy<Tuple2<String, Long>> wmStrategy =
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, ts) -> event.f1);

        DataStream<Tuple2<String, Long>> withWmStream = sourceStream
                .assignTimestampsAndWatermarks(wmStrategy);

        System.out.println("Watermark问题排查:");
        System.out.println("  1. 水位线不对齐问题");
        System.out.println("     - 原因: 多分区数据到达速度不一致");
        System.out.println("     - 解决: 调整水位线策略，使用旁路输出");
        System.out.println();
        System.out.println("  2. 水位线分配太少问题");
        System.out.println("     - 原因: 数据间隔时间大于生成周期");
        System.out.println("     - 解决: 调整生成周期或使用事件驱动策略");
        System.out.println();
        System.out.println("  3. 窗口不触发问题");
        System.out.println("     - 原因: 水位线未推进、水位线策略错误");
        System.out.println("     - 解决: 检查时间语义、检查水位线策略");
    }

    // ==================== 数据乱序处理方案 ====================

    /**
     * 数据乱序处理三种方法:
     * 1. Watermark - 最大乱序时间容忍度
     * 2. AllowLateness - 允许迟到数据再次触发窗口
     * 3. SideOutput - 将迟到数据输出到旁路
     */
    private static void outOfOrderDataHandling(StreamExecutionEnvironment env) {
        System.out.println("\n--- 数据乱序处理方案 ---");

        System.out.println("数据乱序问题原因:");
        System.out.println("  1. 网络延迟");
        System.out.println("  2. 数据源处理时间差异");
        System.out.println("  3. 多分区数据乱序");
        System.out.println();

        DataStream<Tuple2<String, Long>> sourceStream = env.fromElements(
                Tuple2.of("key1", 1000L),
                Tuple2.of("key1", 3000L),
                Tuple2.of("key1", 2000L),  // 乱序: 应该比3000早
                Tuple2.of("key1", 4000L)
        );

        // 水位线策略 - 容忍5秒乱序
        WatermarkStrategy<Tuple2<String, Long>> wmStrategy =
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((event, ts) -> event.f1);

        DataStream<String> windowResult = sourceStream
                .assignTimestampsAndWatermarks(wmStrategy)
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new WindowFunction<Tuple2<String, Long>, String, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window,
                                      Iterable<Tuple2<String, Long>> input,
                                      Collector<String> out) {
                        StringBuilder sb = new StringBuilder();
                        sb.append("Window: ").append(window)
                          .append(", Count: ").append(input.spliterator().estimateSize());
                        for (Tuple2<String, Long> item : input) {
                            sb.append(", EventTime: ").append(item.f1);
                        }
                        out.collect(sb.toString());
                    }
                });

        System.out.println("三种乱序处理方法:");
        System.out.println("  1. Watermark: 设置最大乱序时间");
        System.out.println("  2. AllowLateness: 窗口触发后继续等待迟到数据");
        System.out.println("  3. SideOutput: 将迟到数据输出到旁路流");
        System.out.println();
        System.out.println("Watermark触发窗口条件:");
        System.out.println("  watermark >= window_end_time");
        System.out.println();
        System.out.println("windowAll()数据倾斜问题:");
        System.out.println("  - 原因: 单并行度处理所有数据");
        System.out.println("  - 解决: 重分区、使用GlobalWindow + Shuffle");
    }

    // ==================== 迟到数据处理方案 ====================

    private static void lateDataSolutions(StreamExecutionEnvironment env) {
        System.out.println("\n--- 迟到数据处理方案 ---");

        // 定义迟到数据侧输出流
        final OutputTag<String> lateDataTag = new OutputTag<String>("late-data") {};

        DataStream<Tuple2<String, Long>> sourceStream = env.fromElements(
                Tuple2.of("key1", 1000L),
                Tuple2.of("key1", 2000L),
                Tuple2.of("key1", 5000L),  // 正常时间
                Tuple2.of("key1", 1500L),  // 迟到数据
                Tuple2.of("key1", 2500L)   // 迟到数据
        );

        // 水位线策略: 容忍3秒乱序
        WatermarkStrategy<Tuple2<String, Long>> wmStrategy =
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner((event, ts) -> event.f1);

        // 1. 允许迟到机制
        // 2. 侧输出流收集迟到数据
        SingleOutputStreamOperator<String> windowResult = sourceStream
                .assignTimestampsAndWatermarks(wmStrategy)
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(5)))
                // 允许迟到5秒
                .allowedLateness(Duration.ofSeconds(5))
                // 侧输出流收集迟到超过5秒的数据
                .sideOutputLateData(lateDataTag)
                .aggregate(new AggregateFunction<Tuple2<String, Long>, String, String>() {
                    @Override
                    public String createAccumulator() {
                        return "";
                    }

                    @Override
                    public String add(Tuple2<String, Long> value, String accumulator) {
                        if (accumulator.isEmpty()) return String.valueOf(value.f1);
                        return accumulator + "," + value.f1;
                    }

                    @Override
                    public String getResult(String accumulator) {
                        return "Events: [" + accumulator + "]";
                    }

                    @Override
                    public String merge(String a, String b) {
                        return a + "," + b;
                    }
                });

        // 获取侧输出流
        DataStream<String> lateDataStream = windowResult.getSideOutput(lateDataTag);

        System.out.println("允许迟到(AllowLateness)机制:");
        System.out.println("  - 窗口触发后，继续保持状态等待迟到数据");
        System.out.println("  - 迟到数据会再次触发窗口计算");
        System.out.println("  - 超过允许迟到时间后，窗口状态被清除");
        System.out.println();
        System.out.println("侧输出流(SideOutput)机制:");
        System.out.println("  - 将迟到数据输出到独立的流");
        System.out.println("  - 可用于数据修复或追查");
        System.out.println("  - 不会影响主窗口计算结果");
        System.out.println();
        System.out.println("使用模式:");
        System.out.println("  1. 定义OutputTag");
        System.out.println("  2. 使用sideOutputLateData()收集超过允许迟到时间的数据");
        System.out.println("  3. 使用getSideOutput()获取侧输出流");
    }

    // ==================== 计数窗口 ====================

    /**
     * CountWindow:
     * - 基于数据数量的窗口
     * - 不依赖时间语义
     * - 分为滚动和滑动两种
     */
    private static void countWindowExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- 计数窗口示例 ---");

        DataStream<Tuple2<String, Integer>> sourceStream = env.fromElements(
                Tuple2.of("key1", 10),
                Tuple2.of("key1", 20),
                Tuple2.of("key1", 30),
                Tuple2.of("key1", 40),
                Tuple2.of("key1", 50),
                Tuple2.of("key1", 60),
                Tuple2.of("key1", 70)
        );

        // 滚动计数窗口 (每3条触发一次)
        DataStream<Integer> tumblingCountWindow = sourceStream
                .keyBy(t -> t.f0)
                .countWindow(3)
                .sum(1);

        // 滑动计数窗口 (窗口大小3，滑动步长2)
        DataStream<Integer> slidingCountWindow = sourceStream
                .keyBy(t -> t.f0)
                .countWindow(3, 2)
                .sum(1);

        System.out.println("计数窗口类型:");
        System.out.println("  1. countWindow(size) - 滚动计数窗口");
        System.out.println("     - 每收到size条数据触发一次计算");
        System.out.println("     - 窗口之间不重叠");
        System.out.println();
        System.out.println("  2. countWindow(size, slide) - 滑动计数窗口");
        System.out.println("     - 每收到slide条数据触发一次");
        System.out.println("     - 窗口之间有重叠");
        System.out.println();
        System.out.println("注意: CountWindow使用Processing Time，");
        System.out.println("      数据乱序可能导致窗口触发延迟");
    }

    // ==================== 辅助类: 元组类 (如果没有) ====================
}
