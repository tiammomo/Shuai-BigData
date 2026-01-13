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
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.evictors.Evictor;
import org.apache.flink.streaming.api.windowing.functions.WindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.*;

/**
 * Flink DataStream API Examples - 状态计算、窗口计算、多流Join详解
 */
public class FlinkDataStreamExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        env.setBufferTimeout(100);

        // 开启状态后端检查点
        // env.enableCheckpointing(60000);
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());

        // ==================== 1. 基础DataStream创建 ====================

        DataStream<String> stream1 = env.fromElements("a", "b", "c");
        DataStream<Long> stream2 = env.fromElements(1L, 2L, 3L);

        // ==================== 2. 状态计算示例 ====================

        // 2.1 Keyed State - ValueState (记录上次值)
        keyedValueStateExample(env);

        // 2.2 Keyed State - ListState (记录历史列表)
        keyedListStateExample(env);

        // 2.3 Keyed State - MapState (记录映射关系)
        keyedMapStateExample(env);

        // 2.4 Keyed State - ReducingState (自动聚合)
        keyedReducingStateExample(env);

        // 2.5 Keyed State - AggregatingState (聚合状态)
        keyedAggregatingStateExample(env);

        // 2.6 Operator State - BroadcastState (广播状态)
        broadcastStateExample(env);

        // ==================== 3. 窗口计算示例 ====================

        // 3.1 滚动窗口 (Tumbling Window)
        tumblingWindowExample(env);

        // 3.2 滑动窗口 (Sliding Window)
        slidingWindowExample(env);

        // 3.3 会话窗口 (Session Window)
        sessionWindowExample(env);

        // 3.4 全局窗口 (Global Window) + 触发器
        globalWindowExample(env);

        // 3.5 窗口函数 - 增量聚合 vs 全量
        windowFunctionExample(env);

        // 3.6 迟到数据处理 - 允许迟到 + 侧输出流
        lateDataHandlingExample(env);

        // 3.7 多键分组窗口
        multiKeyWindowExample(env);

        // 3.8 窗口 Join
        windowJoinExample(env);

        // ==================== 4. 多流 Join 示例 ====================

        // 4.1 Window Join (基于窗口的Join)
        windowJoinFullExample(env);

        // 4.2 Interval Join (基于时间区间的Join)
        intervalJoinExample(env);

        // 4.3 两条流 Connect + CoProcessFunction
        connectCoProcessExample(env);

        // 4.4 多条流 Connect
        multiStreamConnectExample(env);

        // 4.5 双流 Union (合流)
        streamUnionExample(env);

        // 4.6 实时对账系统 (多流Join综合案例)
        reconciliationSystemExample(env);

        env.execute("Flink State, Window, Join Examples");
    }

    // ==================== 状态计算示例 ====================

    /**
     * 2.1 Keyed ValueState 示例 - 记录每个key的上次值
     */
    private static void keyedValueStateExample(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("user1", 100),
                Tuple2.of("user1", 200),
                Tuple2.of("user2", 150),
                Tuple2.of("user1", 300)
        );

        // 使用KeyedProcessFunction处理
        DataStream<String> result = input
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {

                    // 定义ValueState
                    private ValueState<Integer> lastValueState;

                    @Override
                    public void open(Configuration parameters) {
                        lastValueState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("lastValue", Integer.class)
                        );
                    }

                    @Override
                    public void processElement(
                            Tuple2<String, Integer> value,
                            Context ctx,
                            Collector<String> out) throws Exception {

                        Integer lastValue = lastValueState.value();

                        if (lastValue == null) {
                            out.collect(value.f0 + " 首次出现, value=" + value.f1);
                        } else {
                            int change = value.f1 - lastValue;
                            out.collect(value.f0 + " 上次值=" + lastValue +
                                    ", 当前值=" + value.f1 + ", 变化=" + change);
                        }

                        // 更新状态
                        lastValueState.update(value.f1);
                    }
                });

        result.print("ValueState");
    }

    /**
     * 2.2 Keyed ListState 示例 - 记录每个key的历史记录
     */
    private static void keyedListStateExample(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, String>> input = env.fromElements(
                Tuple2.of("order", "order-001"),
                Tuple2.of("order", "order-002"),
                Tuple2.of("payment", "pay-001"),
                Tuple2.of("order", "order-003")
        );

        DataStream<String> result = input
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, String>, String>() {

                    private ListState<String> historyState;

                    @Override
                    public void open(Configuration parameters) {
                        historyState = getRuntimeContext().getListState(
                                new ListStateDescriptor<>("history", String.class)
                        );
                    }

                    @Override
                    public void processElement(
                            Tuple2<String, String> value,
                            Context ctx,
                            Collector<String> out) throws Exception {

                        // 添加到历史列表
                        historyState.add(value.f1);

                        // 收集当前key的所有历史记录
                        Iterable<String> history = historyState.get();
                        List<String> list = new ArrayList<>();
                        history.forEach(list::add);

                        out.collect("Key=" + value.f0 + ", History=" + list);
                    }
                });

        result.print("ListState");
    }

    /**
     * 2.3 Keyed MapState 示例 - 记录每个key的字段映射
     */
    private static void keyedMapStateExample(StreamExecutionEnvironment env) {
        // 模拟用户属性变更流
        DataStream<Tuple3<String, String, String>> input = env.fromElements(
                Tuple3.of("user1", "name", "Alice"),
                Tuple3.of("user1", "age", "25"),
                Tuple3.of("user1", "city", "Beijing"),
                Tuple3.of("user2", "name", "Bob"),
                Tuple3.of("user2", "age", "30")
        );

        DataStream<String> result = input
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple3<String, String, String>, String>() {

                    private MapState<String, String> userPropsState;

                    @Override
                    public void open(Configuration parameters) {
                        userPropsState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("userProps", String.class, String.class)
                        );
                    }

                    @Override
                    public void processElement(
                            Tuple3<String, String, String> value,
                            Context ctx,
                            Collector<String> out) throws Exception {

                        // 更新属性
                        userPropsState.put(value.f1, value.f2);

                        // 收集当前用户所有属性
                        Map<String, String> props = new HashMap<>();
                        userPropsState.iterator().forEachRemaining(e -> props.put(e.getKey(), e.getValue()));

                        out.collect("User=" + value.f0 + ", Props=" + props);
                    }
                });

        result.print("MapState");
    }

    /**
     * 2.4 Keyed ReducingState 示例 - 自动聚合
     */
    private static void keyedReducingStateExample(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, Long>> input = env.fromElements(
                Tuple2.of("click", 1L),
                Tuple2.of("click", 1L),
                Tuple2.of("view", 1L),
                Tuple2.of("click", 1L)
        );

        DataStream<Tuple2<String, Long>> result = input
                .keyBy(t -> t.f0)
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(
                            Tuple2<String, Long> v1,
                            Tuple2<String, Long> v2) throws Exception {
                        return Tuple2.of(v1.f0, v1.f1 + v2.f1);
                    }
                });

        result.print("ReducingState");
    }

    /**
     * 2.5 Keyed AggregatingState 示例 - 窗口聚合状态
     */
    private static void keyedAggregatingStateExample(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("score", 85),
                Tuple2.of("score", 90),
                Tuple2.of("score", 78),
                Tuple2.of("score", 92)
        );

        DataStream<String> result = input
                .keyBy(t -> t.f0)
                .process(new KeyedProcessFunction<String, Tuple2<String, Integer>, String>() {

                    private AggregatingState<Integer, Double> avgState;

                    @Override
                    public void open(Configuration parameters) {
                        avgState = getRuntimeContext().getAggregatingState(
                                new AggregatingStateDescriptor<>(
                                        "avgState",
                                        new MyAggregate(),
                                        new org.apache.flink.api.common.typeinfo.TypeInformation<Double>() {}
                                )
                        );
                    }

                    @Override
                    public void processElement(
                            Tuple2<String, Integer> value,
                            Context ctx,
                            Collector<String> out) throws Exception {

                        avgState.add(value.f1);
                        Double avg = avgState.get();
                        out.collect("Current avg score: " + String.format("%.2f", avg));
                    }
                });

        result.print("AggregatingState");
    }

    /**
     * 2.6 BroadcastState 示例 - 规则/配置广播
     */
    private static void broadcastStateExample(StreamExecutionEnvironment env) {
        // 数据流
        DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(
                Tuple2.of("order", 100),
                Tuple2.of("order", 200),
                Tuple2.of("payment", 50)
        );

        // 规则流 - 假设规则动态更新
        DataStream<Map<String, Object>> ruleStream = env.fromElements(
                new HashMap<String, Object>() {{
                    put("type", "order");
                    put("multiplier", 2.0);
                }},
                new HashMap<String, Object>() {{
                    put("type", "payment");
                    put("multiplier", 1.5);
                }}
        );

        // 创建广播状态描述符
        MapStateDescriptor<String, Map<String, Object>> ruleStateDescriptor =
                new MapStateDescriptor<>("rules", String.class, Map.class);

        // 广播规则流
        BroadcastStream<Map<String, Object>> broadcastStream = ruleStream.broadcast(ruleStateDescriptor);

        // 连接数据流和广播流
        DataStream<String> result = dataStream
                .connect(broadcastStream)
                .process(new CoProcessFunction<Tuple2<String, Integer>, Map<String, Object>, String>() {

                    private MapState<String, Map<String, Object>> ruleState;

                    @Override
                    public void open(Configuration parameters) {
                        ruleState = getRuntimeContext().getMapState(ruleStateDescriptor);
                    }

                    @Override
                    public void processElement(
                            Tuple2<String, Integer> value,
                            ReadOnlyContext ctx,
                            Collector<String> out) throws Exception {

                        // 读取广播状态
                        Map<String, Object> rule = ruleState.get(value.f0);
                        if (rule != null) {
                            double multiplier = (double) rule.get("multiplier");
                            double result = value.f1 * multiplier;
                            out.collect("Type=" + value.f0 + ", Value=" + value.f1 +
                                    ", Multiplier=" + multiplier + ", Result=" + result);
                        } else {
                            out.collect("Type=" + value.f0 + ", Value=" + value.f1 + ", No rule found");
                        }
                    }

                    @Override
                    public void processElement1(
                            Map<String, Object> rule,
                            Context ctx,
                            Collector<String> out) throws Exception {

                        // 更新广播状态
                        String type = (String) rule.get("type");
                        ruleState.put(type, rule);
                        out.collect("Rule updated: type=" + type);
                    }
                });

        result.print("BroadcastState");
    }

    // ==================== 窗口计算示例 ====================

    /**
     * 3.1 滚动窗口示例 - 每10秒统计一次
     */
    private static void tumblingWindowExample(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("b", 3),
                Tuple2.of("a", 4),
                Tuple2.of("b", 5)
        );

        // 处理时间滚动窗口
        DataStream<Tuple2<String, Integer>> result = input
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .sum(1);

        // 事件时间滚动窗口
        DataStream<Tuple2<String, Integer>> eventTimeResult = input
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((t, ts) -> System.currentTimeMillis())
                )
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .sum(1);

        result.print("TumblingWindow");
    }

    /**
     * 3.2 滑动窗口示例 - 窗口大小10秒，滑动步长5秒
     */
    private static void slidingWindowExample(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("a", 3),
                Tuple2.of("a", 4)
        );

        // 滑动处理时间窗口
        DataStream<Tuple2<String, Integer>> result = input
                .keyBy(t -> t.f0)
                .window(SlidingProcessingTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .sum(1);

        // 滑动事件时间窗口
        DataStream<Tuple2<String, Integer>> eventTimeResult = input
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                )
                .keyBy(t -> t.f0)
                .window(SlidingEventTimeWindows.of(Duration.ofSeconds(10), Duration.ofSeconds(5)))
                .sum(1);

        result.print("SlidingWindow");
    }

    /**
     * 3.3 会话窗口示例 - 间隔5秒无数据则关闭窗口
     */
    private static void sessionWindowExample(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),  // 2秒后，同一会话
                Tuple2.of("b", 3),
                Tuple2.of("a", 4)   // 3秒后，同一会话
        );

        // 处理时间会话窗口
        DataStream<String> result = input
                .keyBy(t -> t.f0)
                .window(ProcessingTimeSessionWindows.withGap(Duration.ofSeconds(5)))
                .apply(new WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void apply(
                            String key,
                            TimeWindow window,
                            Iterable<Tuple2<String, Integer>> input,
                            Collector<String> out) {

                        int sum = 0;
                        for (Tuple2<String, Integer> t : input) {
                            sum += t.f1;
                        }
                        out.collect("Key=" + key + ", Window=[" + window.getStart() + "," + window.getEnd() +
                                "), Sum=" + sum);
                    }
                });

        result.print("SessionWindow");
    }

    /**
     * 3.4 全局窗口 + 自定义触发器示例
     */
    private static void globalWindowExample(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("a", 3),
                Tuple2.of("b", 4)
        );

        DataStream<String> result = input
                .keyBy(t -> t.f0)
                .window(GlobalWindows.create())
                // 自定义触发器：每3个元素触发一次
                .trigger(new org.apache.flink.streaming.api.windowing.triggers.Trigger<Tuple2<String, Integer>, GlobalWindow>() {
                    @Override
                    public TriggerResult onElement(
                            Tuple2<String, Integer> element,
                            long timestamp,
                            GlobalWindow window,
                            TriggerContext ctx) throws Exception {

                        // 计数器状态
                        ValueState<Integer> countState = ctx.getPartitionedState(
                                new ValueStateDescriptor<>("count", Integer.class)
                        );

                        Integer count = countState.value();
                        if (count == null) count = 0;
                        count++;
                        countState.update(count);

                        if (count >= 3) {
                            countState.clear();
                            return TriggerResult.FIRE;
                        }
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(
                            long timestamp,
                            GlobalWindow window,
                            TriggerContext ctx) {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(
                            long timestamp,
                            GlobalWindow window,
                            TriggerContext ctx) {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(
                            GlobalWindow window,
                            TriggerContext ctx) throws Exception {
                        ctx.getPartitionedState(
                                new ValueStateDescriptor<>("count", Integer.class)
                        ).clear();
                    }
                })
                .apply(new WindowFunction<Tuple2<String, Integer>, String, String, GlobalWindow>() {
                    @Override
                    public void apply(
                            String key,
                            GlobalWindow window,
                            Iterable<Tuple2<String, Integer>> input,
                            Collector<String> out) {

                        int sum = 0;
                        List<Integer> values = new ArrayList<>();
                        for (Tuple2<String, Integer> t : input) {
                            sum += t.f1;
                            values.add(t.f1);
                        }
                        out.collect("Key=" + key + ", Values=" + values + ", Sum=" + sum);
                    }
                });

        result.print("GlobalWindow");
    }

    /**
     * 3.5 窗口函数示例 - 增量聚合 vs 全量窗口函数
     */
    private static void windowFunctionExample(StreamExecutionEnvironment env) {
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("order", 100),
                Tuple2.of("order", 200),
                Tuple2.of("order", 150),
                Tuple2.of("order", 300)
        );

        // 方式1: 增量聚合 (ReduceFunction)
        DataStream<Tuple2<String, Integer>> reduceResult = input
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .reduce(
                        (v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1)
                );

        // 方式2: 全量窗口函数 (WindowFunction)
        DataStream<String> windowResult = input
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new WindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    @Override
                    public void apply(
                            String key,
                            TimeWindow window,
                            Iterable<Tuple2<String, Integer>> input,
                            Collector<String> out) {

                        int count = 0;
                        int sum = 0;
                        int max = Integer.MIN_VALUE;
                        int min = Integer.MAX_VALUE;

                        for (Tuple2<String, Integer> t : input) {
                            count++;
                            sum += t.f1;
                            max = Math.max(max, t.f1);
                            min = Math.min(min, t.f1);
                        }

                        double avg = (double) sum / count;
                        out.collect("Key=" + key + ", Count=" + count +
                                ", Sum=" + sum + ", Avg=" + String.format("%.2f", avg) +
                                ", Max=" + max + ", Min=" + min);
                    }
                });

        // 方式3: 增量 + 全量结合 (AggregateFunction)
        DataStream<String> aggregateResult = input
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .aggregate(new MyAggregateFunction());

        reduceResult.print("Reduce");
        windowResult.print("WindowFunction");
        aggregateResult.print("Aggregate");
    }

    /**
     * 3.6 迟到数据处理 - 允许迟到 + 侧输出流
     */
    private static void lateDataHandlingExample(StreamExecutionEnvironment env) {
        // 定义侧输出流标签
        OutputTag<Tuple2<String, Integer>> lateOutputTag =
                new OutputTag<Tuple2<String, Integer>>("late-data") {};

        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("a", 1),
                Tuple2.of("a", 2),
                Tuple2.of("a", 3)
        );

        DataStream<Tuple2<String, Integer>> result = input
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple2<String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                                .withTimestampAssigner((t, ts) -> System.currentTimeMillis())
                )
                .keyBy(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Duration.ofSeconds(10)))
                .allowedLateness(Duration.ofSeconds(30))  // 允许迟到30秒
                .sideOutputLateData(lateOutputTag)        // 超过允许迟到范围的数据进入侧输出流
                .sum(1);

        // 获取侧输出流处理迟到数据
        DataStream<Tuple2<String, Integer>> lateData = result
                .getSideOutput(lateOutputTag);

        result.print("MainOutput");
        lateData.print("LateData");
    }

    /**
     * 3.7 多键分组窗口示例
     */
    private static void multiKeyWindowExample(StreamExecutionEnvironment env) {
        // 订单数据流: (商品类别, 地区, 金额)
        DataStream<Tuple3<String, String, Double>> orders = env.fromElements(
                Tuple3.of("Electronics", "North", 100.0),
                Tuple3.of("Electronics", "South", 200.0),
                Tuple3.of("Clothing", "North", 150.0),
                Tuple3.of("Electronics", "North", 300.0),
                Tuple3.of("Clothing", "South", 80.0)
        );

        // 按商品类别统计 - 使用Tuple KeySelector
        DataStream<String> byCategory = orders
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new WindowFunction<Tuple3<String, String, Double>, String, String, TimeWindow>() {
                    @Override
                    public void apply(
                            String category,
                            TimeWindow window,
                            Iterable<Tuple3<String, String, Double>> input,
                            Collector<String> out) {

                        double sum = 0;
                        int count = 0;
                        for (Tuple3<String, String, Double> t : input) {
                            sum += t.f2;
                            count++;
                        }
                        out.collect("Category=" + category + ", Sum=" + sum + ", Count=" + count);
                    }
                });

        // 按 (商品类别, 地区) 统计 - 使用自定义KeySelector
        DataStream<String> byCategoryAndRegion = orders
                .keyBy(new KeySelector<Tuple3<String, String, Double>, String>() {
                    @Override
                    public String getKey(Tuple3<String, String, Double> value) throws Exception {
                        return value.f0 + ":" + value.f1;  // 组合key
                    }
                })
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new WindowFunction<Tuple3<String, String, Double>, String, String, TimeWindow>() {
                    @Override
                    public void apply(
                            String key,
                            TimeWindow window,
                            Iterable<Tuple3<String, String, Double>> input,
                            Collector<String> out) {

                        double sum = 0;
                        for (Tuple3<String, String, Double> t : input) {
                            sum += t.f2;
                        }
                        out.collect("Key=" + key + ", Sum=" + sum);
                    }
                });

        byCategory.print("ByCategory");
        byCategoryAndRegion.print("ByCategoryAndRegion");
    }

    /**
     * 3.8 窗口 Join 示例
     */
    private static void windowJoinExample(StreamExecutionEnvironment env) {
        // 订单流
        DataStream<Tuple2<String, Integer>> orders = env.fromElements(
                Tuple2.of("order1", 100),
                Tuple2.of("order2", 200)
        );

        // 支付流
        DataStream<Tuple2<String, Integer>> payments = env.fromElements(
                Tuple2.of("order1", 100),
                Tuple2.of("order2", 200)
        );

        // 基于滚动窗口的Join
        DataStream<String> joinedResult = orders
                .join(payments)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String>() {
                    @Override
                    public String join(
                            Tuple2<String, Integer> order,
                            Tuple2<String, Integer> payment) {

                        return "Order=" + order.f0 + ", OrderAmount=" + order.f1 +
                                ", PaymentAmount=" + payment.f1 +
                                ", Matched=" + (order.f1.equals(payment.f1));
                    }
                });

        joinedResult.print("WindowJoin");
    }

    // ==================== 多流 Join 示例 ====================

    /**
     * 4.1 完整 Window Join 示例 - 双流内连接
     */
    private static void windowJoinFullExample(StreamExecutionEnvironment env) {
        // 订单流
        DataStream<OrderEvent> orders = env.fromElements(
                new OrderEvent("order1", 100, System.currentTimeMillis()),
                new OrderEvent("order2", 200, System.currentTimeMillis())
        );

        // 支付流
        DataStream<PaymentEvent> payments = env.fromElements(
                new PaymentEvent("order1", 100, System.currentTimeMillis()),
                new PaymentEvent("order2", 200, System.currentTimeMillis())
        );

        // Window Join
        DataStream<String> result = orders
                .join(payments)
                .where(OrderEvent::getOrderId)
                .equalTo(PaymentEvent::getOrderId)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .apply(new JoinFunction<OrderEvent, PaymentEvent, String>() {
                    @Override
                    public String join(OrderEvent order, PaymentEvent payment) {
                        return "Order=" + order.getOrderId() +
                                ", Amount=" + order.getAmount() +
                                ", PaymentTime=" + payment.getPaymentTime();
                    }
                });

        result.print("WindowJoin");
    }

    /**
     * 4.2 Interval Join 示例 - 基于时间区间的Join
     */
    private static void intervalJoinExample(StreamExecutionEnvironment env) {
        // 订单流 (事件时间)
        DataStream<Tuple2<String, Long>> orders = env.fromElements(
                Tuple2.of("order1", 1000L),
                Tuple2.of("order2", 2000L),
                Tuple2.of("order3", 3000L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
        );

        // 支付流 (事件时间)
        DataStream<Tuple2<String, Long>> payments = env.fromElements(
                Tuple2.of("order1", 1500L),
                Tuple2.of("order2", 2500L)
        ).assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
        );

        // Interval Join: 订单后-1秒到+1秒内的支付
        DataStream<String> result = orders
                .keyBy(t -> t.f0)
                .intervalJoin(payments.keyBy(t -> t.f0))
                .between(Duration.ofSeconds(-1), Duration.ofSeconds(1))
                .process(new KeyedProcessFunction<String,
                        Tuple2<String, Long>,
                        Tuple2<String, Long>,
                        String>() {

                    @Override
                    public void processElement(
                            Tuple2<String, Long> order,
                            Context ctx,
                            Collector<String> out) {

                        // 查询匹配的支付
                        // 在实际应用中，会在这里处理匹配逻辑
                        out.collect("Order=" + order.f0 + " at " + order.f1 +
                                " has potential matches");
                    }
                });

        result.print("IntervalJoin");
    }

    /**
     * 4.3 Connect + CoProcessFunction 双流处理
     */
    private static void connectCoProcessExample(StreamExecutionEnvironment env) {
        // 订单流
        DataStream<OrderEvent> orders = env.fromElements(
                new OrderEvent("order1", 100, System.currentTimeMillis()),
                new OrderEvent("order2", 200, System.currentTimeMillis())
        );

        // 支付流
        DataStream<PaymentEvent> payments = env.fromElements(
                new PaymentEvent("order1", 100, System.currentTimeMillis())
        );

        // 连接两条流
        ConnectedStreams<OrderEvent, PaymentEvent> connected =
                orders.connect(payments);

        DataStream<String> result = connected
                .process(new CoProcessFunction<OrderEvent, PaymentEvent, String>() {

                    // 订单状态
                    private final MapState<String, OrderEvent> orderState =
                            getRuntimeContext().getMapState(
                                    new MapStateDescriptor<>("orders", String.class, OrderEvent.class)
                            );

                    // 支付状态
                    private final MapState<String, PaymentEvent> paymentState =
                            getRuntimeContext().getMapState(
                                    new MapStateDescriptor<>("payments", String.class, PaymentEvent.class)
                            );

                    @Override
                    public void processElement1(OrderEvent order, Context ctx, Collector<String> out) {
                        // 处理订单流
                        orderState.put(order.getOrderId(), order);

                        // 检查是否有匹配的支付
                        PaymentEvent payment = paymentState.get(order.getOrderId());
                        if (payment != null) {
                            out.collect("MATCHED: Order=" + order.getOrderId() +
                                    ", Amount=" + order.getAmount());
                        } else {
                            out.collect("ORDER: " + order.getOrderId() + " waiting for payment");
                        }
                    }

                    @Override
                    public void processElement2(PaymentEvent payment, Context ctx, Collector<String> out) {
                        // 处理支付流
                        paymentState.put(payment.getOrderId(), payment);

                        // 检查是否有匹配的订单
                        OrderEvent order = orderState.get(payment.getOrderId());
                        if (order != null) {
                            out.collect("MATCHED: Payment=" + payment.getOrderId() +
                                    ", Amount=" + payment.getAmount());
                        } else {
                            out.collect("PAYMENT: " + payment.getOrderId() + " waiting for order");
                        }
                    }
                });

        result.print("ConnectCoProcess");
    }

    /**
     * 4.4 多条流 Connect 示例
     */
    private static void multiStreamConnectExample(StreamExecutionEnvironment env) {
        DataStream<String> streamA = env.fromElements("A1", "A2");
        DataStream<Integer> streamB = env.fromElements(100, 200);
        DataStream<Boolean> streamC = env.fromElements(true, false);

        // 先连接前两条流
        ConnectedStreams<String, Integer> ab = streamA.connect(streamB);

        // 再连接第三条流
        ConnectedStreams<String, Integer> abc = ab.connect(streamC);

        // 处理三条流
        DataStream<String> result = abc
                .process(new CoProcessFunction<String, Integer, Boolean, String>() {

                    @Override
                    public void processElement1(String value, Context ctx, Collector<String> out) {
                        out.collect("StreamA: " + value);
                    }

                    @Override
                    public void processElement2(Integer value, Context ctx, Collector<String> out) {
                        out.collect("StreamB: " + value);
                    }

                    @Override
                    public void processElement3(Boolean value, Context ctx, Collector<String> out) {
                        out.collect("StreamC: " + value);
                    }
                });

        result.print("MultiConnect");
    }

    /**
     * 4.5 多流 Union 示例
     */
    private static void streamUnionExample(StreamExecutionEnvironment env) {
        DataStream<String> stream1 = env.fromElements("from Kafka");
        DataStream<String> stream2 = env.fromElements("from MySQL");
        DataStream<String> stream3 = env.fromElements("from Oracle");

        // Union多条流 (必须是同类型)
        DataStream<String> unified = stream1.union(stream2).union(stream3);

        unified.print("Union");
    }

    /**
     * 4.6 实时对账系统 - 多流Join综合案例
     */
    private static void reconciliationSystemExample(StreamExecutionEnvironment env) {
        // 订单流
        DataStream<OrderEvent> orders = env.fromElements(
                new OrderEvent("order1", 100, System.currentTimeMillis()),
                new OrderEvent("order2", 200, System.currentTimeMillis()),
                new OrderEvent("order3", 150, System.currentTimeMillis())
        );

        // 支付流
        DataStream<PaymentEvent> payments = env.fromElements(
                new PaymentEvent("order1", 100, System.currentTimeMillis()),
                new PaymentEvent("order2", 200, System.currentTimeMillis())
        );

        // 物流流
        DataStream<ShipmentEvent> shipments = env.fromElements(
                new ShipmentEvent("order1", "shipped", System.currentTimeMillis())
        );

        // 连接订单和支付
        ConnectedStreams<OrderEvent, PaymentEvent> ordersPayments =
                orders.connect(payments);

        DataStream<String> result = ordersPayments
                .keyBy(o -> o.getOrderId(), p -> p.getOrderId())
                .process(new KeyedCoProcessFunction<String, OrderEvent, PaymentEvent, String>() {

                    private MapState<String, OrderEvent> orderState;
                    private MapState<String, PaymentEvent> paymentState;

                    @Override
                    public void open(Configuration parameters) {
                        orderState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("orders", String.class, OrderEvent.class)
                        );
                        paymentState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<>("payments", String.class, PaymentEvent.class)
                        );
                    }

                    @Override
                    public void processElement1(OrderEvent order, Context ctx, Collector<String> out) {
                        orderState.put(order.getOrderId(), order);

                        PaymentEvent payment = paymentState.get(order.getOrderId());
                        if (payment != null) {
                            out.collect("ORDER+PAYMENT: " + order.getOrderId());
                        } else {
                            out.collect("ORDER: " + order.getOrderId() + " [waiting payment]");
                        }
                    }

                    @Override
                    public void processElement2(PaymentEvent payment, Context ctx, Collector<String> out) {
                        paymentState.put(payment.getOrderId(), payment);

                        OrderEvent order = orderState.get(payment.getOrderId());
                        if (order != null) {
                            out.collect("PAYMENT+ORDER: " + payment.getOrderId());
                        } else {
                            out.collect("PAYMENT: " + payment.getOrderId() + " [waiting order]");
                        }
                    }
                });

        result.print("Reconciliation");
    }

    // ==================== 辅助类和函数 ====================

    /**
     * 订单事件
     */
    public static class OrderEvent {
        private String orderId;
        private double amount;
        private long timestamp;

        public OrderEvent() {}

        public OrderEvent(String orderId, double amount, long timestamp) {
            this.orderId = orderId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        public String getOrderId() { return orderId; }
        public double getAmount() { return amount; }
        public long getTimestamp() { return timestamp; }

        @Override
        public String toString() {
            return "OrderEvent{orderId='" + orderId + "', amount=" + amount + "}";
        }
    }

    /**
     * 支付事件
     */
    public static class PaymentEvent {
        private String orderId;
        private double amount;
        private long timestamp;

        public PaymentEvent() {}

        public PaymentEvent(String orderId, double amount, long timestamp) {
            this.orderId = orderId;
            this.amount = amount;
            this.timestamp = timestamp;
        }

        public String getOrderId() { return orderId; }
        public double getAmount() { return amount; }
        public long getPaymentTime() { return timestamp; }

        @Override
        public String toString() {
            return "PaymentEvent{orderId='" + orderId + "', amount=" + amount + "}";
        }
    }

    /**
     * 物流事件
     */
    public static class ShipmentEvent {
        private String orderId;
        private String status;
        private long timestamp;

        public ShipmentEvent() {}

        public ShipmentEvent(String orderId, String status, long timestamp) {
            this.orderId = orderId;
            this.status = status;
            this.timestamp = timestamp;
        }

        public String getOrderId() { return orderId; }
        public String getStatus() { return status; }

        @Override
        public String toString() {
            return "ShipmentEvent{orderId='" + orderId + "', status='" + status + "'}";
        }
    }

    /**
     * 自定义Aggregate函数 - 计算平均值
     */
    public static class MyAggregate
            implements AggregateFunction<Integer, Tuple2<Integer, Integer>, Double> {

        @Override
        public Tuple2<Integer, Integer> createAccumulator() {
            return Tuple2.of(0, 0);  // (sum, count)
        }

        @Override
        public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> acc) {
            return Tuple2.of(acc.f0 + value, acc.f1 + 1);
        }

        @Override
        public Double getResult(Tuple2<Integer, Integer> acc) {
            return (double) acc.f0 / acc.f1;
        }

        @Override
        public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
            return Tuple2.of(a.f0 + b.f0, a.f1 + b.f1);
        }
    }

    /**
     * 窗口聚合函数 - 统计汇总
     */
    public static class MyAggregateFunction
            implements AggregateFunction<Tuple2<String, Integer>, Tuple4<String, Integer, Integer, Double>, String> {

        @Override
        public Tuple4<String, Integer, Integer, Double> createAccumulator() {
            return Tuple4.of("", 0, 0, 0.0);
        }

        @Override
        public Tuple4<String, Integer, Integer, Double> add(
                Tuple2<String, Integer> value,
                Tuple4<String, Integer, Integer, Double> acc) {

            String key = value.f0;
            int sum = acc.f1 + value.f1;
            int count = acc.f2 + 1;
            double avg = (double) sum / count;

            // 只在第一次时设置key
            if (acc.f0.isEmpty()) {
                return Tuple4.of(key, sum, count, avg);
            }
            return Tuple4.of(acc.f0, sum, count, avg);
        }

        @Override
        public String getResult(Tuple4<String, Integer, Integer, Double> acc) {
            return "Key=" + acc.f0 + ", Sum=" + acc.f1 +
                    ", Count=" + acc.f2 + ", Avg=" + String.format("%.2f", acc.f3);
        }

        @Override
        public Tuple4<String, Integer, Integer, Double> merge(
                Tuple4<String, Integer, Integer, Double> a,
                Tuple4<String, Integer, Integer, Double> b) {

            return Tuple4.of(
                    a.f0.isEmpty() ? b.f0 : a.f0,
                    a.f1 + b.f1,
                    a.f2 + b.f2,
                    (double) (a.f1 + b.f1) / (a.f2 + b.f2)
            );
        }
    }
}
