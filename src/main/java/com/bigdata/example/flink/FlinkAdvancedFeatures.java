package com.bigdata.example.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.Accumulator;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.accumulators.DoubleCounter;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.*;
import org.apache.flink.streaming.api.windowing.evictors.CountEvictor;
import org.apache.flink.streaming.api.windowing.evictors.TimeEvictor;
import org.apache.flink.streaming.api.windowing.functions.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.*;

/**
 * Flink高级特性示例
 * 包含: Timer定时器、CEP、Checkpoint、SideOutput、Evictor、累加器等
 */
public class FlinkAdvancedFeatures {

    public static void main(String[] args) throws Exception {
        System.out.println("========================================");
        System.out.println("   Flink高级特性完整示例");
        System.out.println("========================================\n");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        env.setBufferTimeout(100);

        // ==================== Checkpoint配置 ====================
        configureCheckpoint(env);

        // ==================== Timer定时器 ====================
        timerExample(env);

        // ==================== SideOutput多输出 ====================
        sideOutputExample(env);

        // ==================== Evictor使用 ====================
        evictorExample(env);

        // ==================== 累加器使用 ====================
        accumulatorExample(env);

        // ==================== ProcessWindowFunction ====================
        processWindowFunctionExample(env);

        // ==================== 分布式缓存 ====================
        distributedCacheExample(env);

        // ==================== BroadcastProcessFunction ====================
        broadcastProcessExample(env);

        // ==================== 状态后端配置 ====================
        stateBackendExample(env);

        System.out.println("\n========================================");
        System.out.println("   Flink高级特性示例执行完成!");
        System.out.println("========================================");
    }

    // ==================== Checkpoint配置 ====================

    /**
     * Checkpoint配置详解:
     * 1. 开启Checkpoint
     * 2. 配置Checkpoint间隔
     * 3. 配置Checkpoint模式 (EXACTLY_ONCE vs AT_LEAST_ONCE)
     * 4. 配置状态后端
     */
    private static void configureCheckpoint(StreamExecutionEnvironment env) {
        System.out.println("\n--- Checkpoint配置示例 ---");

        // 1. 开启Checkpoint, 间隔60秒
        env.enableCheckpointing(60000);

        // 2. 配置Checkpoint模式
        CheckpointConfig config = env.getCheckpointConfig();
        config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        // 3. Checkpoint最小间隔 (防止频繁Checkpoint)
        config.setMinPauseBetweenCheckpoints(30000);

        // 4. Checkpoint超时时间
        config.setCheckpointTimeout(120000);

        // 5. 任务取消时保留Checkpoint
        config.setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        // 6. 允许不Checkpoint的Task失败
        config.setTolerableCheckpointFailureNumber(3);

        // 7. 配置状态后端 (HashMapStateBackend)
        // env.setStateBackend(new HashMapStateBackend());
        // env.getCheckpointConfig().setCheckpointStorage("hdfs://namenode:8020/flink/checkpoints");

        // 8. 增量Checkpoint (RocksDB)
        // config.enableIncrementalCheckpointing();

        System.out.println("Checkpoint配置项:");
        System.out.println("  1. enableCheckpointing(interval): 开启Checkpoint");
        System.out.println("  2. setCheckpointingMode: EXACTLY_ONCE/AT_LEAST_ONCE");
        System.out.println("  3. setMinPauseBetweenCheckpoints: 最小间隔");
        System.out.println("  4. setCheckpointTimeout: 超时时间");
        System.out.println("  5. setExternalizedCheckpointCleanup: 取消时保留策略");
        System.out.println("  6. setStateBackend: 状态后端 (HashMap/RocksDB)");
        System.out.println("  7. enableIncrementalCheckpointing: 增量Checkpoint (RocksDB)");
    }

    // ==================== Timer定时器 ====================

    /**
     * Timer定时器使用:
     * 1. ProcessingTimeTimer - 基于处理时间
     * 2. EventTimeTimer - 基于事件时间
     *
     * 应用场景:
     * - 超时检测
     * - 状态清理
     * - 定时计算
     */
    private static void timerExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- Timer定时器示例 ---");

        // 模拟订单事件流
        DataStream<OrderEvent> orders = env.fromElements(
                new OrderEvent("order1", 100, System.currentTimeMillis()),
                new OrderEvent("order2", 200, System.currentTimeMillis()),
                new OrderEvent("order3", 150, System.currentTimeMillis())
        );

        // 使用Timer检测超时订单 (30秒超时)
        DataStream<String> result = orders
                .keyBy(OrderEvent::getOrderId)
                .process(new KeyedProcessFunction<String, OrderEvent, String>() {

                    // 状态: 记录订单金额
                    private transient ValueState<Double> amountState;
                    // 状态: 记录订单时间
                    private transient ValueState<Long> timestampState;

                    @Override
                    public void open(Configuration parameters) {
                        amountState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("amount", Double.class)
                        );
                        timestampState = getRuntimeContext().getState(
                                new ValueStateDescriptor<>("timestamp", Long.class)
                        );
                    }

                    @Override
                    public void processElement(OrderEvent order, Context ctx,
                                               Collector<String> out) throws Exception {
                        // 记录订单信息
                        amountState.update(order.getAmount());
                        timestampState.update(order.getTimestamp());

                        // 注册30秒后的处理时间定时器
                        long timerTs = ctx.timerService().currentProcessingTime() + 30000;
                        ctx.timerService().registerProcessingTimeTimer(timerTs);

                        out.collect("Order received: " + order.getOrderId() +
                                   ", will timeout at: " + new Date(timerTs));
                    }

                    @Override
                    public void onTimer(long timerTs, OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        // 定时器触发时检查订单状态
                        Double amount = amountState.value();
                        Long orderTs = timestampState.value();

                        if (amount != null) {
                            out.collect("TIMEOUT CHECK for order, amount=" + amount +
                                       ", orderTime=" + new Date(orderTs) +
                                       ", currentTime=" + new Date(System.currentTimeMillis()));

                            // 如果需要，可以在这里清理状态
                            amountState.clear();
                            timestampState.clear();
                        }
                    }
                });

        System.out.println("Timer使用场景:");
        System.out.println("  1. 超时检测: 订单N秒未支付则取消");
        System.out.println("  2. 状态清理: 定时清理过期状态");
        System.out.println("  3. 定时计算: 周期性汇总统计");
        System.out.println();
        System.out.println("Timer类型:");
        System.out.println("  - registerProcessingTimeTimer(): 处理时间定时器");
        System.out.println("  - registerEventTimeTimer(): 事件时间定时器");
        System.out.println("  - deleteProcessingTimeTimer(): 删除定时器");
        System.out.println("  - deleteEventTimeTimer(): 删除事件时间定时器");
    }

    // ==================== SideOutput多输出 ====================

    /**
     * SideOutput使用场景:
     * 1. 迟到数据分流
     * 2. 异常数据检测
     * 3. 多路输出 (A/B测试)
     */
    private static void sideOutputExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- SideOutput多输出示例 ---");

        // 定义多个侧输出标签
        final OutputTag<String> errorTag = new OutputTag<String>("error-data") {};
        final OutputTag<String> warningTag = new OutputTag<String>("warning-data") {};
        final OutputTag<String> debugTag = new OutputTag<String>("debug-data") {};

        // 模拟数据流
        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("normal", 50),
                Tuple2.of("warning", 80),
                Tuple2.of("error", 150),
                Tuple2.of("normal", 60),
                Tuple2.of("error", 200)
        );

        // 使用ProcessFunction进行多路输出
        SingleOutputStreamOperator<String> mainStream = input
                .process(new ProcessFunction<Tuple2<String, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> value,
                                               Context ctx, Collector<String> out) {

                        String type = value.f0;
                        int valueNum = value.f1;

                        if ("error".equals(type)) {
                            // 输出到错误流
                            ctx.output(errorTag, "ERROR: " + value);
                        } else if ("warning".equals(type)) {
                            // 输出到警告流
                            ctx.output(warningTag, "WARNING: " + value);
                        } else {
                            // 主输出
                            out.collect("NORMAL: " + value);
                        }

                        // 调试信息
                        ctx.output(debugTag, "DEBUG: processing " + value);
                    }
                });

        // 获取各路输出
        DataStream<String> errorStream = mainStream.getSideOutput(errorTag);
        DataStream<String> warningStream = mainStream.getSideOutput(warningTag);
        DataStream<String> debugStream = mainStream.getSideOutput(debugTag);

        System.out.println("SideOutput使用步骤:");
        System.out.println("  1. 定义OutputTag: new OutputTag<Type>(\"tag-name\") {}");
        System.out.println("  2. 使用ctx.output(tag, data)输出到侧输出流");
        System.out.println("  3. 使用getSideOutput(tag)获取侧输出流");
        System.out.println();
        System.out.println("SideOutput应用场景:");
        System.out.println("  1. 迟到数据处理");
        System.out.println("  2. 异常/错误数据分流");
        System.out.println("  3. A/B测试分流");
        System.out.println("  4. 数据血缘追踪");
    }

    // ==================== Evictor使用 ====================

    /**
     * Evictor类型:
     * 1. CountEvictor - 按数量驱逐
     * 2. TimeEvictor - 按时间驱逐
     * 3. DeltaEvictor - 按Delta函数驱逐
     */
    private static void evictorExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- Evictor使用示例 ---");

        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("key", 1),
                Tuple2.of("key", 2),
                Tuple2.of("key", 3),
                Tuple2.of("key", 4),
                Tuple2.of("key", 5),
                Tuple2.of("key", 6)
        );

        // 1. CountEvictor - 保留最后3条数据
        DataStream<Integer> countEvictorResult = input
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .evictor(CountEvictor.of(3))  // 只保留3条
                .apply(new WindowFunction<Tuple2<String, Integer>, Integer, String, TimeWindow>() {
                    @Override
                    public void apply(String key, TimeWindow window,
                                      Iterable<Tuple2<String, Integer>> input,
                                      Collector<Integer> out) {
                        int sum = 0;
                        int count = 0;
                        for (Tuple2<String, Integer> t : input) {
                            sum += t.f1;
                            count++;
                        }
                        out.collect(sum);
                    }
                });

        // 2. TimeEvictor - 保留最近5秒的数据
        DataStream<Integer> timeEvictorResult = input
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .evictor(TimeEvictor.of(Duration.ofSeconds(5)))  // 保留5秒内的数据
                .sum(1);

        // 3. 先Evictor再窗口函数 (默认顺序)
        // 或使用 .evictorAfterWindow() 在窗口函数后执行Evictor

        System.out.println("Evictor类型:");
        System.out.println("  1. CountEvictor.of(count): 按数量保留");
        System.out.println("  2. TimeEvictor.of(duration): 按时间保留");
        System.out.println("  3. DeltaEvictor.of(threshold, deltaFunction): 按Delta驱逐");
        System.out.println();
        System.out.println("Evictor执行时机:");
        System.out.println("  - evictorBeforeWindow(): 默认, 窗口函数前执行");
        System.out.println("  - evictorAfterWindow(): 窗口函数后执行");
    }

    // ==================== 累加器使用 ====================

    /**
     * 累加器类型:
     * 1. IntCounter, LongCounter, DoubleCounter - 计数器
     * 2. Histogram - 直方图
     * 3. AverageAccumulator - 平均值累加器
     */
    private static void accumulatorExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- 累加器使用示例 ---");

        DataStream<String> input = env.fromElements(
                "a", "b", "c", "a", "a", "b", "d", "a"
        );

        // 使用RichFunction访问累加器
        DataStream<String> result = input
                .map(new RichMapFunction<String, String>() {
                    // 定义累加器
                    private transient IntCounter elementCounter;
                    private transient LongCounter charCounter;
                    private transient Map<String, IntCounter> typeCounters;

                    @Override
                    public void open(Configuration parameters) {
                        // 初始化累加器
                        elementCounter = getRuntimeContext().getIntCounter("element-count");
                        charCounter = getRuntimeContext().getLongCounter("char-count");
                        typeCounters = new HashMap<>();
                    }

                    @Override
                    public String map(String value) throws Exception {
                        // 更新累加器
                        elementCounter.add(1);
                        charCounter.add(value.length());

                        // 按类型计数
                        if (!typeCounters.containsKey(value)) {
                            typeCounters.put(value, getRuntimeContext().getIntCounter("type-" + value));
                        }
                        typeCounters.get(value).add(1);

                        return value;
                    }

                    @Override
                    public void close() throws Exception {
                        // 在任务结束时输出累加器结果
                        System.out.println("Element count: " + elementCounter.get());
                        System.out.println("Total chars: " + charCounter.get());
                        for (Map.Entry<String, IntCounter> entry : typeCounters.entrySet()) {
                            System.out.println("Type '" + entry.getKey() + "' count: " + entry.getValue().get());
                        }
                    }
                });

        // 获取累加器结果 (需要在Job执行完成后)
        // JobExecutionResult result = env.execute();
        // int count = result.getAccumulatorResult("element-count");

        System.out.println("累加器类型:");
        System.out.println("  1. IntCounter, LongCounter, DoubleCounter: 数值计数器");
        System.out.println("  2. Histogram: 直方图 (需要额外依赖)");
        System.out.println();
        System.out.println("累加器使用步骤:");
        System.out.println("  1. 在RichFunction中定义累加器");
        System.out.println("  2. 在map/filter等方法中更新累加器");
        System.out.println("  3. Job完成后通过ExecutionResult获取结果");
        System.out.println("  4. 注意: 累加器在Task结束时才聚合");
    }

    // ==================== ProcessWindowFunction ====================

    /**
     * ProcessWindowFunction vs 增量聚合:
     * 1. ProcessWindowFunction - 获得窗口所有数据, 可以访问元数据
     * 2. 增量聚合 - 内存效率高, 性能好
     */
    private static void processWindowFunctionExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- ProcessWindowFunction示例 ---");

        DataStream<Tuple2<String, Integer>> input = env.fromElements(
                Tuple2.of("order", 100),
                Tuple2.of("order", 200),
                Tuple2.of("order", 150),
                Tuple2.of("order", 300)
        );

        // 1. 全量窗口函数 - 获取窗口所有信息
        DataStream<String> fullResult = input
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .process(new ProcessWindowFunction<
                        Tuple2<String, Integer>,
                        String,
                        String,
                        TimeWindow>() {

                    @Override
                    public void process(String key,
                                        Context context,
                                        Iterable<Tuple2<String, Integer>> input,
                                        Collector<String> out) {
                        // 获取窗口信息
                        TimeWindow window = context.window();
                        long start = window.getStart();
                        long end = window.getEnd();
                        long currentWatermark = context.currentWatermark();

                        // 遍历窗口内所有数据
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

                        double avg = count > 0 ? (double) sum / count : 0;

                        out.collect("Window [" + new Date(start) + ", " + new Date(end) + "] " +
                                   "Key=" + key + ", Count=" + count +
                                   ", Sum=" + sum + ", Avg=" + String.format("%.2f", avg) +
                                   ", Max=" + max + ", Min=" + min +
                                   ", Watermark=" + currentWatermark);
                    }
                });

        // 2. 增量 + 全量结合 - 先聚合再处理
        DataStream<String> combinedResult = input
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Duration.ofSeconds(10)))
                .reduce(
                        // 增量聚合: 计算总和
                        (v1, v2) -> Tuple2.of(v1.f0, v1.f1 + v2.f1),
                        // 全量处理: 包装结果
                        new ProcessWindowFunction<
                                Tuple2<String, Integer>,
                                String,
                                String,
                                TimeWindow>() {
                            @Override
                            public void process(String key, Context context,
                                                Iterable<Tuple2<String, Integer>> input,
                                                Collector<String> out) {
                                Tuple2<String, Integer> aggregated = input.iterator().next();
                                TimeWindow window = context.window();
                                out.collect("Window " + window + " Key=" + key +
                                           " Total=" + aggregated.f1);
                            }
                        }
                );

        System.out.println("ProcessWindowFunction特点:");
        System.out.println("  1. 可以访问窗口元数据 (window, watermark, state)");
        System.out.println("  2. 可以遍历窗口内所有数据");
        System.out.println("  3. 性能相对增量聚合较低");
        System.out.println();
        System.out.println("Context提供的方法:");
        System.out.println("  - context.window(): 获取窗口对象");
        System.out.println("  - context.currentWatermark(): 获取当前水位线");
        System.out.println("  - context.globalState(): 全局状态");
        System.out.println("  - context.windowState(): 窗口状态");
    }

    // ==================== 分布式缓存 ====================

    /**
     * 分布式缓存使用:
     * - 在open()方法中读取缓存文件
     * - 缓存文件在TaskManager本地可用
     */
    private static void distributedCacheExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- 分布式缓存示例 ---");

        // 注册分布式缓存文件
        try {
            env.registerCachedFile("hdfs://namenode:8020/data/rules.txt", "rules-file");
        } catch (Exception e) {
            System.out.println("注: 缓存文件注册需要实际文件存在");
        }

        DataStream<String> input = env.fromElements(
                "user1", "user2", "user3"
        );

        DataStream<String> result = input
                .process(new ProcessFunction<String, String>() {
                    private transient Map<String, String> rules;

                    @Override
                    public void open(Configuration parameters) {
                        // 读取分布式缓存文件
                        try {
                            org.apache.flink.core.fs.Path cachePath =
                                    getRuntimeContext().getDistributedCache()
                                            .getFile("rules-file");
                            java.io.BufferedReader reader = new java.io.BufferedReader(
                                    new java.io.FileReader(cachePath.getPath()));
                            rules = new HashMap<>();
                            String line;
                            while ((line = reader.readLine()) != null) {
                                String[] parts = line.split(",");
                                if (parts.length >= 2) {
                                    rules.put(parts[0], parts[1]);
                                }
                            }
                            reader.close();
                        } catch (Exception e) {
                            // 使用默认规则
                            rules = new HashMap<>();
                            rules.put("user1", "VIP");
                            rules.put("user2", "Normal");
                        }
                    }

                    @Override
                    public void processElement(String user, Context ctx,
                                               Collector<String> out) {
                        String level = rules.getOrDefault(user, "Default");
                        out.collect("User=" + user + ", Level=" + level);
                    }
                });

        System.out.println("分布式缓存使用场景:");
        System.out.println("  1. 规则/配置文件共享");
        System.out.println("  2. 维度表数据缓存");
        System.out.println("  3. 模型文件共享 (机器学习)");
        System.out.println();
        System.out.println("使用步骤:");
        System.out.println("  1. env.registerCachedFile(path, name): 注册文件");
        System.out.println("  2. getRuntimeContext().getDistributedCache().getFile(name): 获取文件");
        System.out.println("  3. 在open()方法中读取文件内容");
    }

    // ==================== BroadcastProcessFunction ====================

    /**
     * BroadcastProcessFunction vs CoProcessFunction:
     * - BroadcastProcessFunction: 一条流广播, 另一条流读取
     * - CoProcessFunction: 两条流平等处理
     */
    private static void broadcastProcessExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- BroadcastProcessFunction示例 ---");

        // 数据流
        DataStream<Tuple2<String, Integer>> dataStream = env.fromElements(
                Tuple2.of("rule1", 100),
                Tuple2.of("rule2", 200),
                Tuple2.of("rule3", 150)
        );

        // 配置流 (动态更新)
        DataStream<Map<String, Object>> configStream = env.fromElements(
                new HashMap<String, Object>() {{ put("threshold", 120); put("enabled", true); }},
                new HashMap<String, Object>() {{ put("threshold", 180); put("enabled", true); }}
        );

        // 广播配置流
        MapStateDescriptor<String, Map<String, Object>> configDescriptor =
                new MapStateDescriptor<>("config-broadcast", String.class, Map.class);
        BroadcastStream<Map<String, Object>> broadcastConfig =
                configStream.broadcast(configDescriptor);

        // 连接数据流和广播流
        DataStream<String> result = dataStream
                .connect(broadcastConfig)
                .process(new BroadcastProcessFunction<
                        Tuple2<String, Integer>,
                        Map<String, Object>,
                        String>() {

                    @Override
                    public void processElement(Tuple2<String, Integer> value,
                                               ReadOnlyContext ctx,
                                               Collector<String> out) {
                        // 读取广播配置 (只读)
                        ReadOnlyBroadcastState<String, Map<String, Object>> config =
                                ctx.getBroadcastState(configDescriptor);

                        Map<String, Object> configData = config.get("global");
                        if (configData != null) {
                            Boolean enabled = (Boolean) configData.get("enabled");
                            Number threshold = (Number) configData.get("threshold");

                            if (enabled != null && enabled) {
                                if (value.f1 > threshold.intValue()) {
                                    out.collect("ALERT: " + value + " exceeds threshold " + threshold);
                                } else {
                                    out.collect("OK: " + value + " is within threshold");
                                }
                            } else {
                                out.collect("DISABLED: rule " + value.f0 + " processing disabled");
                            }
                        } else {
                            out.collect("NO_CONFIG: " + value + " (using default)");
                        }
                    }

                    @Override
                    public void processBroadcastElement(Map<String, Object> config,
                                                         Context ctx,
                                                         Collector<String> out) {
                        // 更新广播状态
                        ctx.getBroadcastState(configDescriptor).put("global", config);
                        out.collect("CONFIG UPDATED: threshold=" + config.get("threshold"));
                    }
                });

        System.out.println("BroadcastProcessFunction方法:");
        System.out.println("  1. processElement(): 处理数据流 (只读访问广播状态)");
        System.out.println("  2. processBroadcastElement(): 处理广播流 (读写广播状态)");
        System.out.println();
        System.out.println("使用场景:");
        System.out.println("  1. 动态规则引擎");
        System.out.println("  2. 实时配置更新");
        System.out.println("  3. 黑/白名单维护");
    }

    // ==================== 状态后端配置 ====================

    private static void stateBackendExample(StreamExecutionEnvironment env) {
        System.out.println("\n--- 状态后端配置示例 ---");

        System.out.println("状态后端类型:");
        System.out.println("  1. HashMapStateBackend");
        System.out.println("     - 状态存储在TaskManager内存");
        System.out.println("     - 适合状态较小的场景");
        System.out.println("     - 性能最高");
        System.out.println();
        System.out.println("  2. EmbeddedRocksDBStateBackend");
        System.out.println("     - 状态存储在RocksDB (磁盘)");
        System.out.println("     - 适合大状态场景");
        System.out.println("     - 支持增量Checkpoint");
        System.out.println();
        System.out.println("配置方式:");
        System.out.println("  // HashMapStateBackend (默认)");
        System.out.println("  env.setStateBackend(new HashMapStateBackend());");
        System.out.println();
        System.out.println("  // EmbeddedRocksDBStateBackend");
        System.out.println("  env.setStateBackend(new EmbeddedRocksDBStateBackend());");
        System.out.println("  env.getCheckpointConfig().setCheckpointStorage(\"hdfs://...\");");
    }

    // ==================== 辅助类 ====================

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
            return "Order{orderId='" + orderId + "', amount=" + amount + "}";
        }
    }
}
