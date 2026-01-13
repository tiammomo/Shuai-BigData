# Flink 状态管理与 Checkpoint

## 目录

- [状态类型](#状态类型)
- [状态后端](#状态后端)
- [Checkpoint 配置](#checkpoint-配置)
- [Savepoint 操作](#savepoint-操作)
- [状态恢复](#状态恢复)
- [最佳实践](#最佳实践)

---

## 状态类型

### Keyed State

```java
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.configuration.Configuration;

/**
 * Keyed State - 按 Key 分组的状态
 *
 * 特点:
 * - 只能用于 KeyedStream
 * - 每个 Key 一份状态
 * - 自动按 Key 分区
 */
public class KeyedStateExample {

    /**
     * 1. ValueState - 单值状态
     *
     * 存储单个值
     */
    public static class ValueStateExample
            extends KeyedProcessFunction<String, Order, OrderStats> {

        private ValueState<OrderStats> state;

        @Override
        public void open(Configuration parameters) {
            // 获取状态句柄
            ValueStateDescriptor<OrderStats> descriptor =
                new ValueStateDescriptor<>(
                    "order-stats",  // 状态名称
                    OrderStats.class  // 状态类型
                );
            // 配置状态 TTL
            StateTtlConfig ttlConfig = StateTtlConfig
                .newBuilder(org.apache.flink.api.common.time.Time.hours(24))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();
            descriptor.enableTimeToLive(ttlConfig);

            state = getRuntimeContext().getState(descriptor);
        }

        @Override
        public void processElement(Order order,
                                    Context ctx,
                                    Collector<OrderStats> out) throws Exception {
            // 读取状态
            OrderStats currentStats = state.value();
            if (currentStats == null) {
                currentStats = new OrderStats();
            }

            // 更新状态
            currentStats.count++;
            currentStats.totalAmount =
                currentStats.totalAmount.add(order.getAmount());
            currentStats.lastOrderTime = order.getOrderTime();

            // 保存状态
            state.update(currentStats);

            // 输出
            out.collect(currentStats);
        }

        @Override
        public void onTimer(long timestamp,
                            OnTimerContext ctx,
                            Collector<OrderStats> out) {
            // 定时触发
            OrderStats stats = state.value();
            if (stats != null) {
                out.collect(stats);
            }
        }
    }

    /**
     * 2. ListState - 列表状态
     *
     * 存储元素列表
     */
    public static class ListStateExample
            extends KeyedProcessFunction<String, Order, Order> {

        private ListState<Order> orderListState;

        @Override
        public void open(Configuration parameters) {
            ListStateDescriptor<Order> descriptor =
                new ListStateDescriptor<>("order-list", Order.class);
            orderListState = getRuntimeContext().getListState(descriptor);
        }

        @Override
        public void processElement(Order order,
                                    Context ctx,
                                    Collector<Order> out) throws Exception {
            // 添加到列表
            orderListState.add(order);

            // 读取列表
            Iterable<Order> orders = orderListState.get();
            for (Order o : orders) {
                out.collect(o);
            }
        }
    }

    /**
     * 3. MapState - 映射状态
     *
     * 存储 Key-Value 对
     */
    public static class MapStateExample
            extends KeyedProcessFunction<String, Order, Order> {

        private MapState<String, Order> orderMapState;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, Order> descriptor =
                new MapStateDescriptor<>(
                    "order-map",
                    String.class,
                    Order.class
                );
            orderMapState = getRuntimeContext().getMapState(descriptor);
        }

        @Override
        public void processElement(Order order,
                                    Context ctx,
                                    Collector<Order> out) throws Exception {
            // 存储订单
            orderMapState.put(order.getOrderId().toString(), order);

            // 查询订单
            Order stored = orderMapState.get(order.getOrderId().toString());

            // 遍历所有订单
            for (String key : orderMapState.keys()) {
                Order o = orderMapState.get(key);
                out.collect(o);
            }
        }
    }

    /**
     * 4. ReducingState - 归约状态
     *
     * 自动归约的值
     */
    public static class ReducingStateExample
            extends KeyedProcessFunction<String, Order, OrderStats> {

        private ReducingState<OrderStats> reducingState;

        @Override
        public void open(Configuration parameters) {
            ReducingStateDescriptor<OrderStats> descriptor =
                new ReducingStateDescriptor<>(
                    "reducing-stats",
                    (a, b) -> {
                        a.count += b.count;
                        a.totalAmount = a.totalAmount.add(b.totalAmount);
                        return a;
                    },
                    OrderStats.class
                );
            reducingState = getRuntimeContext().getReducingState(descriptor);
        }

        @Override
        public void processElement(Order order,
                                    Context ctx,
                                    Collector<OrderStats> out) throws Exception {
            // 自动归约
            OrderStats stats = new OrderStats();
            stats.count = 1;
            stats.totalAmount = order.getAmount();
            reducingState.add(stats);

            // 获取结果
            out.collect(reducingState.get());
        }
    }

    /**
     * 5. AggregatingState - 聚合状态
     *
     * 自动聚合的值，支持不同输入输出类型
     */
    public static class AggregatingStateExample
            extends KeyedProcessFunction<String, Order, OrderStats> {

        private AggregatingState<Order, OrderStats> aggregatingState;

        @Override
        public void open(Configuration parameters) {
            AggregatingStateDescriptor<Order, OrderAccumulator, OrderStats> descriptor =
                new AggregatingStateDescriptor<>(
                    "aggregating-stats",
                    new AggregateFunction<Order, OrderAccumulator, OrderStats>() {
                        @Override
                        public OrderAccumulator createAccumulator() {
                            return new OrderAccumulator();
                        }

                        @Override
                        public OrderAccumulator add(Order order,
                                                     OrderAccumulator acc) {
                            acc.count++;
                            acc.totalAmount =
                                acc.totalAmount.add(order.getAmount());
                            return acc;
                        }

                        @Override
                        public OrderStats getResult(OrderAccumulator acc) {
                            return new OrderStats(
                                acc.count,
                                acc.totalAmount.divide(
                                    BigDecimal.valueOf(acc.count), 2,
                                    RoundingMode.HALF_UP
                                )
                            );
                        }

                        @Override
                        public OrderAccumulator merge(OrderAccumulator a,
                                                        OrderAccumulator b) {
                            a.count += b.count;
                            a.totalAmount = a.totalAmount.add(b.totalAmount);
                            return a;
                        }
                    },
                    OrderAccumulator.class
                );
            aggregatingState = getRuntimeContext().getAggregatingState(descriptor);
        }

        @Override
        public void processElement(Order order,
                                    Context ctx,
                                    Collector<OrderStats> out) throws Exception {
            aggregatingState.add(order);
            out.collect(aggregatingState.get());
        }
    }

    // ============ 辅助类 ============

    public static class Order {
        private Long orderId;
        private String customerId;
        private BigDecimal amount;
        private java.sql.Timestamp orderTime;
        // getter/setter
    }

    public static class OrderStats {
        public int count;
        public BigDecimal totalAmount = BigDecimal.ZERO;
        public java.sql.Timestamp lastOrderTime;
    }

    public static class OrderAccumulator {
        public int count;
        public BigDecimal totalAmount = BigDecimal.ZERO;
    }
}
```

### Operator State

```java
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;

/**
 * Operator State - 算子级别的状态
 *
 * 特点:
 * - 用于非 Keyed 算子
 * - 需要实现 CheckpointedFunction 接口
 - 每个并行实例一份状态
 */
public class OperatorStateExample {

    /**
     * 1. List State (并行实例列表)
     *
     * 场景: 多并行度源，存储分区偏移量
     */
    public static class OffsetListSink
            implements SinkFunction<String>,
                       CheckpointedFunction {

        private transient ListState<String> checkpointedState;
        private List<String> bufferedElements;

        public OffsetListSink(int batchSize) {
            this.batchSize = batchSize;
            this.bufferedElements = new ArrayList<>();
        }

        @Override
        public void open(Configuration parameters) {
            // 初始化
        }

        @Override
        public void invoke(String value, Context context) {
            bufferedElements.add(value);

            if (bufferedElements.size() >= batchSize) {
                // 批量写入
                for (String element : bufferedElements) {
                    // 写入外部系统
                }
                bufferedElements.clear();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            // Checkpoint 时保存状态
            checkpointedState.clear();
            for (String element : bufferedElements) {
                checkpointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            // 初始化或恢复状态
            ListStateDescriptor<String> descriptor =
                new ListStateDescriptor<>(
                    "buffered-elements",
                    String.class
                );

            checkpointedState =
                context.getOperatorStateStore().getListState(descriptor);

            // 从 Checkpoint 恢复
            if (context.isRestored()) {
                for (String element : checkpointedState.get()) {
                    bufferedElements.add(element);
                }
            }
        }

        private int batchSize;
    }

    /**
     * 2. Union List State (所有实例获取全部状态)
     *
     * 场景: 需要访问所有分区数据的场景
     */
    public static class UnionListSink
            implements SinkFunction<String>,
                       CheckpointedFunction {

        private transient UnionListState<String> unionState;

        @Override
        public void initializeState(FunctionInitializationContext context) {
            UnionListStateDescriptor<String> descriptor =
                new UnionListStateDescriptor<>(
                    "union-state",
                    String.class
                );

            unionState =
                context.getOperatorStateStore().getUnionListState(descriptor);

            // 所有并行实例都可以获取全部数据
            for (String element : unionState.get()) {
                // 处理恢复的数据
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            // 保存状态
            for (String element : bufferedElements) {
                unionState.add(element);
            }
        }
    }

    /**
     * 3. Broadcast State (广播状态)
     *
     * 场景: 小维度表，全局同步
     */
    public static class BroadcastStateExample
            extends KeyedProcessFunction<String, Order, Order> {

        private transient BroadcastState<String, Rule> broadcastState;

        @Override
        public void open(Configuration parameters) {
            MapStateDescriptor<String, Rule> descriptor =
                new MapStateDescriptor<>(
                    "broadcast-rules",
                    String.class,
                    Rule.class
                );
            broadcastState = getRuntimeContext().getBroadcastState(descriptor);
        }

        @Override
        public void processElement(Order order,
                                    ReadOnlyContext ctx,
                                    Collector<Order> out) {
            // 读取广播状态
            ReadOnlyBroadcastState<String, Rule> state =
                ctx.getBroadcastState(new MapStateDescriptor<>(
                    "broadcast-rules", String.class, Rule.class));

            Rule rule = state.get(order.getCustomerId());
            if (rule != null) {
                // 应用规则
                order.apply(rule);
            }
            out.collect(order);
        }
    }

    /**
     * 4. 广播流连接
     */
    public static void broadcastStreamJoin() {
        // 主数据流
        DataStream<Order> orderStream = env.fromCollection(getOrders());

        // 规则流
        DataStream<Rule> ruleStream = env.fromCollection(getRules());

        // 广播规则
        BroadcastStream<Rule> broadcastStream = ruleStream.broadcast(
            new MapStateDescriptor<>(
                "rules",
                String.class,
                Rule.class
            )
        );

        // 连接
        DataStream<Order> result = orderStream
            .connect(broadcastStream)
            .process(
                new KeyedBroadcastProcessFunction<
                    String, Order, Rule, Order>() {

                    private MapState<String, Order> orderState;

                    @Override
                    public void open(Configuration parameters) {
                        orderState = getRuntimeContext().getMapState(
                            new MapStateDescriptor<>(
                                "orders",
                                String.class,
                                Order.class
                            )
                        );
                    }

                    @Override
                    public void processElement(Order order,
                                                ReadOnlyContext ctx,
                                                Collector<Order> out) {
                        // 读取广播状态
                        ReadOnlyBroadcastState<String, Rule> broadcastState =
                            ctx.getBroadcastState(
                                new MapStateDescriptor<>(
                                    "rules", String.class, Rule.class
                                )
                            );
                        // 处理
                    }

                    @Override
                    public void processBroadcastElement(Rule rule,
                                                         Context ctx) {
                        // 更新广播状态
                        ctx.getBroadcastState(
                            new MapStateDescriptor<>(
                                "rules", String.class, Rule.class
                            )
                        ).put(rule.getRuleId(), rule);
                    }
                }
            );
    }
}
```

---

## 状态后端

### 类型对比

```
┌─────────────────────────────────────────────────────────────────┐
│                    状态后端对比                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  | 特性         | MemoryStateBackend | FsStateBackend | RocksDB | │
│  |--------------|-------------------|---------------|---------| │
│  | 状态存储     | JVM 堆内存         | 文件系统      | RocksDB | │
│  | 状态大小     | 小 (GB 级)        | 中 (百GB 级)  | 大 (TB) | │
│  | 读写性能     | 最高              | 高           | 中     │ │
│  | 快照性能     | 快                | 快           | 慢     │ │
│  | 增量快照     | 不支持            | 不支持       | 支持   │ │
│  | 适用场景     | 开发测试          | 生产(中状态) | 生产(大状态)| │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 配置示例

```java
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 状态后端配置
 */
public class StateBackendConfig {

    /**
     * 1. MemoryStateBackend
     *
     * 状态存储在 JVM 堆内存
     */
    public static void memoryStateBackend() {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // 方式1: 配置最大状态大小
        env.setStateBackend(
            new MemoryStateBackend(1024 * 1024 * 1024)  // 1GB
        );
    }

    /**
     * 2. FsStateBackend
     *
     * 状态快照存储在文件系统
     */
    public static void fsStateBackend() {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // 方式1: 状态存储在内存，快照到文件系统
        env.setStateBackend(
            new FsStateBackend(
                "hdfs://namenode:8020/flink/checkpoints",
                true  // 异步快照
            )
        );

        // 方式2: 通过 CheckpointConfig 配置
        env.getCheckpointConfig().setCheckpointStorage(
            "hdfs://namenode:8020/flink/checkpoints"
        );
    }

    /**
     * 3. RocksDBStateBackend (推荐生产环境)
     *
     * 增量 Checkpoint，支持超大状态
     */
    public static void rocksDBStateBackend() {
        StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();

        // 创建 RocksDB 状态后端
        RocksDBStateBackend rocksDB =
            new RocksDBStateBackend(
                "hdfs://namenode:8020/flink/checkpoints",
                true  // 启用增量 Checkpoint
            );

        // 配置本地路径
        rocksDB.setDbStoragePath("/tmp/flink-rocksdb");

        // 预定义选项 (针对特定场景优化)
        rocksDB.setPredefinedOptions(
            PredefinedOptions.SPINNING_DISK_OPTIMIZED
        );

        // 增量 Checkpoint 配置
        rocksDB.enableIncrementalCheckpointing(true);

        // RocksDB 内存管理
        rocksDB.setRocksDBOptions(
            new org.rocksdb.Options()
                .setWriteBufferSize(64 * 1024 * 1024)  // 64MB
                .setMaxWriteBufferNumber(3)
                .setTargetFileSizeBase(64 * 1024 * 1024)
        );

        env.setStateBackend(rocksDB);

        // 配置 Checkpoint
        env.enableCheckpointing(60000);
        env.getCheckpointConfig().setCheckpointingMode(
            CheckpointingMode.EXACTLY_ONCE
        );
    }

    /**
     * 4. 通过配置文件配置
     */
    public static void configureViaConfig() {
        // flink-conf.yaml
        // state.backend: rocksdb
        // state.backend.incremental: true
        // state.backend.fs.checkpointdir: hdfs://namenode:8020/flink/checkpoints
        // state.rocksdb.memory.managed: true
        // state.rocksdb.memory.fixed-per-slot: 256mb
    }
}
```

---

## Checkpoint 配置

### 基本配置

```java
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;

/**
 * Checkpoint 配置详解
 */
public class CheckpointConfiguration {

    public static void basicConfig(StreamExecutionEnvironment env) {
        // 1. 启用 Checkpoint
        env.enableCheckpointing(60000);  // 60秒间隔

        // 2. Checkpoint 模式
        env.getCheckpointConfig().setCheckpointingMode(
            CheckpointingMode.EXACTLY_ONCE  // 精确一次
            // CheckpointingMode.AT_LEAST_ONCE  // 至少一次
        );

        // 3. Checkpoint 超时时间
        env.getCheckpointConfig()
            .setCheckpointTimeout(600000);  // 10分钟

        // 4. 最小间隔
        env.getCheckpointConfig()
            .setMinPauseBetweenCheckpoints(5000);  // 5秒

        // 5. 最大并发 Checkpoint 数
        env.getCheckpointConfig()
            .setMaxConcurrentCheckpoints(1);

        // 6. Checkpoint 存储
        env.getCheckpointConfig()
            .setCheckpointStorage("hdfs://namenode:8020/flink/checkpoints");

        // 7. 任务取消时保留 Checkpoint
        env.getCheckpointConfig()
            .setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup
                    .RETAIN_ON_CANCELLATION  // 保留
                    // DELETE_ON_CANCELLATION  // 删除
            );
    }

    public static void advancedConfig(StreamExecutionEnvironment env) {
        // 1. 非对齐 Checkpoint (减少反压影响)
        env.getCheckpointConfig()
            .enableUnalignedCheckpoints();

        // 2. 启用 Checkpoint 恢复
        env.getCheckpointConfig()
            .setTolerableCheckpointFailureNumber(3);

        // 3. Checkpoint 清理策略
        env.getCheckpointConfig()
            .setMaxConcurrentCheckpoints(1);
    }
}
```

### 增量 Checkpoint

```java
/**
 * 增量 Checkpoint 配置 (RocksDB)
 */
public class IncrementalCheckpointConfig {

    public static void configure(StreamExecutionEnvironment env) {
        RocksDBStateBackend rocksDB =
            new RocksDBStateBackend(
                "hdfs://namenode:8020/flink/checkpoints",
                true  // 启用增量 Checkpoint
            );

        // 配置增量 Checkpoint
        rocksDB.enableIncrementalCheckpointing(true);

        // 配置 RocksDB 内存
        rocksDB.setRocksDBMemoryAllocation(
            new RocksDBMemoryAllocationConfig()
                .setManagedMemory(512 * 1024 * 1024)  // 托管内存
                .setFixedMemoryPerSlot(256 * 1024 * 1024)  // 每 Slot 固定内存
                .setWriteBufferRatio(0.5)  // 写缓冲比例
        );

        // 配置压缩
        rocksDB.setPredefinedOptions(
            PredefinedOptions.SPINNING_DISK_OPTIMIZED
        );

        // 配置线程
        rocksDB.setNumberOfTransferThreads(4);
        rocksDB.setNumberOfSnapshotsPerCheckpoint(1);

        env.setStateBackend(rocksDB);
    }
}
```

---

## Savepoint 操作

### Savepoint 命令

```bash
#!/bin/bash
# Savepoint 操作命令

# ============ 触发 Savepoint ============

# 为 Job 触发 Savepoint
flink savepoint <jobId> [targetDirectory]

# 使用 YARN
flink savepoint -yid <yarnAppId> <jobId> [targetDirectory]

# 触发 Savepoint 并取消 Job
flink cancel -s [targetDirectory] <jobId>

# ============ 从 Savepoint 恢复 ============

# 使用 Savepoint 启动 Job
flink run -s <savepointPath> [runArgs]

# 跳过无法恢复的状态
flink run -s <savepointPath> --allowNonRestoredState [runArgs]

# ============ 管理 Savepoint ============

# 列出 Savepoint
ls hdfs://namenode:8020/flink/savepoints/

# 删除 Savepoint
flink savepoint -d <savepointPath>

# 清理孤立 Savepoint
# 手动删除过期文件

# ============ 分配器相关 Savepoint ============

# 分配 Savepoint
flink savepoint <jobId> <targetDirectory> --yid <yarnAppId>
```

### Savepoint 生命周期

```
┌─────────────────────────────────────────────────────────────────┐
│                    Savepoint 生命周期                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. 触发阶段                                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  - 暂停所有 Checkpoint                                    │   │
│  │  - 通知所有算子执行 snapshotState()                        │   │
│  │  - 等待所有算子完成                                        │   │
│  │  - 写入 Savepoint 到存储                                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  2. 存储阶段                                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  - _metadata (元数据文件)                                 │   │
│  │  - op_xxx (算子状态文件)                                  │   │
│  │  - shared (共享数据)                                      │   │
│  │  - private (私有数据)                                     │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  3. 恢复阶段                                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  - 读取 Savepoint 元数据                                  │   │
│  │  - 恢复算子状态                                           │   │
│  │  - 继续处理                                               │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 状态恢复

### 故障恢复

```java
/**
 * 状态恢复配置
 */
public class StateRecovery {

    /**
     * 1. 从 Checkpoint 恢复
     *
     * Flink 自动从最近的 Checkpoint 恢复
     */
    public static void fromCheckpoint() {
        // 配置状态后端
        env.setStateBackend(
            new RocksDBStateBackend(
                "hdfs://namenode:8020/flink/checkpoints",
                true
            )
        );

        // 配置 Checkpoint
        env.enableCheckpointing(60000);

        // 启动时自动从 Checkpoint 恢复
        // 无需额外配置
    }

    /**
     * 2. 从 Savepoint 恢复
     */
    public static void fromSavepoint() {
        // 方式1: 命令行参数
        // flink run -s hdfs://.../savepoint-xxx [jar] [args]

        // 方式2: 程序内配置 (不推荐)
        // Savepoint 恢复点只能在提交时指定
    }

    /**
     * 3. 处理状态兼容性问题
     */
    public static void stateCompatibility() {
        // 场景: 算子状态结构变化

        // 方案1: 使用 UnionListState
        // 所有并行实例获取完整状态列表

        // 方案2: 重新分配状态
        // 实现 CustomSchemaEvolution 逻辑

        // 方案3: 跳过不可恢复状态
        // flink run -s <savepoint> --allowNonRestoredState
    }

    /**
     * 4. 状态迁移
     */
    public static void stateMigration() {
        // 场景: 升级 Flink 版本

        // 步骤:
        // 1. 触发 Savepoint
        // 2. 升级 Flink
        // 3. 从 Savepoint 恢复
        // 4. 验证状态完整性
    }
}
```

---

## 最佳实践

### 状态管理最佳实践

```java
/**
 * 状态管理最佳实践
 */
public class StateBestPractices {

    /**
     * 1. 合理选择状态后端
     */
    public static void chooseStateBackend() {
        // 开发测试: MemoryStateBackend
        env.setStateBackend(new MemoryStateBackend());

        // 生产小状态: FsStateBackend
        env.setStateBackend(
            new FsStateBackend("hdfs://namenode:8020/flink/checkpoints")
        );

        // 生产大状态: RocksDBStateBackend
        RocksDBStateBackend rocksDB =
            new RocksDBStateBackend(
                "hdfs://namenode:8020/flink/checkpoints",
                true
            );
        rocksDB.setDbStoragePath("/tmp/flink-rocksdb");
        env.setStateBackend(rocksDB);
    }

    /**
     * 2. 状态 TTL 配置
     */
    public static void stateTTLConfig() {
        // 为状态配置 TTL，自动清理过期状态
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(24))  // 24小时过期
            .setUpdateType(
                StateTtlConfig.UpdateType.OnCreateAndWrite  // 创建和写入时更新
                // OnReadAndWrite  // 读写都更新
            )
            .setStateVisibility(
                StateTtlConfig.StateVisibility.NeverReturnExpired  // 不返回过期值
                // ReturnExpiredIfNotCleanedUp  // 清理前返回
            )
            .setCleanupFullSnapshot(true)  // 快照时清理
            .setCleanupIncrement(100, 10)  // 增量清理
            .build();

        // 应用到状态描述符
        ValueStateDescriptor<MyState> descriptor =
            new ValueStateDescriptor<>("my-state", MyState.class);
        descriptor.enableTimeToLive(ttlConfig);
    }

    /**
     * 3. 监控状态大小
     */
    public static void monitorStateSize() {
        // 通过 Metric 监控
        // - numRegisteredKeyedState
        // - keyedStateSize
        // - numCompletedCheckpoints
        // - lastCheckpointDuration

        // 告警配置
        // alert if keyedStateSize > 10GB
        // alert if numRegisteredKeyedState > 10000000
    }

    /**
     * 4. Checkpoint 优化
     */
    public static void checkpointOptimization() {
        // 1. 合理设置 Checkpoint 间隔
        env.enableCheckpointing(60000);  // 60秒

        // 2. 使用非对齐 Checkpoint
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        // 3. 配置 Checkpoint 超时
        env.getCheckpointConfig()
            .setCheckpointTimeout(600000);  // 10分钟

        // 4. 合理设置并发数
        env.getCheckpointConfig()
            .setMaxConcurrentCheckpoints(1);
    }
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Flink 架构详解](01-architecture.md) | Flink 核心架构和概念 |
| [Flink DataStream API](02-datastream.md) | DataStream API 详解 |
| [Flink Table API / SQL](03-table-sql.md) | Table API 和 SQL 详解 |
| [Flink CEP](05-cep.md) | 复杂事件处理 |
| [Flink 运维指南](06-operations.md) | 集群部署和运维 |
