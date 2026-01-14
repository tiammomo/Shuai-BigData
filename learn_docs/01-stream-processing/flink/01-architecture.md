# Flink 架构详解

## 目录

- [核心概念](#核心概念)
- [集群架构](#集群架构)
- [任务调度](#任务调度)
- [状态管理](#状态管理)
- [时间机制](#时间机制)
- [容错机制](#容错机制)
- [内存管理](#内存管理)
- [核心特性](#核心特性)

---

## 核心概念

### Flink 是什么?

```
┌─────────────────────────────────────────────────────────────────┐
│                    Apache Flink 简介                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Apache Flink 是一个分布式流处理框架，具有以下核心能力:           │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  ✓ 有状态流处理 - 端到端精确一次语义                      │   │
│  │  ✓ 事件时间处理 - 正确处理乱序事件                        │   │
│  │  ✓ 窗口计算 - 灵活的时间/计数窗口                         │   │
│  │  ✓ 流批一体 - 同一 API 处理流和批                         │   │
│  │  ✓ 高吞吐量 - 每秒数百万条消息                            │   │
│  │  ✓ 低延迟 - 毫秒级处理                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  vs 其他框架:                                                    │
│                                                                 │
│  | 特性        | Flink    | Spark Streaming | Storm  |         │
│  |------------|----------|-----------------|--------|         │
│  | 处理模型    | 原生流   | 微批           | 原生流  |         │
│  | 延迟        | 毫秒级   | 秒级           | 毫秒级  |         │
│  | 状态管理    | 内置     | 需额外         | 无     |         │
│  | 时间语义    | 完善     | 有限           | 无     |         │
│  | Exactly-Once| 支持    | 支持           | 支持    |         │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 核心特性

| 特性 | 说明 |
|------|------|
| **精确一次语义** | 端到端精确一次处理保证 |
| **流批一体** | 同一 API 处理流处理和批处理 |
| **状态管理** | 强大的有状态计算支持 |
| **时间语义** | 支持事件时间和处理时间 |
| **窗口计算** | 灵活的窗口机制 |
| **容错机制** | Checkpoint + Savepoint |

### 版本信息

```xml
<flink.version>1.18.1</flink.version>
```

Maven 依赖：
```xml
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-java</artifactId>
    <version>${flink.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-streaming-java</artifactId>
    <version>${flink.version}</version>
</dependency>
<dependency>
    <groupId>org.apache.flink</groupId>
    <artifactId>flink-clients</artifactId>
    <version>${flink.version}</version>
</dependency>
```

### 核心术语

| 术语 | 说明 | 重要性 |
|------|------|--------|
| **Job** | 一个 Flink 应用程序，包含多个 Operator | 核心 |
| **Task** | 算子的一个并行实例 | 核心 |
| **Operator** | 数据转换操作 (Map, Filter, Reduce 等) | 核心 |
| **Slot** | TaskManager 的资源单位，决定并发能力 | 重要 |
| **Parallelism** | 并行度，决定数据处理的并发程度 | 重要 |
| **Checkpoint** | 定期保存状态快照，用于容错 | 核心 |
| **Savepoint** | 手动触发的状态快照，用于升级/迁移 | 重要 |

### 核心术语

```java
/**
 * Flink 核心术语详解
 */
public class FlinkCoreConcepts {

    /**
     * 1. DataStream
     *
     * - 核心数据类型，表示无界数据流
     * - 不可变，转换操作返回新的 DataStream
     * - 支持并行处理
     */
    public static class DataStreamExample {
        DataStream<String> stream = env.fromElements("a", "b", "c");
    }

    /**
     * 2. Operator
     *
     * - 数据转换操作
     * - 包括 Source, Transform, Sink
     * - 每个 Operator 可以设置并行度
     */
    public static class OperatorExample {
        // Source: 数据输入
        // Transform: 数据转换 (map, filter, keyBy)
        // Sink: 数据输出
    }

    /**
     * 3. Job
     *
     * - 一个完整的 Flink 应用程序
     * - 包含从 Source 到 Sink 的完整拓扑
     * - 提交到集群执行
     */
    public static class JobExample {
        // 一个 Job 包含多个 Operator
        // StreamGraph -> JobGraph -> ExecutionGraph
    }

    /**
     * 4. Task
     *
     * - Operator 的一个并行实例
     * - 在 TaskManager 中执行
     * - 接收上游数据，处理后发送到下游
     */
    public static class TaskExample {
        // 如果 Operator 并行度为 4，则有 4 个 Task
    }

    /**
     * 5. Slot
     *
     * - TaskManager 的资源单位
     * - 包含 CPU 和内存
     * - 一个 Slot 可以运行多个 Task
     */
    public static class SlotExample {
        // TaskManager: 3个, 每个 4个 Slot = 12个 Slot
        // Job 并行度: 12 = 最大并行度
    }

    /**
     * 6. Parallelism
     *
     * - 数据处理的并发程度
     * - 可在多个级别设置
     * - 全局、算子、作业级别
     */
    public static class ParallelismExample {
        // env.setParallelism(4);  // 全局
        // stream.map(...).setParallelism(2);  // 算子
    }

    /**
     * 7. Checkpoint
     *
     * - 定期保存状态快照
     * - 用于故障恢复
     * - 保证精确一次语义
     */
    public static class CheckpointExample {
        // env.enableCheckpointing(60000);
    }

    /**
     * 8. Savepoint
     *
     * - 手动触发的状态快照
     * - 用于作业升级、迁移
     - 需要时恢复状态
     */
    public static class SavepointExample {
        // flink savepoint <jobId> [targetDirectory]
    }
}
```

---

## 集群架构

### 整体架构

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Flink 集群架构                                   │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      Client                                      │   │
│  │  - 提交 Job                                                      │   │
│  │  - 打包应用                                                      │   │
│  │  - 解析 DataFlow                                                │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      JobManager                                  │   │
│  │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  │   │
│  │  │ Dispatcher      │  │ Resource        │  │ JobMaster       │  │   │
│  │  │ (分派器)        │  │ Manager         │  │ (作业主)        │  │   │
│  │  │ - REST API     │  │ - 资源管理      │  │ - 作业调度      │  │   │
│  │  │ - 提交 Job     │  │ - TM 管理       │  │ - Checkpoint   │  │   │
│  │  │ - 提供 UI     │  │ - Slot 管理     │  │ - 容错         │  │   │
│  │  └─────────────────┘  └─────────────────┘  └─────────────────┘  │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                     ┌──────────────┼──────────────┐                    │
│                     ▼              ▼              ▼                    │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                      TaskManager (多个)                          │   │
│  │  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐            │   │
│  │  │ Task   │  │ Task   │  │ Task   │  │ Task   │            │   │
│  │  │ Slot 1 │  │ Slot 2 │  │ Slot 3 │  │ Slot 4 │            │   │
│  │  └─────────┘  └─────────┘  └─────────┘  └─────────┘            │   │
│  │                                                                  │   │
│  │  - 内存管理                                                     │   │
│  │  - 网络栈                                                       │   │
│  │  - Task 执行                                                    │   │
│  │  - 状态后端                                                     │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │                    外部系统                                       │   │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐  ┌───────────┐    │   │
│  │  │  HDFS     │  │  Kafka    │  │  MySQL    │  │  Redis    │    │   │
│  │  └───────────┘  └───────────┘  └───────────┘  └───────────┘    │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### JobManager 详解

```java
/**
 * JobManager 核心组件
 */
public class JobManagerComponents {

    /**
     * 1. Dispatcher (分派器)
     *
     * 职责:
     * - 接收客户端提交的 Job
     * - 启动 JobMaster
     * - 提供 REST API
     * - 提供 Web UI
     */
    public static class Dispatcher {
        // REST API 端点
        // - /jobs - 提交和管理 Job
        // - /overview - 集群概览
        // - /savepoints - 管理 Savepoint
    }

    /**
     * 2. Resource Manager (资源管理器)
     *
     * 职责:
     * - 管理 TaskManager 注册
     * - 分配和释放 Slot
     * - 与不同资源管理器交互 (YARN, K8s, Standalone)
     */
    public static class ResourceManager {
        // SlotManager
        // - 维护 Slot 状态
        // - 分配 Slot 给 JobMaster
        // - 处理 Slot 请求和释放
    }

    /**
     * 3. JobMaster (作业主)
     *
     * 职责:
     * - 执行单个 Job
     * - 调度 Task 到 TaskManager
     * - 管理 Checkpoint
     * - 处理 Task 失败
     */
    public static class JobMaster {
        // Scheduler - 任务调度
        // CheckpointCoordinator - Checkpoint 协调
        // TaskManagerGateway - 与 TM 通信
    }
}
```

### TaskManager 详解

```java
/**
 * TaskManager 详解
 *
 * 职责:
 * - 执行 Task
 * - 管理内存
 * - 网络通信
 * - 提供 Slot
 */
public class TaskManagerComponents {

    /**
     * TaskManager 启动流程
     */
    public static void startupProcess() {
        // 1. 启动 TaskManager
        // 2. 向 ResourceManager 注册
        // 3. 等待分配 Slot
        // 4. 接收并执行 Task
    }

    /**
     * Slot 管理
     */
    public static class SlotManagement {
        // 每个 Slot 包含:
        // - CPU 资源
        // - 内存资源 (Task 内存 + 托管内存)
        // - 运行时环境

        // Slot 状态:
        // - Free: 空闲
        // - Allocated: 已分配
        // - Running: 运行中
    }

    /**
     * 内存管理
     */
    public static class MemoryManagement {
        // Total Process Memory = JVM Heap + Off-Heap
        // - JVM Heap: Flink 使用
        // - Off-Heap: 托管内存 + 直接内存
    }
}
```

---

## 任务调度

### Job 执行流程

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          Job 执行流程                                     │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                          │
│  1. Client 提交 Job                                                      │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  Client                                                         │   │
│  │  - 构建 StreamGraph                                            │   │
│  │  - 优化为 JobGraph                                             │   │
│  │  - 提交到 JobManager                                           │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  2. JobManager 接收并初始化                                              │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  JobMaster                                                     │   │
│  │  - 生成 ExecutionGraph                                         │   │
│  │  - 请求 Slot                                                    │   │
│  │  - 调度 Task                                                   │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                    │                                    │
│                                    ▼                                    │
│  3. Task 执行                                                           │
│  ┌─────────────────────────────────────────────────────────────────┐   │
│  │  TaskManager                                                   │   │
│  │  - 接收 Task                                                   │   │
│  │  - 加载状态 (从 Checkpoint)                                    │   │
│  │  - 执行算子链                                                   │   │
│  │  - 发送结果到下游                                               │   │
│  └─────────────────────────────────────────────────────────────────┘   │
│                                                                          │
└─────────────────────────────────────────────────────────────────────────┘
```

### 图转换

```java
/**
 * Flink Job 图转换过程
 *
 * StreamGraph -> JobGraph -> ExecutionGraph
 */
public class GraphTransformation {

    /**
     * 1. StreamGraph (流图)
     *
     * - 用户代码直接转换
     * - 包含所有算子
     * - 未优化
     */
    public static void streamGraphExample() {
        // DataStream API 构建
        // env.fromSource(...).map(...).filter(...).print();

        // 生成 StreamGraph
        // 节点: Source, Map, Filter, Sink
        // 边: 数据流转
    }

    /**
     * 2. JobGraph (作业图)
     *
     * - StreamGraph 优化后生成
     * - 算子链 (Operator Chaining)
     * - 减少数据传输开销
     */
    public static void jobGraphExample() {
        // 算子链优化
        // 将多个算子合并为一个 Task
        // 条件: 上下游并行度相同、无重分区

        // 示例:
        // map().filter() -> 合并为一个 Task
        // 减少线程切换和网络传输
    }

    /**
     * 3. ExecutionGraph (执行图)
     *
     * - JobGraph 展开为并行版本
     * - 包含所有并行实例
     * - 描述 Task 执行关系
     */
    public static void executionGraphExample() {
        // 每个并行度对应一个 ExecutionVertex
        // Task 之间通过 IntermediateResult 通信
    }
}
```

### Slot 分配

```
┌─────────────────────────────────────────────────────────────────┐
│                    Slot 分配策略                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Slot 分配示例:                                                   │
│                                                                 │
│  TaskManager 1 (4 Slot)        TaskManager 2 (4 Slot)          │
│  ┌───────────────────┐        ┌───────────────────┐            │
│  │ Slot 1: Task 1.1  │        │ Slot 1: Task 2.1  │            │
│  │ Slot 2: Task 1.2  │        │ Slot 2: Task 2.2  │            │
│  │ Slot 3: Task 1.3  │        │ Slot 3: Task 2.3  │            │
│  │ Slot 4: Task 1.4  │        │ Slot 4: Task 2.4  │            │
│  └───────────────────┘        └───────────────────┘            │
│                                                                 │
│  分配策略:                                                        │
│                                                                 │
│  1. 尽可能均衡 (Balance)                                          │
│     - 每个 TaskManager 的 Slot 使用率相近                          │
│                                                                 │
│  2. 共享 Slot (Shared Slot)                                       │
│     - 同一 Job 的不同算子可以共享 Slot                              │
│     - 提高资源利用率                                               │
│                                                                 │
│  3. 位置感知 (Location Preference)                                │
│     - 优先分配到数据所在位置                                        │
│     - 减少网络传输                                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 状态管理

### 状态类型

```java
import org.apache.flink.api.common.state.*;
import org.apache.flink.streaming.api.functions.windowing.*;
import org.apache.flink.streaming.api.windowing.windows.*;

/**
 * Flink 状态管理详解
 *
 * 状态类型:
 * 1. Keyed State - 按 Key 分组的状态
 * 2. Operator State - 算子级别的状态
 */
public class StateManagement {

    /**
     * 1. Keyed State
     *
     * - 只能用于 KeyedStream
     * - 每个 Key 一份状态
     * - 自动按 Key 分区
     */
    public static class KeyedStateExample {

        public static void keyedStateTypes() {
            // a) ValueState - 单值状态
            // 存储单个值
            ValueState<String> valueState;

            // b) ListState - 列表状态
            // 存储元素列表
            ListState<Long> listState;

            // c) MapState - 映射状态
            // 存储 Key-Value 对
            MapState<String, Integer> mapState;

            // d) ReducingState - 归约状态
            // 自动归约的值
            ReducingState<Long> reducingState;

            // e) AggregatingState - 聚合状态
            // 自动聚合的值
            AggregatingState<Long, String> aggregatingState;
        }

        /**
         * ValueState 使用示例
         */
        public static class CountWindowFunction
                extends KeyedProcessFunction<String, String, String> {

            private ValueState<Integer> countState;

            @Override
            public void open(Configuration parameters) {
                // 获取状态句柄
                countState = getRuntimeContext().getState(
                    new ValueStateDescriptor<>("count", Integer.class)
                );
            }

            @Override
            public void processElement(String value,
                                        Context ctx,
                                        Collector<String> out) throws Exception {
                // 读取状态
                Integer currentCount = countState.value();
                if (currentCount == null) {
                    currentCount = 0;
                }

                // 更新状态
                currentCount++;
                countState.update(currentCount);

                // 输出结果
                out.collect("Key: " + value + ", Count: " + currentCount);
            }
        }
    }

    /**
     * 2. Operator State
     *
     * - 用于非 Keyed 算子
     - 需要实现 CheckpointedFunction 接口
     */
    public static class OperatorStateExample {

        public static class BufferingSink
                implements SinkFunction<Tuple2<String, Integer>>,
                CheckpointedFunction {

            private transient ListState<Tuple2<String, Integer>> checkpointedState;
            private List<Tuple2<String, Integer>> bufferedElements;

            @Override
            public void open(Configuration parameters) {
                // 初始化缓冲区
                bufferedElements = new ArrayList<>();
            }

            @Override
            public void invoke(Tuple2<String, Integer> value,
                               Context context) throws Exception {
                bufferedElements.add(value);

                // 批量写入
                if (bufferedElements.size() >= 100) {
                    for (Tuple2<String, Integer> element : bufferedElements) {
                        // 写入外部系统
                    }
                    bufferedElements.clear();
                }
            }

            @Override
            public void snapshotState(FunctionSnapshotContext context) throws Exception {
                // Checkpoint 时保存状态
                checkpointedState.clear();
                for (Tuple2<String, Integer> element : bufferedElements) {
                    checkpointedState.add(element);
                }
            }

            @Override
            public void initializeState(FunctionInitializationContext context) throws Exception {
                // 初始化或恢复状态
                ListStateDescriptor<Tuple2<String, Integer>> descriptor =
                    new ListStateDescriptor<>("buffered-elements",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

                checkpointedState = context.getOperatorStateStore().getListState(descriptor);

                // 恢复状态
                if (context.isRestored()) {
                    for (Tuple2<String, Integer> element : checkpointedState.get()) {
                        bufferedElements.add(element);
                    }
                }
            }
        }
    }
}
```

### 状态后端

```java
/**
 * 状态后端配置
 *
 * 类型:
 * 1. MemoryStateBackend - 内存存储
 * 2. FsStateBackend - 文件系统存储
 * 3. RocksDBStateBackend - RocksDB 存储
 */
public class StateBackendConfig {

    /**
     * 1. MemoryStateBackend
     *
     * 优点: 速度快
     * 缺点: 内存有限，不支持大状态
     * 适用: 开发测试
     */
    public static void memoryStateBackend() {
        env.setStateBackend(new MemoryStateBackend());
    }

    /**
     * 2. FsStateBackend
     *
     * 优点: 支持较大状态，持久化
     * 缺点: 状态在内存中，受限
     * 适用: 中等状态场景
     */
    public static void fsStateBackend() {
        env.setStateBackend(new FsStateBackend("hdfs://namenode:8020/flink/checkpoints"));
    }

    /**
     * 3. RocksDBStateBackend
     *
     * 优点: 支持超大状态，增量 Checkpoint
     * 缺点: 写入性能较低
     * 适用: 生产环境，大状态场景
     */
    public static void rocksDBStateBackend() {
        RocksDBStateBackend rocksDB = new RocksDBStateBackend(
            "hdfs://namenode:8020/flink/checkpoints",
            true  // 启用增量 Checkpoint
        );

        // 配置本地目录
        rocksDB.setDbStoragePath("/tmp/flink-rocksdb");

        // 配置内存管理
        rocksDB.setPredefinedOptions(PredefinedOptions.SPINNING_DISK_OPTIMIZED);

        env.setStateBackend(rocksDB);
    }
}
```

---

## 时间机制

### 时间语义

```
┌─────────────────────────────────────────────────────────────────┐
│                    Flink 时间语义                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Event Time (事件时间)                                        │
│                                                                 │
│  定义: 事件实际发生的时间                                         │
│  特点: 处理乱序事件，保证结果正确                                  │
│  需求: 水印 (Watermark)                                          │
│                                                                 │
│  示例:                                                          │
│  事件: {"orderId": 1, "time": 12:00:01}                        │
│  处理时间: 12:00:05                                              │
│                                                                 │
│  2. Processing Time (处理时间)                                   │
│                                                                 │
│  定义: Flink 处理事件的时间                                       │
│  特点: 简单，低延迟                                              │
│  问题: 结果不确定                                                │
│                                                                 │
│  示例:                                                          │
│  事件: {"orderId": 1, "time": 12:00:01}                        │
│  处理时间: 12:00:05                                              │
│                                                                 │
│  3. Ingestion Time (摄入时间)                                    │
│                                                                 │
│  定义: Flink 接收事件的时间                                       │
│  特点: 介于两者之间                                               │
│  不推荐使用                                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 水印机制

```java
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.watermark.Watermark;

/**
 * 水印 (Watermark) 详解
 *
 * 作用:
 * - 标记事件时间进度
 * - 触发窗口计算
 * - 处理乱序事件
 */
public class WatermarkExample {

    /**
     * 1. 固定延迟水印
     *
     * 假设事件最大延迟 N 秒
     * 水印 = 当前最大事件时间 - N
     */
    public static class FixedLatencyWatermark
            extends BoundedOutOfOrdernessTimestampExtractor<OrderEvent> {

        public FixedLatencyWatermark() {
            // 最大延迟 5 秒
            super(Time.seconds(5));
        }

        @Override
        public long extractTimestamp(OrderEvent event) {
            // 返回事件的事件时间
            return event.getTimestamp();
        }
    }

    /**
     * 2. 周期性水印
     *
     * 定期生成水印
     */
    public static void periodicWatermark() {
        // 配置水印生成间隔
        env.getConfig().setAutoWatermarkInterval(2000);  // 2秒

        // 自定义水印策略
        DataStream<OrderEvent> stream = env
            .addSource(new KafkaSource<>())
            .assignTimestampsAndWatermarks(
                new WatermarkStrategy<OrderEvent>() {
                    @Override
                    public WatermarkGenerator<OrderEvent> createWatermarkGenerator(
                            WatermarkGeneratorSupplier.Context context) {
                        return new PeriodicWatermarkGenerator();
                    }

                    @Override
                    public TimestampAssigner<OrderEvent> createTimestampAssigner(
                            TimestampAssignerSupplier.Context context) {
                        return (event, timestamp) -> event.getTimestamp();
                    }
                }
            );
    }

    /**
     * 3. 标点水印
     *
     * 每个特定事件触发水印
     */
    public static class PunctuatedWatermark
            implements WatermarkGenerator<OrderEvent> {

        private long maxTimestamp = 0;

        @Override
        public void onEvent(OrderEvent event, long eventTimestamp,
                           WatermarkOutput output) {
            // 每个特殊事件触发水印
            if (event.isSpecial()) {
                maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
                output.emitWatermark(new Watermark(maxTimestamp - 5000));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 周期性调用 (如果需要)
        }
    }
}
```

---

## 容错机制

### Checkpoint

```java
/**
 * Checkpoint 配置详解
 *
 * 容错机制:
 * - 定期保存状态快照
 * - 故障时恢复到最近快照
 * - 保证精确一次语义
 */
public class CheckpointConfig {

    /**
     * 基本配置
     */
    public static void basicConfig() {
        // 启用 Checkpoint，间隔 60秒
        env.enableCheckpointing(60000);

        // Checkpoint 模式
        // EXACTLY_ONCE: 精确一次
        // AT_LEAST_ONCE: 至少一次
        env.getCheckpointConfig().setCheckpointingMode(
            CheckpointingMode.EXACTLY_ONCE
        );

        // Checkpoint 超时时间 (10分钟)
        env.getCheckpointConfig().setCheckpointTimeout(600000);

        // 最小间隔 (5秒)
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(5000);

        // 最大并发 Checkpoint 数
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
    }

    /**
     * 高级配置
     */
    public static void advancedConfig() {
        // Checkpoint 存储
        env.getCheckpointConfig().setCheckpointStorage(
            new org.apache.flink.core.fs.Path("hdfs://namenode:8020/flink/checkpoints")
        );

        // 任务取消时保留 Checkpoint
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
            ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );

        // 启用非对齐 Checkpoint (减少反压影响)
        env.getCheckpointConfig().enableUnalignedCheckpoints();

        // 增量 Checkpoint (RocksDB)
        RocksDBStateBackend rocksDB = new RocksDBStateBackend(
            "hdfs://namenode:8020/flink/checkpoints",
            true  // 增量
        );
    }

    /**
     * 重启策略
     */
    public static void restartStrategy() {
        // 固定延迟重启
        env.setRestartStrategy(
            RestartStrategies.fixedDelayRestart(
                3,  // 最大重试次数
                Time.seconds(10)  // 重试间隔
            )
        );

        // 失败率重启
        env.setRestartStrategy(
            RestartStrategies.failureRateRestart(
                3,  // 最大失败次数
                Time.minutes(5),  // 失败率计算时间窗口
                Time.seconds(10)  // 重试间隔
            )
        );

        // 指数退避重启
        env.setRestartStrategy(
            RestartStrategies.exponentialDelayRestart(
                Time.milliseconds(1),
                Time.seconds(300),
                1.5,  // 退避因子
                Time.seconds(60),  // 最大延迟
                0.1,  // 抖动因子
                true
            )
        );
    }
}
```

### Savepoint

```bash
# ============ Savepoint 操作 ============

# 触发 Savepoint
flink savepoint <jobId> [targetDirectory]

# 使用 YARN
flink savepoint -yid <yarnAppId> <jobId> [targetDirectory]

# 从 Savepoint 恢复
flink run -s <savepointPath> [runArgs]

# 取消作业并触发 Savepoint
flink cancel -s [targetDirectory] <jobId>

# 删除 Savepoint
flink savepoint -d <savepointPath>

# 列出所有 Savepoint
ls hdfs://namenode:8020/flink/savepoints/
```

---

## 内存管理

### 内存模型

```
┌─────────────────────────────────────────────────────────────────┐
│                    Flink 内存模型                                 │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Total Process Memory                                            │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  JVM Heap (堆内存)                                        │   │
│  │  - Flink 使用: 框架 + 算子                                │   │
│  │  - TaskManager: 约 75%                                   │   │
│  │  - JobManager: 约 50%                                    │   │
│  └─────────────────────────────────────────────────────────┘   │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  Off-Heap (堆外内存)                                     │   │
│  │  - 托管内存 (Managed Memory)                             │   │
│  │    * RocksDB                                             │   │
│  │    * 排序/哈希                                           │   │
│  │  - 直接内存 (Direct Memory)                              │   │
│  │    * 网络缓冲                                            │   │
│  │    * RPC                                                 │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  配置方式:                                                        │
│                                                                 │
│  1. 托管内存比例                                                  │
│     taskmanager.memory.managed.fraction=0.4                      │
│                                                                 │
│  2. 网络内存                                                      │
│     taskmanager.memory.network.fraction=0.1                       │
│                                                                 │
│  3. 固定内存                                                      │
│     taskmanager.memory.fixed=1024m                                │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 内存配置

```bash
# ============ TaskManager 内存配置 ============

# 总进程内存
taskmanager.memory.process.size=4096m

# 总 Flink 内存
taskmanager.memory.flink.size=3584m

# JVM Heap
taskmanager.memory.heap.size=2048m

# 托管内存比例
taskmanager.memory.managed.fraction=0.4

# 网络内存比例
taskmanager.memory.network.fraction=0.1

# 最小网络内存
taskmanager.memory.network.min=64m

# 状态后端内存 (RocksDB)
taskmanager.memory.state.fraction=0.4

# ============ JobManager 内存配置 ============

taskmanager.memory.process.size=2048m
taskmanager.memory.heap.size=1024m
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Flink DataStream API](02-datastream.md) | DataStream API 详解 |
| [Flink Table API / SQL](03-table-sql.md) | Table API 和 SQL 详解 |
| [Flink 状态管理](04-state-checkpoint.md) | 状态管理和 Checkpoint |
| [Flink CEP](05-cep.md) | 复杂事件处理 |
| [Flink 运维指南](06-operations.md) | 集群部署和运维 |
