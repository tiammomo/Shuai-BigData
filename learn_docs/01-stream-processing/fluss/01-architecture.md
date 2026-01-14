# Apache Fluss 架构详解

## 目录

- [概述](#概述)
- [核心概念](#核心概念)
- [架构原理](#架构原理)
- [状态管理](#状态管理)
- [容错机制](#容错机制)

---

## 概述

Apache Fluss 是一个分布式流处理引擎，提供低延迟、高吞吐的实时数据处理能力。

### 设计目标

| 目标 | 说明 |
|------|------|
| **低延迟** | 毫秒级处理延迟 |
| **高吞吐** | 支持每秒数百万事件 |
| **精确一次** | 端到端精确一次语义 |
| **易于使用** | 友好的 API 和运维 |
| **弹性扩展** | 动态扩缩容 |

---

## 核心概念

### 核心术语

```
┌─────────────────────────────────────────────────────────────────┐
│                    Fluss 核心概念                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Job          │  作业，一个完整的流处理任务                      │
│  Task         │  任务，作业的执行单元                            │
│  Operator     │  算子，数据处理逻辑                              │
│  Source       │  数据源，数据输入                                │
│  Sink         │  数据汇，数据输出                                │
│  State        │  状态，作业维护的数据                            │
│  Checkpoint   │  检查点，状态快照                                │
│  Watermark    │  水印，事件时间进度标记                          │
│  Window       │  窗口，数据的时间/数量划分                       │
│  Parallelism  │  并行度，处理并发数                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 数据流图

```
┌─────────────────────────────────────────────────────────────────┐
│                    Fluss Job Graph                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                      Job Graph                           │   │
│  │                                                         │   │
│  │    Source(2) ──> Map(2) ──> KeyBy(4) ──> Window(4)     │   │
│  │        │            │             │             │        │   │
│  │        └────────────┴─────────────┴─────────────┘        │   │
│  │                          │                                 │   │
│  │                          ▼                                 │   │
│  │                    Sink(4)                                 │   │
│  │                                                         │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  数字表示并行度                                                 │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 架构原理

### 集群架构

```
┌─────────────────────────────────────────────────────────────────┐
│                    Fluss 集群架构                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Job Manager                           │   │
│  │  ┌───────────────────────────────────────────────────┐  │   │
│  │  │  Job Master                                       │  │   │
│  │  │  - 作业调度                                       │  │   │
│  │  │  - 资源分配                                       │  │   │
│  │  │  - 进度监控                                       │  │   │
│  │  └───────────────────────────────────────────────────┘  │   │
│  │  ┌───────────────────────────────────────────────────┐  │   │
│  │  │  Resource Manager                                 │  │   │
│  │  │  - 集群资源管理                                    │  │   │
│  │  │  - 任务分配                                       │  │   │
│  │  └───────────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                           │                                     │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Task Manager (多个)                   │   │
│  │  ┌───────────────────────────────────────────────────┐  │   │
│  │  │  Task Executor                                    │  │   │
│  │  │  - 任务执行                                       │  │   │
│  │  │  - 状态管理                                       │  │   │
│  │  │  - Checkpoint 协调                                │  │   │
│  │  └───────────────────────────────────────────────────┘  │   │
│  │  ┌───────────────────────────────────────────────────┐  │   │
│  │  │  Memory State Backend                             │  │   │
│  │  │  - 状态存储                                       │  │   │
│  │  └───────────────────────────────────────────────────┘  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 数据分区

```
┌─────────────────────────────────────────────────────────────────┐
│                    数据分区与并行                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  Topic/Stream Partitioning:                                     │
│                                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐           │
│  │ Part 0  │  │ Part 1  │  │ Part 2  │  │ Part 3  │           │
│  │  (P0)   │  │  (P0)   │  │  (P0)   │  │  (P0)   │           │
│  └────┬────┘  └────┬────┘  └────┬────┘  └────┬────┘           │
│       │            │            │            │                  │
│       ▼            ▼            ▼            ▼                  │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐  ┌─────────┐           │
│  │ Task 0  │  │ Task 1  │  │ Task 2  │  │ Task 3  │           │
│  │  (P0)   │  │  (P1)   │  │  (P2)   │  │  (P3)   │           │
│  └─────────┘  └─────────┘  └─────────┘  └─────────┘           │
│                                                                 │
│  Keyed Stream:                                                  │
│                                                                 │
│  KeyHash:    key % parallelism                                  │
│  KeyRange:   key range split                                    │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 状态管理

### 状态类型

```
┌─────────────────────────────────────────────────────────────────┐
│                    Fluss 状态类型                               │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Keyed State                           │   │
│  │  - 按 Key 隔离                                           │   │
│  │  - ValueState                                           │   │
│  │  - ListState                                            │   │
│  │  - MapState                                             │   │
│  │  - ReducingState                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    Operator State                        │   │
│  │  - 全局共享                                             │   │
│  │  - ListState                                            │   │
│  │  - UnionListState                                       │   │
│  │  - BroadcastState                                       │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │                    State Backend                         │   │
│  │  - MemoryStateBackend                                   │   │
│  │  - FsStateBackend                                       │   │
│  │  - RocksDBStateBackend                                  │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 状态使用

```java
// Keyed State
public class CountFunction extends KeyedProcessFunction<String, String, String> {

    private ValueState<Integer> countState;

    @Override
    public void open(Configuration parameters) {
        countState = getRuntimeContext().getState(
            new ValueStateDescriptor<>("count", Integer.class)
        );
    }

    @Override
    public void processElement(String value, Context ctx, Collector<String> out) {
        Integer current = countState.value();
        if (current == null) current = 0;
        countState.update(current + 1);
        out.collect("Count: " + (current + 1));
    }
}

// Operator State
public class BufferSink implements SinkFunction<String> {

    private transient ListState<String> bufferedState;

    @Override
    public void open(Configuration parameters) {
        bufferedState = getRuntimeContext().getListState(
            new ListStateDescriptor<>("buffered", String.class)
        );
    }

    @Override
    public void invoke(String value, Context context) {
        bufferedState.add(value);
    }
}
```

---

## 容错机制

### Checkpoint

```
┌─────────────────────────────────────────────────────────────────┐
│                    Checkpoint 流程                              │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. Job Manager 触发 Checkpoint                                 │
│       │                                                        │
│       ▼                                                        │
│  2. 向下游算子发送 Barrier                                      │
│       │                                                        │
│       ▼                                                        │
│  3. 算子同步状态快照                                            │
│       │                                                        │
│       ▼                                                        │
│  4. 异步快照状态到持久存储                                      │
│       │                                                        │
│       ▼                                                        │
│  5. Checkpoint 完成                                             │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 配置 Checkpoint

```java
// 启用 Checkpoint
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(60000);  // 60秒

// Checkpoint 配置
CheckpointConfig config = env.getCheckpointConfig();

// 精确一次语义
config.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

// Checkpoint 超时
config.setCheckpointTimeout(120000);  // 120秒

// 最小间隔
config.setMinPauseBetweenCheckpoints(30000);  // 30秒

// 任务取消时保留 Checkpoint
config.setRetainCheckpointsOnCancellation(true);

// 存储后端
env.setStateBackend(new RocksDBStateBackend("hdfs://namenode:9000/fluss/checkpoints"));
```

### Savepoint

```bash
# 触发 Savepoint
fluss savepoint <jobId> [targetDirectory]

# 从 Savepoint 恢复
fluss run -s <savepointPath> ...

# 取消带 Savepoint
fluss cancel <jobId> -s <targetDirectory>
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [README.md](README.md) | 索引文档 |
