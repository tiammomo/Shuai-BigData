# Pulsar Functions

## 目录

- [概述](#概述)
- [Java 开发](#java-开发)
- [Python 开发](#python-开发)
- [部署运行](#部署运行)
- [窗口函数](#窗口函数)

---

## 概述

Pulsar Functions 是轻量级的计算框架，支持在 Pulsar 上运行无状态函数。

### 核心特性

| 特性 | 说明 |
|------|------|
| **轻量级** | 无需独立部署 |
| **事件驱动** | 自动响应消息 |
| **容错** | 自动重试和恢复 |
| **窗口** | 支持时间/计数窗口 |

---

## Java 开发

### Maven 依赖

```xml
<dependency>
    <groupId>org.apache.pulsar</groupId>
    <artifactId>pulsar-functions-api</artifactId>
    <version>3.0.0</version>
</dependency>
```

### 函数实现

```java
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class MyFunction implements Function<String, String> {

    @Override
    public String process(String input, Context context) {
        // 处理消息
        String result = input.toUpperCase();

        // 记录日志
        context.getLogger().info("Processed: " + input);

        // 发送指标
        context.recordMetric("processed_count", 1);

        return result;
    }
}
```

### 带上下文的函数

```java
import org.apache.pulsar.functions.api.*;

public class ContextFunction implements Function<String, Void> {

    @Override
    public Void process(String input, Context context) {
        // 获取函数信息
        String funcName = context.getFunctionName();
        String tenant = context.getTenant();
        String namespace = context.getNamespace();
        String logTopic = context.getOutputTopic();

        // 获取配置
        String myConfig = context.getUserConfigValue("my-key").toString();

        // 访问状态
        // context.getState("my-state");

        // 发送输出
        context.publish("output-topic", "result: " + input);

        return null;
    }
}
```

### Key-Value 函数

```java
import org.apache.pulsar.functions.api.*;

public class KVFunction implements Function<KeyValue<String, Integer>, KeyValue<String, Integer>> {

    @Override
    public KeyValue<String, Integer> process(KeyValue<String, Integer> input, Context context) {
        String key = input.getKey();
        int value = input.getValue();

        // 处理
        return new KeyValue<>(key, value * 2);
    }
}
```

---

## Python 开发

### 函数实现

```python
from pulsar import Function

class MyPythonFunction(Function):
    def process(self, input, context):
        # 处理消息
        result = input.upper()

        # 记录日志
        context.log("Processed: %s", input)

        # 发送指标
        context.record_metric("processed_count", 1)

        return result
```

### 高级 Python 函数

```python
from pulsar import Function

class AdvancedFunction(Function):
    def process(self, input, context):
        # 获取配置
        threshold = context.get_user_config_value('threshold')

        # 获取输入属性
        app = context.get_input_property().get('app')

        # 获取当前时间
        event_time = context.get_current_wall_time()

        # 记录日志
        context.log.info(f"Processing message: {input}")

        # 发送指标
        context.record_metric('message_size', len(input))

        # 返回结果
        return f"processed: {input}"
```

---

## 部署运行

### CLI 部署

```bash
# 提交函数
bin/pulsar-admin functions create \
    --tenant public \
    --namespace default \
    --name my-function \
    --jar my-function.jar \
    --classname com.example.MyFunction \
    --input-topic my-input-topic \
    --output-topic my-output-topic \
    --log-topic my-log-topic \
    --parallelism 3

# 更新函数
bin/pulsar-admin functions update \
    --name my-function \
    --jar my-function.jar

# 删除函数
bin/pulsar-admin functions delete --name my-function

# 查看状态
bin/pulsar-admin functions status --name my-function

# 查看统计
bin/pulsar-admin functions stats --name my-function
```

### 触发函数

```bash
# 触发函数
bin/pulsar-admin functions trigger \
    --name my-function \
    --trigger-value "test message"
```

### 配置函数

```bash
# 设置资源配置
bin/pulsar-admin functions create \
    --name my-function \
    --cpu 2 \
    --ram 4096 \
    --disk 10240

# 设置窗口配置
bin/pulsar-admin functions create \
    --name window-function \
    --window-length-duration 1000 \
    --window-slide-duration 500
```

---

## 窗口函数

### 计数窗口

```java
public class CountWindowFunction implements Function<String, Void> {

    @Override
    public Void process(String input, Context context) {
        // 滑动计数窗口
        Collection<String> window = context.getWindow();

        // 聚合窗口数据
        int count = window.size();
        String result = "Window count: " + count;

        // 发送结果
        context.publish(context.getOutputTopic(), result);

        return null;
    }
}
```

### 时间窗口

```java
public class TimeWindowFunction implements Function<String, Void> {

    @Override
    public Void process(String input, Context context) {
        // 获取时间窗口
        long windowStartTime = context.getWindowStartTimestamp();
        long windowEndTime = context.getWindowEndTimestamp();

        // 聚合数据
        List<String> window = context.getWindow();
        double sum = window.stream()
            .mapToDouble(Double::parseDouble)
            .sum();

        // 发送结果
        context.publish(context.getOutputTopic(),
            String.format("window: %d-%d, sum: %.2f",
                windowStartTime, windowEndTime, sum));

        return null;
    }
}
```

### 窗口配置

```java
// 配置滑动窗口
Function function = new Function()
    .withWindowSlidingInterval(Duration.ofSeconds(10));

// 提交时配置
bin/pulsar-admin functions create \
    --name window-function \
    --window-length-duration 60000 \
    --window-slide-duration 10000
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-producer.md](02-producer.md) | 生产者指南 |
| [03-consumer.md](03-consumer.md) | 消费者指南 |
| [README.md](README.md) | 索引文档 |
