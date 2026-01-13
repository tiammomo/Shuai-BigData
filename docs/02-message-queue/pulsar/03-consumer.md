# Pulsar 消费者指南

## 目录

- [订阅模式](#订阅模式)
- [消费者配置](#消费者配置)
- [消息接收](#消息接收)
- [确认机制](#确认机制)
- [背压处理](#背压处理)

---

## 订阅模式

### Exclusive (独占)

```java
// 只有一个消费者能消费消息
Consumer<String> consumer = client.newConsumer()
    .topic("persistent://my-tenant/my-namespace/my-topic")
    .subscriptionName("my-subscription")
    .subscriptionType(SubscriptionType.Exclusive)
    .subscribe();
```

### Shared (共享)

```java
// 多个消费者共享消费，消息轮询分配
Consumer<String> consumer1 = client.newConsumer()
    .topic("persistent://my-tenant/my-namespace/my-topic")
    .subscriptionName("shared-subscription")
    .subscriptionType(SubscriptionType.Shared)
    .subscribe();

Consumer<String> consumer2 = client.newConsumer()
    .topic("persistent://my-tenant/my-namespace/my-topic")
    .subscriptionName("shared-subscription")
    .subscriptionType(SubscriptionType.Shared)
    .subscribe();
```

### Failover (故障转移)

```java
// 主备模式，主消费者故障后备用接管
Consumer<String> primaryConsumer = client.newConsumer()
    .topic("persistent://my-tenant/my-namespace/my-topic")
    .subscriptionName("failover-subscription")
    .subscriptionType(SubscriptionType.Failover)
    .subscribe();
```

### Key_Shared (按键共享)

```java
// 相同 Key 的消息由同一消费者处理
Consumer<String> consumer = client.newConsumer()
    .topic("persistent://my-tenant/my-namespace/my-topic")
    .subscriptionName("key-shared-subscription")
    .subscriptionType(SubscriptionType.Key_Shared)
    .subscribe();
```

---

## 消费者配置

### Java 客户端

```java
Consumer<String> consumer = client.newConsumer(Schema.STRING)
    .topic("persistent://my-tenant/my-namespace/my-topic")
    .subscriptionName("my-subscription")
    .consumerName("my-consumer")
    .ackTimeout(60, TimeUnit.SECONDS)         // 确认超时
    .receiverQueueSize(1000)                   // 接收队列
    .maxTotalReceiverQueueSizeAcrossPartitions(50000)  // 总队列限制
    .subscribe();

// 负数重试
consumer.negativeAcknowledge(message);
```

### 配置参数

```java
ConsumerBuilder<String> builder = client.newConsumer(Schema.STRING);

// 消费者名称
builder.consumerName("consumer-1");

// 接收队列大小
builder.receiverQueueSize(5000);

// 总接收队列限制
builder.maxTotalReceiverQueueSizeAcrossPartitions(100000);

// ACK 超时
builder.ackTimeout(30, TimeUnit.SECONDS);

// 监听 ACK 超时
builder.ackTimeoutTickTime(1, TimeUnit.SECONDS);

// 负数确认
builder.negativeAckRedeliveryDelay(1, TimeUnit.SECONDS);

// 优先级
builder.priorityLevel(0);

// 是否读取压缩消息
builder.readCompacted(true);
```

---

## 消息接收

### 同步接收

```java
// 阻塞接收
Message<String> message = consumer.receive();

// 带超时接收
Message<String> msg = consumer.receive(1, TimeUnit.SECONDS);

// 批量接收
Messages<String> messages = consumer.batchReceive();
for (Message<String> m : messages) {
    System.out.println("Received: " + m.getValue());
    consumer.acknowledge(m);
}
```

### 异步接收

```java
// 异步接收
consumer.receiveAsync().thenAccept(message -> {
    System.out.println("Received: " + message.getValue());
    consumer.acknowledgeAsync(message);
});

// 监听器模式
consumer.messageListener((consumer, msg) -> {
    try {
        System.out.println("Received: " + msg.getValue());
        consumer.acknowledge(msg);
    } catch (Exception e) {
        consumer.negativeAcknowledge(msg);
    }
});
```

### 消息属性

```java
Message<String> message = consumer.receive();

// 获取消息内容
String value = message.getValue();

// 获取消息 ID
MessageId msgId = message.getMessageId();

// 获取 Key
String key = message.getKey();

// 获取事件时间
long eventTime = message.getEventTime();

// 获取发布时间
long publishTime = message.getPublishTime();

// 获取属性
String app = message.getProperty("app");
Map<String, String> props = message.getProperties();

// 获取重试次数
int retryCount = message.getRedeliveryCount();

// 获取生产者名称
String producerName = message.getProducerName();
```

---

## 确认机制

### 单条确认

```java
Message<String> message = consumer.receive();

// 同步确认
consumer.acknowledge(message);

// 异步确认
consumer.acknowledgeAsync(message).thenRun(() -> {
    System.out.println("Message acknowledged");
});
```

### 累积确认

```java
// 确认所有消息，包括当前消息
consumer.acknowledgeCumulative(message);

// 批量累积确认
consumer.acknowledgeCumulativeAsync(message).thenRun(() -> {
    System.out.println("Cumulative ack sent");
});
```

### 负数确认

```java
// 负数确认，消息将被重新投递
consumer.negativeAcknowledge(message);

// 延迟负数确认 (消息将延迟重新投递)
consumer.negativeAcknowledge(message);  // 默认延迟 1 分钟
```

### 死信队列

```java
// 配置死信队列
Consumer<String> consumer = client.newConsumer()
    .topic("persistent://my-tenant/my-namespace/my-topic")
    .subscriptionName("my-subscription")
    .deadLetterPolicy(DeadLetterPolicy.builder()
        .maxRedeliverCount(3)
        .deadLetterTopic("persistent://my-tenant/my-namespace/dead-letter-topic")
        .build())
    .subscribe();
```

---

## 背压处理

### 接收队列控制

```java
// 设置接收队列大小
builder.receiverQueueSize(1000);

// 手动获取消息
while (true) {
    Message<String> message = consumer.receive(100, TimeUnit.MILLISECONDS);
    if (message != null) {
        processMessage(message);
        consumer.acknowledge(message);
    }
    // 检查处理速度，必要时暂停
}
```

### 流量控制

```java
// 设置流量控制阈值
builder.maxTotalReceiverQueueSizeAcrossPartitions(50000);

// 检查可用permit
consumer.getAvailablePermits();  // 查看可用permit
consumer.flow(100);              // 增加permit
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-producer.md](02-producer.md) | 生产者指南 |
| [04-functions.md](04-functions.md) | Pulsar Functions |
| [README.md](README.md) | 索引文档 |
