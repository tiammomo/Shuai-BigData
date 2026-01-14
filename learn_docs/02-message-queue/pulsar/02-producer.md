# Pulsar 生产者指南

## 目录

- [生产者配置](#生产者配置)
- [消息发送](#消息发送)
- [批量发送](#批量发送)
- [压缩策略](#压缩策略)
- [可靠性保证](#可靠性保证)

---

## 生产者配置

### Java 客户端

```java
import org.apache.pulsar.client.api.*;

PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://localhost:6650")
    .build();

Producer<String> producer = client.newProducer(Schema.STRING)
    .topic("persistent://my-tenant/my-namespace/my-topic")
    .producerName("my-producer")
    .compressionType(CompressionType.LZ4)
    .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
    .batchingMaxMessages(1000)
    .maxPendingMessages(1000)
    .sendTimeout(30, TimeUnit.SECONDS)
    .blockIfQueueFull(true)
    .create();
```

### 配置参数

```java
// 生产者配置
ProducerBuilder<T> builder = client.newProducer();

// 消息压缩
builder.compressionType(CompressionType.ZSTD);  // ZSTD, LZ4, SNAPPY, ZLIB

// 批量发送
builder.batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS);
builder.batchingMaxMessages(1000);

// 确认超时
builder.sendTimeout(30, TimeUnit.SECONDS);

// 队列满了阻塞
builder.blockIfQueueFull(true);

// 最大挂起消息
builder.maxPendingMessages(5000);

// 初始化序列号
builder.startSequenceId(0L);
```

---

## 消息发送

### 同步发送

```java
// 同步发送
MessageId msgId = producer.send("Hello Pulsar".getBytes());

// 带密钥路由
producer.newMessage()
    .key("my-key")
    .value("message content")
    .send();

// 带属性
producer.newMessage()
    .property("app", "myapp")
    .property("version", "1.0")
    .value("message with properties")
    .send();

// 带事件时间
producer.newMessage()
    .eventTime(System.currentTimeMillis())
    .value("message with event time")
    .send();
```

### 异步发送

```java
// 异步发送
CompletableFuture<MessageId> future = producer.sendAsync("async message");

future.thenAccept(msgId -> {
    System.out.println("Message sent: " + msgId);
}).exceptionally(e -> {
    System.err.println("Failed to send: " + e.getMessage());
    return null;
});

// 等待异步结果
try {
    MessageId result = producer.sendAsync("async msg").get(5, TimeUnit.SECONDS);
} catch (Exception e) {
    e.printStackTrace();
}
```

---

## 批量发送

### 配置批量

```java
Producer<String> batchProducer = client.newProducer(Schema.STRING)
    .topic("persistent://my-tenant/my-namespace/batch-topic")
    .batchingMaxPublishDelay(50, TimeUnit.MILLISECONDS)  // 50ms 内批量
    .batchingMaxMessages(10000)                           // 或 10000 条
    .maxPendingMessages(100000)                           // 最大挂起
    .enableChunking(true)                                 // 支持大消息分块
    .chunkMaxMessageSize(1000000)                         // 1MB 分块
    .create();

// 自动批量发送
for (int i = 0; i < 100000; i++) {
    batchProducer.sendAsync("batch message " + i);
}
batchProducer.flush();  // 手动刷新
```

### 批处理监控

```java
// 批处理统计
producer.getStats().getNumMsgsSent();        // 发送消息数
producer.getStats().getNumBytesSent();       // 发送字节数
producer.getStats().getBatchSizeAvg();       // 平均批量大小
producer.getStats().getSendLatencyMillisAvg(); // 平均延迟
```

---

## 压缩策略

```java
// 无压缩
builder.compressionType(CompressionType.NONE);

// LZ4 (快速)
builder.compressionType(CompressionType.LZ4);

// ZSTD (高压缩比)
builder.compressionType(CompressionType.ZSTD);

// SNAPPY
builder.compressionType(CompressionType.SNAPPY);

// ZLIB
builder.compressionType(CompressionType.ZLIB);

// 选择建议:
// - 追求速度: LZ4
// - 追求压缩率: ZSTD
// - 通用场景: LZ4
```

---

## 可靠性保证

### 发送确认

```java
// 等待所有副本确认
producer.newMessage()
    .value("reliable message")
    .send();  // 默认等待多数派确认

// 自定义确认超时
producer.newMessage()
    .sendTimeout(10, TimeUnit.SECONDS)  // 10秒超时
    .value("timeout config message")
    .send();
```

### 发送失败处理

```java
// 失败重试配置
producer.newMessage()
    .value("retry message")
    .send();

// 自定义重试
int maxRetries = 3;
for (int i = 0; i < maxRetries; i++) {
    try {
        producer.send(message);
        break;
    } catch (PulsarClientException e) {
        if (i == maxRetries - 1) throw e;
        Thread.sleep(1000 * (i + 1));  // 指数退避
    }
}
```

### 幂等发送

```java
// 生产者幂等性 (Exactly-Once)
Producer<String> idempotentProducer = client.newProducer()
    .topic("persistent://my-tenant/my-namespace/idempotent-topic")
    .producerName("idempotent-producer")
    .enableChunking(true)
    .create();

// 每条消息携带唯一 ID，由 Broker 去重
producer.newMessage()
    .sequenceId(12345678L)  // 唯一序列号
    .value("idempotent message")
    .send();
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-consumer.md](03-consumer.md) | 消费者指南 |
| [04-functions.md](04-functions.md) | Pulsar Functions |
| [README.md](README.md) | 索引文档 |
