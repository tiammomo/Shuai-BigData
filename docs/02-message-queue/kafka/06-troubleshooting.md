# Kafka 故障排查指南

## 目录

- [生产问题](#生产问题)
- [消费问题](#消费问题)
- [性能问题](#性能问题)
- [集群问题](#集群问题)
- [故障恢复](#故障恢复)
- [诊断工具](#诊断工具)

---

## 生产问题

### 问题1: 消息发送失败

**现象**: 生产者发送消息时返回错误，或超时

**诊断步骤**:

```bash
# 1. 检查生产者日志
tail -f /data/kafka/logs/producer.log

# 2. 检查网络连通性
telnet kafka1 9092
nc -zv kafka1 9092

# 3. 检查 Broker 状态
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# 4. 检查 Topic 是否存在
kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

**解决方案**:

```java
// 方案1: 增加重试次数和超时时间
Properties props = new Properties();
props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
props.put("retries", 5);
props.put("retry.backoff.ms", 1000);
props.put("request.timeout.ms", 60000);
props.put("delivery.timeout.ms", 120000);

// 方案2: 启用幂等性防止重复
props.put("enable.idempotence", true);
props.put("acks", "all");
props.put("max.in.flight.requests.per.connection", 5);

// 方案3: 处理特定异常
producer.send(record, (metadata, exception) -> {
    if (exception != null) {
        if (exception instanceof TimeoutException) {
            // 超时 - 重试
            retrySend(record);
        } else if (exception instanceof SerializationException) {
            // 序列化错误 - 修复代码
            log.error("序列化错误", exception);
        } else if (exception instanceof KafkaException) {
            // Kafka 错误 - 可能需要切换 Broker
            handleKafkaError(exception);
        }
    }
});
```

**预防措施**:

```yaml
# 1. 配置健康检查
healthCheck:
  endpoint: /health
  interval: 30s
  timeout: 5s

# 2. 配置熔断
circuitBreaker:
  failureRateThreshold: 50
  waitDurationInOpenState: 60000
  slidingWindowSize: 100

# 3. 监控指标
metrics:
  - name: send_errors_total
    threshold: 10
```

---

### 问题2: 消息丢失

**现象**: 发送成功的消息无法被消费者消费

**诊断步骤**:

```bash
# 1. 检查消息是否写入
kafka-console-consumer.sh --topic orders --from-beginning \
    --bootstrap-server localhost:9092 \
    --timeout-ms 5000 | head -100

# 2. 检查分区末端偏移量
kafka-run-class.sh kafka.tools.GetOffsetShell \
    --broker-list localhost:9092 \
    --topic orders \
    --time -1

# 3. 检查生产者 ACK 配置
# 确认 acks=all 而不是 acks=0

# 4. 检查 ISR 状态
zookeeper-shell localhost:2181 get /kafkaISRChangeHandler
```

**解决方案**:

```java
// 方案1: 使用正确的 ACK 配置
Properties props = new Properties();
props.put("acks", "all");  // 等待所有 ISR 副本确认
props.put("min.insync.replicas", 2);  // 最小 ISR 数量
props.put("retries", Integer.MAX_VALUE);
props.put("enable.idempotence", true);

// 方案2: 同步发送关键消息
for (Order order : orders) {
    ProducerRecord<String, Order> record = createRecord(order);
    try {
        Future<RecordMetadata> future = producer.send(record);
        RecordMetadata metadata = future.get(30, TimeUnit.SECONDS);  // 同步等待
        log.info("消息发送成功: offset={}", metadata.offset());
    } catch (Exception e) {
        log.error("消息发送失败", e);
        // 发送到死信队列
        sendToDeadLetterQueue(order, e);
    }
}

// 方案3: 启用生产者拦截器记录发送状态
props.put("interceptor.classes", "com.example.SendTrackingInterceptor");
```

**预防措施**:

```yaml
# 1. 死信队列配置
deadLetterQueue:
  topic: orders-dlq
  maxRetries: 3
  retryDelay: 60000

# 2. 消息持久化
persistence:
  enabled: true
  storage: "database"

# 3. 监控告警
alerts:
  - name: message_loss
    condition: rate(send_success) < rate(send_total) * 0.99
    severity: critical
```

---

### 问题3: 消息重复发送

**现象**: 消费者收到重复消息

**诊断步骤**:

```bash
# 1. 检查生产者重试日志
tail -f /data/kafka/logs/producer.log | grep retry

# 2. 检查幂等性配置
# 确认 enable.idempotence=true

# 3. 检查生产者 PID
# 每次发送检查 producer.id 是否变化

# 4. 检查消费者偏移量提交
kafka-consumer-groups.sh --describe --group order-service \
    --bootstrap-server localhost:9092
```

**解决方案**:

```java
// 方案1: 启用幂等性 (Kafka 2.0+)
Properties props = new Properties();
props.put("enable.idempotence", true);
props.put("max.in.flight.requests.per.connection", 5);  // 配合幂等性

// 方案2: 消费者端幂等处理
public class IdempotentConsumer {

    private final Set<String> processedIds = ConcurrentHashMap.newKeySet();

    public void consume(ConsumerRecord<String, Order> record) {
        Order order = record.value();

        // 检查是否已处理
        if (!processedIds.add(order.getOrderId())) {
            log.warn("重复消息，跳过: orderId={}", order.getOrderId());
            return;
        }

        // 处理消息
        processOrder(order);
    }
}

// 方案3: 数据库幂等操作
@Transactional
public void processOrder(Order order) {
    // 使用数据库唯一约束防止重复
    orderRepository.save(order);
}
```

---

## 消费问题

### 问题4: 消费者无法消费

**现象**: 消费者启动后无法拉取消息

**诊断步骤**:

```bash
# 1. 检查消费者组状态
kafka-consumer-groups.sh --describe --group order-service \
    --bootstrap-server localhost:9092

# 2. 检查分区分配
# 查看 ASSIGNING / SYNCING 状态

# 3. 检查消费者日志
tail -f /data/kafka/logs/consumer.log

# 4. 检查偏移量
# 查看 offset 是否正常提交

# 5. 检查 Topic 分区数
kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092
```

**解决方案**:

```java
// 方案1: 正确配置消费者组
Properties props = new Properties();
props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");
props.put("group.id", "order-service-group");
props.put("key.deserializer", StringDeserializer.class.getName());
props.put("value.deserializer", OrderDeserializer.class.getName());

// 禁用自动提交，手动控制
props.put("enable.auto.commit", false);
props.put("auto.offset.reset", "earliest");

// 配置分配策略
props.put("partition.assignment.strategy",
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

// 方案2: 处理 Rebalance
consumer.subscribe(Arrays.asList("orders"), new ConsumerRebalanceListener() {
    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        // 保存当前处理进度
        commitSync();
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        // 从保存的位置恢复
        for (TopicPartition partition : partitions) {
            consumer.seek(partition, loadOffset(partition));
        }
    }
});

// 方案3: 优雅处理关闭
Runtime.getRuntime().addShutdownHook(new Thread(() -> {
    log.info("收到关闭信号...");
    consumer.wakeup();  // 中断 poll()
    consumer.close(Duration.ofSeconds(30));
}));
```

---

### 问题5: 消费延迟过大

**现象**: 消费者处理速度跟不上消息产生速度

**诊断步骤**:

```bash
# 1. 检查消费者延迟
kafka-consumer-groups.sh --describe --group order-service \
    --bootstrap-server localhost:9092

# 2. 检查消息积压
kafka-consumer-groups.sh --describe --group order-service \
    --bootstrap-server localhost:9092 \
    --members

# 3. 检查处理耗时
# 查看单条消息处理时间

# 4. 检查消费者配置
# fetch.max.wait.ms, max.poll.records

# 5. 检查资源使用
top -H  # 查看线程 CPU 使用
```

**解决方案**:

```java
// 方案1: 增加消费者数量
// 分区数 = 消费者数 时吞吐量最大

// 方案2: 优化消费者配置
Properties props = new Properties();
props.put("fetch.min.bytes", 1024 * 100);  // 100KB
props.put("fetch.max.wait.ms", 500);        // 500ms
props.put("max.poll.records", 500);         // 每次 500 条
props.put("receive.buffer.bytes", 65536);   // 64KB

// 方案3: 异步处理 + 批量提交
public void consumeAsync() {
    final List<ConsumerRecord<String, Order>> buffer = new ArrayList<>();

    while (running) {
        ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));

        for (ConsumerRecord<String, Order> record : records) {
            buffer.add(record);

            if (buffer.size() >= batchSize) {
                processBatch(buffer);  // 批量处理
                consumer.commitSync();  // 批量提交
                buffer.clear();
            }
        }
    }
}

// 方案4: 增加处理线程
ExecutorService executor = Executors.newFixedThreadPool(4);

private void processBatch(List<ConsumerRecord<String, Order>> records) {
    executor.submit(() -> {
        for (ConsumerRecord<String, Order> record : records) {
            processOrder(record.value());
        }
    });
}
```

---

### 问题6: Rebalance 风暴

**现象**: 消费者频繁 Rebalance，导致无法正常消费

**诊断步骤**:

```bash
# 1. 检查 Rebalance 频率
kafka-consumer-groups.sh --describe --group order-service \
    --bootstrap-server localhost:9092

# 2. 检查消费者心跳
# 查看 session.timeout.ms 配置

# 3. 检查消费者处理时间
# 查看 max.poll.interval.ms 配置

# 4. 检查日志
tail -f /data/kafka/logs/server.log | grep Rebalance
```

**解决方案**:

```java
// 方案1: 调整会话配置
Properties props = new Properties();
props.put("session.timeout.ms", 30000);        // 会话超时
props.put("heartbeat.interval.ms", 10000);     // 心跳间隔
props.put("max.poll.interval.ms", 300000);     // 处理间隔 (5分钟)

// 方案2: 使用静态成员资格 (Kafka 2.4+)
props.put("group.instance.id", "consumer-1");  // 唯一 ID

// 方案3: 优化分配策略
props.put("partition.assignment.strategy",
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

// 方案4: 增加 Rebalance 超时
props.put("rebalance.timeout.ms", 60000);
```

**服务器端配置**:

```properties
# server.properties
# 增加会话超时
group.min.session.timeout.ms=6000
group.max.session.timeout.ms=300000
group.max.rebalance.delay.ms=300
```

---

## 性能问题

### 问题7: 生产吞吐量低

**现象**: 消息发送速度慢，无法达到预期吞吐量

**诊断步骤**:

```bash
# 1. 检查生产者指标
# 查看 batch.size, linger.ms 配置

# 2. 检查网络带宽
iftop -i eth0

# 3. 检查磁盘 IO
iostat -x 1

# 4. 检查 Broker 负载
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# 5. 测试基础吞吐量
kafka-producer-perf-test.sh \
    --topic orders \
    --num-records 1000000 \
    --throughput 100000 \
    --record-size 1000 \
    --producer-props bootstrap.servers=localhost:9092
```

**解决方案**:

```java
// 方案1: 优化生产者配置
Properties props = new Properties();
props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

// 批量发送
props.put("batch.size", 131072);  // 128KB
props.put("linger.ms", 10);        // 10ms 等待

// 缓冲区
props.put("buffer.memory", 134217728L);  // 128MB

// 压缩
props.put("compression.type", "lz4");

// 并发
props.put("max.in.flight.requests.per.connection", 10);

// 方案2: 增加分区数
// 提高并行度

// 方案3: 使用正确的序列化器
// StringSerializer vs JSONSerializer
```

**服务器端优化**:

```properties
# server.properties
# 网络线程
num.network.threads=16

# IO 线程
num.io.threads=32

# 请求队列
queued.max.requests=1000
```

---

### 问题8: 消费吞吐量低

**现象**: 消费者处理速度慢，消息积压

**诊断步骤**:

```bash
# 1. 检查消费者配置
# fetch.min.bytes, fetch.max.wait.ms

# 2. 检查消费者组状态
kafka-consumer-groups.sh --describe --group order-service

# 3. 检查资源使用
vmstat 1
iostat -x 1

# 4. 检查处理逻辑
# 分析单条消息处理时间

# 5. 测试消费者性能
kafka-consumer-perf-test.sh \
    --topic orders \
    --consumer.config consumer.properties \
    --fetch-size 1048576 \
    --messages 1000000
```

**解决方案**:

```java
// 方案1: 优化消费者配置
Properties props = new Properties();
props.put("fetch.min.bytes", 1024 * 100);  // 100KB
props.put("fetch.max.wait.ms", 300);        // 300ms
props.put("max.poll.records", 1000);        // 1000 条
props.put("receive.buffer.bytes", 65536);

// 方案2: 并行处理
ExecutorService executor = Executors.newFixedThreadPool(8);

while (true) {
    ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));

    // 并行处理不同分区
    Map<TopicPartition, List<ConsumerRecord<String, Order>>> byPartition =
        records.partitions().stream()
            .collect(Collectors.groupingBy(
                p -> p,
                Collectors.mapping(
                    records::records,
                    Collectors.toList()
                )
            ));

    List<CompletableFuture<Void>> futures = byPartition.entrySet().stream()
        .map(e -> executor.submit(() -> processPartition(e.getValue())))
        .map(f -> CompletableFuture.runAsync(() -> {
            try { f.get(); } catch (Exception e) { log.error("处理失败", e); }
        }))
        .collect(Collectors.toList());

    // 等待完成
    CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
}
```

---

### 问题9: 磁盘空间不足

**现象**: Broker 磁盘空间满，无法写入

**诊断步骤**:

```bash
# 1. 检查磁盘使用
df -h /data/kafka

# 2. 检查日志段
ls -lh /data/kafka/data/*/ |

# 3. 检查保留策略
kafka-configs.sh --describe --entity-type topics --entity-name orders

# 4. 检查日志清理
kafka-logdirs.sh --describe --broker-list 0
```

**解决方案**:

```bash
# 方案1: 调整保留策略
kafka-configs.sh --alter \
    --bootstrap-server localhost:9092 \
    --entity-type topics \
    --entity-name orders \
    --add-config retention.ms=43200000,retention.bytes=53687091200

# 方案2: 立即清理日志
kafka-logdirs.sh --broker-list 0 \
    --topic-list orders \
    --delete-dir

# 方案3: 添加新磁盘
# 配置新的 log.dirs

# 方案4: 删除旧 Topic
kafka-topics.sh --delete --topic old-topic
```

**预防措施**:

```yaml
# 1. 监控磁盘使用
alerts:
  - name: disk_usage
    condition: disk.usage.percent > 80
    severity: warning
  - name: disk_usage_critical
    condition: disk.usage.percent > 90
    severity: critical

# 2. 自动清理策略
cleanup:
  enabled: true
  policy: delete
  retention:
    time: 7d
    size: 100GB

# 3. 分区存储均衡
rebalance:
  threshold: 0.1
  interval: 1h
```

---

## 集群问题

### 问题10: Broker 无法启动

**现象**: Kafka Broker 启动失败

**诊断步骤**:

```bash
# 1. 检查日志
tail -f /data/kafka/logs/server.log

# 2. 检查端口占用
netstat -tlnp | grep 9092

# 3. 检查数据目录权限
ls -la /data/kafka/data/

# 4. 检查 ZooKeeper 连接
zkCli.sh -server localhost:2181 get /kafka/brokers/ids/0

# 5. 检查 JVM 内存
free -m
```

**解决方案**:

```bash
# 方案1: 修复数据目录权限
chown -R kafka:kafka /data/kafka
chmod 755 /data/kafka

# 方案2: 清理锁文件
rm -f /data/kafka/data/*.lock

# 方案3: 恢复元数据
# 从备份恢复 /kafka 目录

# 方案4: 增加 JVM 内存
export KAFKA_HEAP_OPTS="-Xms16g -Xmx16g"

# 方案5: 修复 ZooKeeper
# 删除无效的 Broker ID
zkCli.sh -server localhost:2181
rmr /kafka/brokers/ids/0
```

---

### 问题11: Leader 选举频繁

**现象**: 分区 Leader 频繁切换

**诊断步骤**:

```bash
# 1. 检查 Leader 变化日志
tail -f /data/kafka/logs/server.log | grep -i leader

# 2. 检查 ISR 状态
kafka-leader-election.sh --bootstrap-server localhost:9092 \
    --election-type preferred \
    --topic orders

# 3. 检查副本同步状态
kafka-replicas-per-partition.sh --topic orders \
    --bootstrap-server localhost:9092

# 4. 检查网络延迟
ping kafka1

# 5. 检查 Broker 负载
top -b -n 1 | grep java
```

**解决方案**:

```properties
# 方案1: 调整副本同步配置
# server.properties

# 增加同步超时
replica.lag.time.max.ms=30000
replica.fetch.timeout.ms=30000

# 调整复制因子
default.replication.factor=3
min.insync.replicas=2

# 减少不均衡检查
leader.imbalance.check.interval.seconds=600
leader.imbalance.per.broker.percentage=20
```

```java
// 方案2: 手动触发 Preferred Leader
kafka-leader-election.sh --bootstrap-server localhost:9092 \
    --election-type preferred \
    --topic orders \
    --partition 0

// 方案3: 检查并修复网络问题
// 联系网络团队排查
```

---

### 问题12: ZooKeeper 连接失败

**现象**: Broker 无法连接 ZooKeeper

**诊断步骤**:

```bash
# 1. 检查 ZooKeeper 状态
zkCli.sh -server localhost:2181 ruok

# 2. 检查 ZooKeeper 日志
tail -f /data/kafka/zookeeper/zookeeper.out

# 3. 检查连接数
netstat -an | grep 2181 | wc -l

# 4. 检查防火墙
iptables -L -n | grep 2181
```

**解决方案**:

```bash
# 方案1: 重启 ZooKeeper
zookeeper-server-stop.sh
sleep 5
zookeeper-server-start.sh -daemon /etc/kafka/zookeeper.properties

# 方案2: 修复 ZooKeeper 配置
# 清理快照
rm -rf /data/kafka/zookeeper/version-2/*
rm -rf /data/kafka/zookeeper/*.txn

# 方案3: 升级到 KRaft 模式 (Kafka 4.0+)
# 无需 ZooKeeper
```

---

## 故障恢复

### Broker 故障恢复

```bash
#!/bin/bash
# recover-broker.sh - Broker 故障恢复脚本

BROKER_ID=$1
BACKUP_DIR=/backup/kafka

echo "开始恢复 Broker $BROKER_ID"

# 1. 停止 Broker
ssh kafka$BROKER_ID "kafka-server-stop.sh"

# 2. 清理数据目录
ssh kafka$BROKER_ID "rm -rf /data/kafka/data/*"

# 3. 从备份恢复
rsync -av $BACKUP_DIR/broker-$BROKER_ID/data/ kafka$BROKER_ID:/data/kafka/data/

# 4. 同步元数据
# 在 ZooKeeper 中注册新 Broker
zkCli.sh -server zk1:2181 \
    create /kafka/brokers/ids/$BROKER_ID \
    '{"host":"kafka'$BROKER_ID'","port":9092,"jmx_port":9999,"timestamp":"'$(date +%s)'"}'

# 5. 启动 Broker
ssh kafka$BROKER_ID "kafka-server-start.sh -daemon /etc/kafka/server.properties"

# 6. 验证状态
sleep 10
kafka-broker-api-versions.sh --bootstrap-server kafka$BROKER_ID:9092

echo "Broker $BROKER_ID 恢复完成"
```

### 数据恢复

```bash
#!/bin/bash
# recover-data.sh - 数据恢复脚本

TOPIC=$1
BACKUP_DATE=$2
BACKUP_DIR=/backup/kafka

echo "开始恢复 Topic: $TOPIC, 日期: $BACKUP_DATE"

# 1. 恢复元数据
kafka-metadata-shell.sh --import \
    --file $BACKUP_DIR/$TOPIC/$BACKUP_DATE/metadata.txt

# 2. 创建新 Topic (如果已删除)
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic $TOPIC \
    --partitions 6 \
    --replication-factor 3

# 3. 使用 MirrorMaker 恢复数据
cat > restore-connector.json << EOF
{
    "name": "restore-connector",
    "config": {
        "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
        "topics": "$TOPIC",
        "file": "$BACKUP_DIR/$TOPIC/$BACKUP_DATE/data.txt",
        "format.class": "org.apache.kafka.connect.storage.json.JsonFormat"
    }
}
EOF

# 4. 验证恢复结果
kafka-consumer-groups.sh --describe --group restore-group \
    --bootstrap-server localhost:9092

echo "数据恢复完成"
```

### 集群恢复

```bash
#!/bin/bash
# recover-cluster.sh - 集群恢复脚本

BACKUP_DIR=/backup/kafka
DATE=$(date +%Y%m%d)

echo "开始集群恢复: $DATE"

# 1. 停止所有 Broker
for i in 1 2 3; do
    ssh kafka$i "kafka-server-stop.sh"
done

# 2. 恢复 ZooKeeper
for i in 1 2 3; do
    ssh zk$i "systemctl stop zookeeper"
    rsync -av $BACKUP_DIR/zookeeper/$i/ zk$i:/data/zookeeper/
    ssh zk$i "systemctl start zookeeper"
done

# 3. 恢复 Broker 数据
for i in 1 2 3; do
    rsync -av $BACKUP_DIR/broker-$i/data/ kafka$i:/data/kafka/data/
done

# 4. 启动 ZooKeeper
sleep 30
for i in 1 2 3; do
    ssh zk$i "systemctl status zookeeper | grep Active"
done

# 5. 启动 Broker
for i in 1 2 3; do
    ssh kafka$i "kafka-server-start.sh -daemon /etc/kafka/server.properties"
done

# 6. 验证集群状态
kafka-broker-api-versions.sh --bootstrap-server kafka1:9092

# 7. 验证 Topic 完整性
kafka-topics.sh --describe --bootstrap-server localhost:9092

echo "集群恢复完成"
```

---

## 诊断工具

### 命令行工具

```bash
# ============ 集群诊断 ============
# Broker API 版本
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# 集群元数据
kafka-metadata-shell.sh --describe

# ============ Topic 诊断 ============
# Topic 列表
kafka-topics.sh --list --bootstrap-server localhost:9092

# Topic 详情
kafka-topics.sh --describe --topic orders --bootstrap-server localhost:9092

# 分区状态
kafka-replicas-per-partition.sh --topic orders --bootstrap-server localhost:9092

# ============ 消费者诊断 ============
# 消费者组列表
kafka-consumer-groups.sh --list --bootstrap-server localhost:9092

# 消费者组详情
kafka-consumer-groups.sh --describe --group order-service \
    --bootstrap-server localhost:9092 --members --verbose

# 消费者偏移量
kafka-consumer-groups.sh --describe --group order-service \
    --bootstrap-server localhost:9092 --offsets

# ============ ACL 诊断 ============
# ACL 列表
kafka-acls.sh --list --bootstrap-server localhost:9092

# ============ 配置诊断 ============
# Broker 配置
kafka-configs.sh --describe --entity-type brokers --entity-name 0

# Topic 配置
kafka-configs.sh --describe --entity-type topics --entity-name orders
```

### JMX 诊断

```java
// 获取 JMX 指标
MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();

// Broker 指标
ObjectName name = new ObjectName("kafka.server:type=BrokerTopicMetrics,name=TotalProduceRequestsPerSec");
double produceRate = (Double) mBeanServer.getAttribute(name, "OneMinuteRate");

// 分区指标
ObjectName partitionName = new ObjectName("kafka.log:type=Log,name=LogEndOffset,topic=orders,partition=0");
long logEndOffset = (Long) mBeanServer.getAttribute(partitionName, "Value");

// 消费者指标
ObjectName consumerName = new ObjectName("kafka.consumer:type=consumer-metrics,client-id=consumer-1");
double pollRate = (Double) mBeanServer.getAttribute(consumerName, "poll-rate");
```

### 日志分析

```bash
#!/bin/bash
# analyze-logs.sh - 日志分析脚本

LOG_DIR=/data/kafka/logs
DATE=$(date +%Y%m%d)

# 1. 统计错误
echo "=== 错误统计 ==="
grep -l "ERROR" $LOG_DIR/*.log | xargs grep "ERROR" | wc -l

# 2. 统计警告
echo "=== 警告统计 ==="
grep -l "WARN" $LOG_DIR/*.log | xargs grep "WARN" | wc -l

# 3. 统计 Leader 变化
echo "=== Leader 变化 ==="
grep "Leader" $LOG_DIR/server.log | grep -v "No leader" | wc -l

# 4. 统计 Rebalance
echo "=== Rebalance ==="
grep -i "rebalance" $LOG_DIR/*.log | wc -l

# 5. 统计连接问题
echo "=== 连接问题 ==="
grep -i "connection" $LOG_DIR/*.log | grep -i "fail\|refuse\|timeout" | wc -l

# 6. 生成报告
cat > /tmp/log-report-$DATE.txt << EOF
Kafka 日志分析报告
日期: $DATE
错误数: $(grep -l "ERROR" $LOG_DIR/*.log | xargs grep "ERROR" | wc -l)
警告数: $(grep -l "WARN" $LOG_DIR/*.log | xargs grep "WARN" | wc -l)
Leader 变化: $(grep "Leader" $LOG_DIR/server.log | grep -v "No leader" | wc -l)
Rebalance: $(grep -i "rebalance" $LOG_DIR/*.log | wc -l)
连接问题: $(grep -i "connection" $LOG_DIR/*.log | grep -i "fail\|refuse\|timeout" | wc -l)
EOF

echo "报告已生成: /tmp/log-report-$DATE.txt"
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Kafka 架构详解](01-architecture.md) | Kafka 核心架构和概念 |
| [Kafka Producer 指南](02-producer.md) | 生产者 API 和配置详解 |
| [Kafka Consumer 指南](03-consumer.md) | 消费者 API 和配置详解 |
| [Kafka Streams 指南](04-streams.md) | 流处理 API 详解 |
| [Kafka 运维指南](05-operations.md) | 集群部署和运维 |
