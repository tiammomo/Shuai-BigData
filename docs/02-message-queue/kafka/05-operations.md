# Kafka 运维指南

## 目录

- [集群部署](#集群部署)
- [主题管理](#主题管理)
- [配置管理](#配置管理)
- [监控告警](#监控告警)
- [性能调优](#性能调优)
- [容量规划](#容量规划)
- [备份恢复](#备份恢复)
- [安全配置](#安全配置)

---

## 集群部署

### 环境准备

```bash
# 系统要求
- 操作系统: Linux (推荐 CentOS/Ubuntu)
- CPU: 8 核以上
- 内存: 32GB 以上
- 磁盘: SSD (建议 NVMe)
- 网络: 千兆网卡

# 安装 Java (Kafka 3.0+ 需要 Java 11+)
yum install java-11-openjdk-devel -y
java -version

# 创建用户
useradd kafka
passwd kafka

# 创建目录
mkdir -p /data/kafka/{data,logs}
chown -R kafka:kafka /data/kafka
```

### 集群规划

```
┌─────────────────────────────────────────────────────────────────┐
│                    Kafka 集群规划示例                             │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  集群规模: 3 节点 (最小生产配置)                                  │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  节点 1 (Master)                                         │   │
│  │  - Broker ID: 0                                         │   │
│  │  - 端口: 9092                                           │   │
│  │  - 数据目录: /data/kafka/data                           │   │
│  │  - 内存: 32GB                                           │   │
│  │  - 磁盘: 2TB SSD                                        │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  节点 2 (Slave)                                          │   │
│  │  - Broker ID: 1                                         │   │
│  │  - 端口: 9092                                           │   │
│  │  - 数据目录: /data/kafka/data                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ┌─────────────────────────────────────────────────────────┐   │
│  │  节点 3 (Slave)                                          │   │
│  │  - Broker ID: 2                                         │   │
│  │  - 端口: 9092                                           │   │
│  │  - 数据目录: /data/kafka/data                           │   │
│  └─────────────────────────────────────────────────────────┘   │
│                                                                 │
│  ZooKeeper 集群 (3 节点)                                         │
│  - 端口: 2181                                                  │
│  - 数据目录: /data/kafka/zookeeper                              │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 配置文件

```properties
# server.properties - Kafka Broker 配置

# ============ 基础配置 ============
# Broker ID (集群内唯一)
broker.id=0

# 监听器配置
listeners=PLAINTEXT://0.0.0.0:9092

# 外部访问地址 (客户端连接使用)
advertised.listeners=PLAINTEXT://kafka1:9092

# 协议映射
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SSL:SSL

# ============ 数据存储 ============
# 日志目录
log.dirs=/data/kafka/data

# 每个 Topic 分区日志段大小
log.segment.bytes=1073741824  # 1GB

# 日志保留时间
log.retention.hours=168  # 7天

# 日志保留大小
log.retention.bytes=107374182400  # 100GB

# 最小清理时间间隔
log.cleaner.min.compaction.lag.ms=0

# ============ 网络配置 ============
# 最大连接数
max.connections=1000

# 空闲连接超时
connections.max.idle.ms=600000  # 10分钟

# Socket 发送缓冲区
socket.send.buffer.bytes=102400  # 100KB

# Socket 接收缓冲区
socket.receive.buffer.bytes=102400  # 100KB

# 最大请求大小
socket.request.max.bytes=104857600  # 100MB

# ============ 副本配置 ============
# 默认复制因子
default.replication.factor=3

# 最小 ISR 数量
min.insync.replicas=2

# 副本同步最大延迟
replica.lag.time.max.ms=10000

# ============ 分区配置 ============
# 默认分区数
num.partitions=3

# 分区分配策略
log.dir.one.size=2147483648  # 2GB

# ============ ZooKeeper 配置 ============
zookeeper.connect=zk1:2181,zk2:2181,zk3:2181/kafka
zookeeper.connection.timeout.ms=6000
zookeeper.session.timeout.ms=30000

# ============ KRaft 模式 (Kafka 4.0+) ============
# process.roles=broker,controller
# node.id=0
# controller.quorum.voters=1@zk1:9093,2@zk2:9093,3@zk3:9093
# controller.listener.names=CONTROLLER
```

```properties
# zookeeper.properties - ZooKeeper 配置

# 数据目录
dataDir=/data/kafka/zookeeper

# 客户端端口
clientPort=2181

# 客户端连接数限制
maxClientCnxns=60

# 自动清理配置
autopurge.snapRetainCount=3
autopurge.purgeInterval=1

# 集群配置
server.1=zk1:2888:3888
server.2=zk2:2888:3888
server.3=zk3:2888:3888
```

### 启动脚本

```bash
#!/bin/bash
# kafka-start.sh - Kafka 启动脚本

KAFKA_HOME=/opt/kafka
CONFIG_DIR=/etc/kafka
LOG_DIR=/data/kafka/logs

# 创建日志目录
mkdir -p $LOG_DIR

# JVM 配置
export KAFKA_HEAP_OPTS="-Xms16g -Xmx16g -XX:+UseG1GC -XX:MaxGCPauseMillis=200"

# 启动 Kafka
$KAFKA_HOME/bin/kafka-server-start.sh \
    -daemon \
    $CONFIG_DIR/server.properties

# 检查状态
sleep 5
if ps aux | grep -q "[kafka.Kafka]"; then
    echo "Kafka 启动成功"
else
    echo "Kafka 启动失败"
    exit 1
fi
```

### 集群管理命令

```bash
# ============ 服务管理 ============
# 启动 Broker
kafka-server-start.sh -daemon /etc/kafka/server.properties

# 停止 Broker
kafka-server-stop.sh

# 查看运行状态
jps -l | grep Kafka

# ============ ZooKeeper ============
# 启动 ZooKeeper
zookeeper-server-start.sh -daemon /etc/kafka/zookeeper.properties

# 连接 ZooKeeper CLI
zkCli.sh -server localhost:2181

# ============ 验证集群状态 ============
# 检查 Broker 列表
kafka-broker-api-versions.sh --bootstrap-server localhost:9092

# 检查集群元数据
kafka-metadata-shell.sh --describe --entity-type brokers

# 检查控制器
zookeeper-shell localhost:2181 get /kafka/controller
```

---

## 主题管理

### 主题创建

```bash
# ============ 创建 Topic ============
# 基本创建
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --partitions 6 \
    --replication-factor 3

# 详细创建 (带配置)
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --partitions 6 \
    --replication-factor 3 \
    --config retention.ms=604800000 \
    --config cleanup.policy=delete \
    --config min.insync.replicas=2

# 创建内部 Topic (用于 Kafka Streams)
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic wordcount-output \
    --partitions 3 \
    --replication-factor 3 \
    --config cleanup.policy=compact
```

### 主题配置

```bash
# ============ 查看 Topic 配置 ============
kafka-configs.sh --describe \
    --bootstrap-server localhost:9092 \
    --entity-type topics \
    --entity-name orders

# ============ 修改 Topic 配置 ============
# 修改保留时间
kafka-configs.sh --alter \
    --bootstrap-server localhost:9092 \
    --entity-type topics \
    --entity-name orders \
    --add-config retention.ms=86400000

# 修改分区数
kafka-topics.sh --alter \
    --bootstrap-server localhost:9092 \
    --topic orders \
    --partitions 12

# 修改复制因子
kafka-reassign-partitions.sh \
    --bootstrap-server localhost:9092 \
    --reassignment-json-file reassign.json \
    --execute

# ============ 删除 Topic ============
kafka-topics.sh --delete \
    --bootstrap-server localhost:9092 \
    --topic orders

# 强制删除 (需要配置 delete.topic.enable=true)
kafka-topics.sh --delete \
    --bootstrap-server localhost:9092 \
    --topic orders
```

### 主题分区管理

```json
// reassign-partitions.json - 分区重分配配置
{
    "version": 1,
    "partitions": [
        {
            "topic": "orders",
            "partition": 0,
            "replicas": [0, 1, 2]
        },
        {
            "topic": "orders",
            "partition": 1,
            "replicas": [1, 2, 0]
        },
        {
            "topic": "orders",
            "partition": 2,
            "replicas": [2, 0, 1]
        }
    ]
}
```

```bash
# 执行分区重分配
kafka-reassign-partitions.sh \
    --bootstrap-server localhost:9092 \
    --reassignment-json-file reassign.json \
    --execute

# 验证重分配状态
kafka-reassign-partitions.sh \
    --bootstrap-server localhost:9092 \
    --reassignment-json-file reassign.json \
    --verify

# 生成最佳分区分配
kafka-reassign-partitions.sh \
    --bootstrap-server localhost:9092 \
    --topics-to-move-json-file topics.json \
    --broker-list "0,1,2" \
    --generate
```

---

## 配置管理

### Broker 配置

```properties
# 生产环境 Broker 配置示例

# ============ 内存配置 ============
# JVM 堆大小 (建议物理内存的 1/4-1/3)
KAFKA_HEAP_OPTS="-Xms32g -Xmx32g"

# G1GC 垃圾回收器
KAFKA_JVM_PERFORMANCE_OPTS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ParallelRefProcEnabled"

# ============ 网络配置 ============
# 最大连接数
max.connections=2000

# 空闲连接超时
connections.max.idle.ms=300000  # 5分钟

# 请求处理线程数
num.io.threads=16

# 网络处理线程数
num.network.threads=8

# ============ 日志配置 ============
# 日志段大小
log.segment.bytes=536870912  # 512MB

# 日志保留时间
log.retention.hours=168  # 7天

# 日志保留大小
log.retention.bytes=536870912000  # 500GB

# 清理线程数
log.cleaner.threads=4

# ============ 副本配置 ============
# 复制因子
default.replication.factor=3

# 最小 ISR
min.insync.replicas=2

# 副本同步超时
replica.fetch.timeout.ms=30000

# ============ 分区配置 ============
# 默认分区数
num.partitions=12

#Leader 选举配置
leader.imbalance.check.interval.seconds=300
leader.imbalance.per.broker.percentage=10
```

### 生产者配置

```java
// 生产环境生产者配置
Properties props = new Properties();

// 集群地址
props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

// 序列化
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

// 可靠性
props.put("acks", "all");
props.put("retries", Integer.MAX_VALUE);
props.put("retry.backoff.ms", 100);

// 幂等性 (Kafka 2.0+)
props.put("enable.idempotence", true);

// 批量发送
props.put("batch.size", 65536);  // 64KB
props.put("linger.ms", 10);

// 缓冲区
props.put("buffer.memory", 67108864L);  // 64MB

// 压缩
props.put("compression.type", "lz4");

// 超时
props.put("request.timeout.ms", 30000);
props.put("delivery.timeout.ms", 120000);
```

### 消费者配置

```java
// 生产环境消费者配置
Properties props = new Properties();

// 集群地址
props.put("bootstrap.servers", "kafka1:9092,kafka2:9092,kafka3:9092");

// 消费者组
props.put("group.id", "production-consumer-group");
props.put("group.instance.id", "consumer-1");  // 静态成员资格

// 反序列化
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

// 提交配置
props.put("enable.auto.commit", false);
props.put("auto.offset.reset", "earliest");

// 拉取配置
props.put("fetch.min.bytes", 1024 * 50);  // 50KB
props.put("fetch.max.wait.ms", 500);
props.put("max.poll.records", 500);

// 会话配置
props.put("session.timeout.ms", 30000);
props.put("heartbeat.interval.ms", 10000);
props.put("max.poll.interval.ms", 300000);

// 分配策略
props.put("partition.assignment.strategy",
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
```

---

## 监控告警

### JMX 指标

```bash
# 启用 JMX
export JMX_PORT=9999
export KAFKA_JMX_OPTS="-Dcom.sun.management.jmxremote \
    -Dcom.sun.management.jmxremote.port=9999 \
    -Dcom.sun.management.jmxremote.ssl=false \
    -Dcom.sun.management.jmxremote.authenticate=true"

# 关键指标 (通过 JMX 或 Metrics API)

# Broker 指标
kafka.server:name=TotalFetchRequestsPerSec,type=RequestMetrics
kafka.server:name=TotalProduceRequestsPerSec,type=RequestMetrics
kafka.server:name=BytesInPerSec,type=BrokerTopicMetrics
kafka.server:name=BytesOutPerSec,type=BrokerTopicMetrics

# 分区指标
kafka.log:name=LogEndOffset,type=Log,topic=.*,partition=.*
kafka.log:name=LogStartOffset,type=Log,topic=.*,partition=.*
kafka.log:name=NumLogSegments,type=Log,topic=.*,partition=.*

# 副本指标
kafka.server:name=UnderReplicatedPartitions,type=ReplicaManager
kafka.server:name=IsrExpandsPerSec,type=ReplicaManager
kafka.server:name=IsrShrinksPerSec,type=ReplicaManager

# 消费者组指标
kafka.consumer:name=records-consumed-total,type=consumer-metrics
kafka.consumer:name=records-lag-max,type=consumer-metrics
```

### Prometheus 监控

```yaml
# prometheus.yml - Prometheus 配置
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'kafka'
    static_configs:
      - targets: ['kafka1:9100', 'kafka2:9100', 'kafka3:9100']
    metrics_path: /metrics

  - job_name: 'kafka-exporter'
    static_configs:
      - targets: ['kafka-exporter:9308']
```

```yaml
# kafka-exporter - Kubernetes 部署
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kafka-exporter
spec:
  replicas: 1
  selector:
    matchLabels:
      app: kafka-exporter
  template:
    metadata:
      labels:
        app: kafka-exporter
      annotations:
        prometheus.io/scrape: "true"
        prometheus.io/port: "9308"
    spec:
      containers:
      - name: kafka-exporter
        image: danielqsj/kafka_exporter:latest
        ports:
        - containerPort: 9308
        env:
        - name: KAFKA_SERVERS
          value: "kafka1:9092,kafka2:9092,kafka3:9092"
```

### Grafana 仪表板

```json
{
  "dashboard": {
    "title": "Kafka 监控仪表板",
    "panels": [
      {
        "title": "消息吞吐量",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(kafka_server_brokertopicmetric_messagesin_total{job='kafka'}[5m])",
            "legendFormat": "{{instance}}"
          }
        ]
      },
      {
        "title": "字节速率",
        "type": "graph",
        "targets": [
          {
            "expr": "rate(kafka_server_brokertopicmetric_bytesin_total{job='kafka'}[5m])",
            "legendFormat": "{{instance}} - In"
          },
          {
            "expr": "rate(kafka_server_brokertopicmetric_bytesout_total{job='kafka'}[5m])",
            "legendFormat": "{{instance}} - Out"
          }
        ]
      },
      {
        "title": "未复制分区",
        "type": "stat",
        "targets": [
          {
            "expr": "kafka_server_replicamanager_underreplicatedpartitions{job='kafka'}",
            "legendFormat": "{{instance}}"
          }
        ]
      },
      {
        "title": "消费者延迟",
        "type": "graph",
        "targets": [
          {
            "expr": "kafka_consumer_group_current_offset{job='kafka'}",
            "legendFormat": "{{group}} - {{topic}}"
          }
        ]
      }
    ]
  }
}
```

### 告警规则

```yaml
# alert-rules.yml - Prometheus 告警规则
groups:
  - name: kafka-alerts
    rules:
      # Broker 宕机
      - alert: KafkaBrokerDown
        expr: kafka_server_replicamanager_underreplicatedpartitions > 0
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "Kafka Broker 不可用"
          description: "Kafka Broker {{ $labels.instance }} 未响应"

      # 未复制分区
      - alert: UnderReplicatedPartitions
        expr: kafka_server_replicamanager_underreplicatedpartitions > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "存在未复制分区"
          description: "Topic {{ $labels.topic }} 有 {{ $value }} 个未复制分区"

      # ISR 不足
      - alert: IsrShrinks
        expr: increase(kafka_server_replicamanager_isrshrinkspersec_total[5m]) > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "ISR 频繁收缩"
          description: "ISR 收缩次数过多"

      # 消费者延迟过大
      - alert: ConsumerLagHigh
        expr: kafka_consumer_group_lag > 10000
        for: 10m
        labels:
          severity: warning
        annotations:
          summary: "消费者延迟过高"
          description: "消费者组 {{ $labels.consumergroup }} 延迟 {{ $value }} 条消息"

      # 请求延迟过高
      - alert: RequestLatencyHigh
        expr: kafka_network_requestmetrics_totaltimems > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "请求延迟过高"
          description: "请求延迟超过 1 秒"
```

---

## 性能调优

### 磁盘调优

```bash
# ============ 文件系统配置 ============
# 使用 XFS 或 ext4
mkfs.xfs /dev/sdb -f
mount /dev/sdb /data/kafka

# 挂载选项 (noatime 禁用访问时间更新)
mount -o noatime /dev/sdb /data/kafka

# ============ IO 调度器 ============
# 查看当前调度器
cat /sys/block/sda/queue/scheduler

# 设置为 deadline 或 noop (SSD)
echo deadline > /sys/block/sda/queue/scheduler

# ============ 读写优化 ============
# 预读设置
blockdev --setra 4096 /dev/sda

# 写回缓存
echo 1 > /sys/block/sda/queue/nr_requests

# ============ 文件描述符 ============
# 增加文件描述符限制
ulimit -n 1000000
```

### 网络调优

```bash
# ============ 网络缓冲区 ============
# 发送缓冲区
sysctl -w net.core.wmem_max=134217728
sysctl -w net.core.wmem_default=2097152

# 接收缓冲区
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.rmem_default=2097152

# TCP 缓冲区
sysctl -w net.ipv4.tcp_wmem="2097152 4194304 134217728"
sysctl -w net.ipv4.tcp_rmem="2097152 4194304 134217728"

# ============ 连接优化 ============
# TIME_WAIT 复用
sysctl -w net.ipv4.tcp_tw_reuse=1

# 快速回收
sysctl -w net.ipv4.tcp_fin_timeout=15

# 连接队列
sysctl -w net.core.somaxconn=65535
```

### JVM 调优

```bash
# JVM 选项 (KAFKA_HEAP_OPTS)
export KAFKA_HEAP_OPTS="-Xms32g -Xmx32g \
    -XX:+UseG1GC \
    -XX:MaxGCPauseMillis=200 \
    -XX:InitiatingHeapOccupancyPercent=35 \
    -XX:G1HeapRegionSize=16m \
    -XX:MetaspaceSize=256m \
    -XX:MaxMetaspaceSize=256m \
    -XX:+ParallelRefProcEnabled \
    -XX:+UnlockExperimentalVMOptions \
    -XX:G1NewSizePercent=30 \
    -XX:G1MaxNewSizePercent=40"

# GC 日志
export KAFKA_GC_LOG_OPTS="-Xlog:gc*:file=/data/kafka/logs/gc.log:time,uptime,level,tags:filecount=10,filesize=100M"
```

---

## 容量规划

### 磁盘容量计算

```
┌─────────────────────────────────────────────────────────────────┐
│                    磁盘容量规划                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  输入参数:                                                       │
│  - 日消息量: 100万条/天                                          │
│  - 平均消息大小: 1KB                                             │
│  - 保留时间: 7天                                                 │
│  - 复制因子: 3                                                   │
│  - 日志段大小: 1GB                                               │
│                                                                 │
│  计算公式:                                                       │
│  每日存储 = 日消息量 × 平均消息大小                               │
│           = 1,000,000 × 1KB = 1GB                               │
│                                                                 │
│  原始数据 = 每日存储 × 保留天数                                   │
│           = 1GB × 7 = 7GB                                       │
│                                                                 │
│  复制后数据 = 原始数据 × 复制因子                                 │
│            = 7GB × 3 = 21GB                                     │
│                                                                 │
│  额外开销 = 复制后数据 × 20%                                      │
│          = 21GB × 0.2 = 4.2GB                                   │
│                                                                 │
│  总磁盘需求 = 复制后数据 + 额外开销                               │
│            = 21GB + 4.2GB = 25.2GB                              │
│                                                                 │
│  建议配置: 50GB SSD × 3 (RAID/分布式)                            │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 分区规划

```
┌─────────────────────────────────────────────────────────────────┐
│                    分区数规划                                     │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  分区数影响因素:                                                 │
│  1. 吞吐量需求                                                   │
│     - 单分区吞吐量: ~10MB/s (单消费者)                           │
│     - 所需分区数 = 峰值吞吐量 / 单分区吞吐量                      │
│                                                                 │
│  2. 并行度                                                       │
│     - 消费者数 ≤ 分区数                                          │
│     - 建议: 分区数 = 消费者数 × 2~3                              │
│                                                                 │
│  3. 未来扩展                                                     │
│     - 分区数只能增加，不能减少                                    │
│     - 建议预留 20-30%                                            │
│                                                                 │
│  示例:                                                           │
│  - 峰值吞吐量: 100MB/s                                           │
│  - 单分区吞吐量: 10MB/s                                          │
│  - 消费者数: 20                                                  │
│                                                                 │
│  计算:                                                           │
│  - 最小分区数 = 100 / 10 = 10                                   │
│  - 建议分区数 = max(10, 20) × 2 = 40                            │
│  - 预留分区数 = 40 × 1.3 = 52                                   │
│                                                                 │
│  最终配置: 64 分区                                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 内存规划

```
┌─────────────────────────────────────────────────────────────────┐
│                    内存规划                                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  内存分配:                                                       │
│  - JVM 堆: 32GB (用于缓存、分区副本)                             │
│  - 操作系统缓存: 32GB (页缓存，加速读写)                          │
│  - RocksDB: 8GB (状态存储)                                       │
│  - 其他: 16GB                                                   │
│                                                                 │
│  总计: 88GB                                                     │
│                                                                 │
│  建议: 128GB 内存                                               │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

---

## 备份恢复

### 主题备份

```bash
#!/bin/bash
# backup-topic.sh - 主题备份脚本

TOPIC=$1
BACKUP_DIR=/backup/kafka
DATE=$(date +%Y%m%d)

# 创建备份目录
mkdir -p $BACKUP_DIR/$TOPIC/$DATE

# 备份元数据
kafka-metadata-shell.sh --describe \
    --entity-type topics \
    --entity-name $TOPIC > $BACKUP_DIR/$TOPIC/$DATE/metadata.txt

# 使用 Kafka Connect 备份
cat > sink-connector.json << EOF
{
    "name": "backup-sink",
    "config": {
        "connector.class": "org.apache.kafka.connect.file.FileStreamSinkConnector",
        "topics": "$TOPIC",
        "file": "$BACKUP_DIR/$TOPIC/$DATE/data.txt",
        "format.class": "org.apache.kafka.connect.storage.json.JsonFormat"
    }
}
EOF
```

### 恢复主题

```bash
#!/bin/bash
# restore-topic.sh - 主题恢复脚本

BACKUP_FILE=$1
TOPIC="restored-"$(basename $BACKUP_FILE .txt)

# 创建新主题
kafka-topics.sh --create \
    --bootstrap-server localhost:9092 \
    --topic $TOPIC \
    --partitions 6 \
    --replication-factor 3

# 使用 Kafka Connect 恢复
cat > source-connector.json << EOF
{
    "name": "restore-source",
    "config": {
        "connector.class": "org.apache.kafka.connect.file.FileStreamSourceConnector",
        "file": "$BACKUP_FILE",
        "format.class": "org.apache.kafka.connect.storage.json.JsonFormat",
        "topic": "$TOPIC"
    }
}
EOF
```

### 跨集群复制

```properties
# mirror-maker.properties - 跨集群复制配置

# 源集群
bootstrap.servers=source-kafka1:9092,source-kafka2:9092,source-kafka3:9092

# 目标集群
dest.bootstrap.servers=dest-kafka1:9092,dest-kafka2:9092,dest-kafka3:9092

# 要复制的 Topic 列表
whitelist=topic1,topic2,topic3

# 复制组
consumer.group.id=mirror-maker-group

# 并行度
tasks.max=10

# 复制因子
replication.factor=3

# 同步复制
sync.topic.acks.enabled=true
```

```bash
# 启动 MirrorMaker
kafka-mirror-maker.sh \
    --consumer.config source-consumer.properties \
    --producer.config dest-producer.properties \
    --whitelist "orders,payments" \
    --num.streams 4
```

---

## 安全配置

### ACL 权限控制

```bash
# ============ 授权 CLI ============

# 查看 ACL
kafka-acls.sh --list \
    --bootstrap-server localhost:9092 \
    --principal User:alice

# 添加生产者权限
kafka-acls.sh --authorizer-properties \
    zookeeper.connect=localhost:2181 \
    --add \
    --allow-principal User:producer \
    --operation Write \
    --topic orders

# 添加消费者权限
kafka-acls.sh --authorizer-properties \
    zookeeper.connect=localhost:2181 \
    --add \
    --allow-principal User:consumer \
    --operation Read \
    --group order-service-group \
    --topic orders

# 删除权限
kafka-acls.sh --authorizer-properties \
    zookeeper.connect=localhost:2181 \
    --remove \
    --allow-principal User:alice \
    --operation Read \
    --topic orders
```

### SASL 认证

```properties
# server.properties - SASL 配置
listeners=PLAINTEXT://0.0.0.0:9092,SASL_PLAINTEXT://0.0.0.0:9093
listener.security.protocol.map=PLAINTEXT:PLAINTEXT,SASL_PLAINTEXT:SASL_PLAINTEXT,SSL:SSL

# SASL 机制
sasl.mechanism.inter.broker.protocol=PLAIN
sasl.enabled.mechanisms=PLAIN

# JAAS 配置
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="admin" \
    password="admin-secret" \
    user_admin="admin-secret" \
    user_alice="alice-secret";
```

```properties
# producer.properties - 带认证的生产者
bootstrap.servers=kafka1:9093,kafka2:9093,kafka3:9093
security.protocol=SASL_PLAINTEXT
sasl.mechanism=PLAIN
sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required \
    username="producer" \
    password="producer-secret";
```

### SSL 加密

```bash
# 生成 SSL 证书
keytool -keystore kafka.keystore.jks -alias kafka -keyalg RSA \
    -storepass keystorepass -keypass keypass \
    -dname "CN=kafka1" -genkey

# 创建 CA
openssl req -new -x509 -keyout ca-key -out ca-cert \
    -days 365 -subj "/CN=CA"

# 签名证书
keytool -keystore kafka.keystore.jks -alias kafka \
    -certreq -file cert-file | \
    openssl x509 -req -CA ca-cert -CAkey ca-key \
    -in cert-file -out cert-signed -days 365 \
    -CAcreateserial

# 导入证书
keytool -keystore kafka.keystore.jks -alias CARoot \
    -import -file ca-cert
keytool -keystore kafka.keystore.jks -alias kafka \
    -import -file cert-signed
```

```properties
# server.properties - SSL 配置
listeners=SSL://0.0.0.0:9094
ssl.keystore.location=/etc/kafka/ssl/kafka.keystore.jks
ssl.keystore.password=keystorepass
ssl.key.password=keypass
ssl.truststore.location=/etc/kafka/ssl/kafka.truststore.jks
ssl.truststore.password=truststorepass
ssl.client.auth=required
```

---

## 最佳实践

### 生产者最佳实践

```java
/**
 * 生产者最佳实践总结
 */
public class ProducerBestPractices {

    public static Properties optimalConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());

        // ============ 可靠性配置 (重要) ============
        // 确认机制: all 确保消息不丢失
        props.put("acks", "all");

        // 重试次数: 设置较大值保证可靠性
        props.put("retries", Integer.MAX_VALUE);
        props.put("retry.backoff.ms", 100);

        // 启用幂等性 (Kafka 0.11.0+)
        // 防止因重试导致的消息重复
        props.put("enable.idempotence", true);
        props.put("max.in.flight.requests.per.connection", 5);

        // ============ 性能优化配置 ============
        // 批量发送
        props.put("batch.size", 32768);        // 32KB 批量大小
        props.put("linger.ms", 5);             // 5ms 等待时间

        // 缓冲内存
        props.put("buffer.memory", 67108864L); // 64MB

        // 压缩
        props.put("compression.type", "lz4");  // lz4 压缩比和速度平衡好

        // ============ 超时配置 ============
        props.put("request.timeout.ms", 30000);
        props.put("delivery.timeout.ms", 120000);  // 发送总超时

        return props;
    }

    /**
     * 生产者使用建议
     */
    public static void usageTips() {
        // 1. 复用 Producer 实例
        // Producer 是线程安全的，应在应用中共享实例

        // 2. 使用异步发送提高吞吐量
        // 同步发送适用于需要保证顺序的场景

        // 3. 合理设置分区数
        // 分区数决定了并行度，通常设置为 Broker 数量的 2-4 倍

        // 4. 监控关键指标
        // - record-send-rate: 发送速率
        // - request-latency-ms: 请求延迟
        // - record-retry-rate: 重试率
        // - record-error-rate: 错误率

        // 5. 处理异常
        // - 可重试异常: 网络超时、Leader 选举等
        // - 不可重试异常: 消息过大、认证失败等
    }
}
```

### 消费者最佳实践

```java
/**
 * 消费者最佳实践总结
 */
public class ConsumerBestPractices {

    public static Properties optimalConfig() {
        Properties props = new Properties();

        // ============ 基础配置 ============
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "consumer-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());

        // ============ 消费策略 ============
        // 从最早位置开始消费
        props.put("auto.offset.reset", "earliest");

        // ============ 提交策略 ============
        // 根据业务需求选择
        // 自动提交: 简单但可能丢失消息
        // 手动提交: 精确控制但复杂
        props.put("enable.auto.commit", false);
        props.put("auto.commit.interval.ms", 5000);

        // ============ 拉取配置 ============
        props.put("max.poll.records", 500);          // 每次拉取记录数
        props.put("fetch.min.bytes", 1);             // 最小拉取大小
        props.put("fetch.max.wait.ms", 500);         // 最大等待时间
        props.put("fetch.max.bytes", 52428800);      // 最大拉取大小 50MB

        // ============ 会话配置 ============
        props.put("session.timeout.ms", 30000);
        props.put("heartbeat.interval.ms", 10000);

        // ============ 超时配置 ============
        props.put("request.timeout.ms", 40000);
        props.put("max.poll.interval.ms", 300000);   // 5分钟处理超时

        return props;
    }

    /**
     * 消费者使用建议
     */
    public static void usageTips() {
        // 1. 合理设置分区数和消费者数
        // 消费者数 <= 分区数
        // 分区数 = 预期吞吐量 / 单分区吞吐量

        // 2. 处理消息的时间要控制
        // 避免超过 max.poll.interval.ms

        // 3. 手动提交 Offset 的时机
        // - 消息处理成功后提交
        // - 使用批量提交减少提交频率

        // 4. 幂等处理
        // 消费者可能收到重复消息，业务层要处理

        // 5. 监控消费延迟
        // consumer-lag: 消费延迟
        // records-lag-max: 最大延迟分区

        // 6. 优雅关闭
        // 使用 try-with-resources 或 finally 块
    }
}
```

### 集群配置最佳实践

```java
/**
 * Broker 集群配置最佳实践
 */
public class BrokerBestPractices {

    public static void main(String[] args) {
        // Broker 关键配置

        // ============ 网络配置 ============
        // 网络处理线程数
        int numNetworkThreads = Runtime.getRuntime().availableProcessors() * 2;

        // I/O 处理线程数
        int numIoThreads = Runtime.getRuntime().availableProcessors() * 2;

        // ============ 日志配置 ============
        // 日志目录，多个目录用逗号分隔
        String logDirs = "/data/kafka/logs1,/data/kafka/logs2";

        // 每个分区的日志段大小
        long segmentBytes = 1073741824L;  // 1GB

        // ============ 内存配置 ============
        // JVM 堆大小
        // 通常设置为 4GB-8GB，不宜过大
        int heapSizeMB = 4096;

        // ============ 复制配置 ============
        // 默认副本因子
        short defaultReplicationFactor = 3;

        // 最少同步副本数
        int minInsyncReplicas = 2;

        // ============ 清理配置 ============
        // 消息保留时间
        long logRetentionHours = 168;  // 7天

        // 消息保留大小
        long logRetentionBytes = 100_000_000_000L;  // 100GB

        // 段文件检查间隔
        long logRollHours = 168;  // 7天

        // ============ 压缩配置 ============
        // 压缩类型
        String compressionType = "lz4";

        // ============ 副本管理 ============
        // 副本Fetcher线程数
        int replicaFetcherThreads = 4;

        // Leader 选举策略
        // - unclean.leader.election.enable: 是否允许非 ISR 副本选举为 Leader
        // 生产环境建议设置为 false，避免数据不一致
        boolean uncleanLeaderElection = false;
    }
}
```

### 监控指标

| 指标类型 | 指标名称 | 说明 |
|---------|---------|------|
| **生产者** | record-send-rate | 消息发送速率 (消息数/秒) |
| **生产者** | byte-send-rate | 字节发送速率 (字节/秒) |
| **生产者** | request-latency-ms | 请求平均延迟 (毫秒) |
| **生产者** | record-retry-rate | 请求重试率 |
| **生产者** | record-error-rate | 记录错误率 |
| **消费者** | records-consumed-rate | 消息消费速率 (消息数/秒) |
| **消费者** | bytes-consumed-rate | 字节消费速率 (字节/秒) |
| **消费者** | records-lag-max | 消费延迟 (消息数) |
| **Broker** | under-replicated-partitions | 落后副本数 |
| **Broker** | isr-shrinks | ISR 收缩次数 |
| **Broker** | leader-elections | Leader 选举次数 |

---

## 常见问题

### Q1: 消息丢失 (Message Loss)?

**现象**: 消费者收不到消息，消息计数不完整

**诊断步骤**:
```bash
# 1. 检查生产者发送配置
# 查看 acks 参数设置

# 2. 检查 Broker 副本状态
kafka-topics.sh --describe --topic your-topic --bootstrap-server localhost:9092

# 3. 查看副本同步状态
# UnderReplicatedPartitions = 0 ?

# 4. 检查消费者偏移量
kafka-consumer-groups.sh --describe --group your-group --bootstrap-server localhost:9092
```

**解决方案**:
```java
// 1. 配置正确的 acks (必须!)
props.put("acks", "all");  // 等待所有 ISR 副本确认

// 2. 设置合适的复制因子 (生产环境至少 3)
props.put("replication.factor", 3);

// 3. 使用同步发送 (关键消息)
RecordMetadata metadata = producer.send(record).get();  // 阻塞等待确认

// 4. 启用幂等性 (Kafka 0.11+)
props.put("enable.idempotence", true);
props.put("max.in.flight.requests", 5);  // 幂等性要求 <= 5
props.put("retries", Integer.MAX_VALUE);
```

**预防措施**:
- 监控 `under-replicated-partitions` 指标
- 监控 `isr-shrinks` 指标 (应接近 0)
- 设置告警: `under-replicated-partitions > 0`

---

### Q2: 消息重复 (Message Duplication)?

**现象**: 消费者收到重复消息，数据计数超出预期

**解决方案**:
```java
// ========== 生产者端 ==========
// 1. 启用幂等性 (推荐)
props.put("enable.idempotence", true);
props.put("max.in.flight.requests", 5);  // 必须 <= 5

// 2. 使用事务 (精确一次语义)
producer.initTransactions();
try {
    producer.beginTransaction();
    producer.send(record);
    producer.commitTransaction();
} catch (Exception e) {
    producer.abortTransaction();
}

// ========== 消费者端 ==========
// 1. 手动提交偏移量
props.put("enable.auto.commit", false);  // 禁用自动提交

// 2. 幂等处理消息 (去重表)
String messageId = extractMessageId(record.value());
if (redis.exists("kafka:processed:" + messageId)) {
    continue;  // 已处理，跳过
}
// 业务处理...
redis.opsForValue().set("kafka:processed:" + messageId, "1",
    Duration.ofHours(24));
```

---

### Q3: 消费者 lag 过大 (High Consumer Lag)?

**现象**: 消费延迟增加，消息堆积

**解决方案**:
```java
// 1. 增加消费者实例 (不超过分区数)
int partitionCount = 3;
int consumerCount = 3;

// 2. 增加分区数
kafka-topics.sh --alter --topic your-topic --partitions 6 --bootstrap-server localhost:9092

// 3. 优化消费者配置
props.put("max.poll.records", 1000);
props.put("session.timeout.ms", 30000);
props.put("heartbeat.interval.ms", 10000);

// 4. 批量处理
public void batchProcess(ConsumerRecords<String, String> records) {
    List<Request> requests = records.stream()
        .map(r -> parseRecord(r))
        .collect(Collectors.toList());
    jdbcTemplate.batchUpdate("INSERT INTO table VALUES (?, ?)", ...);
}
```

**预防措施**:
- 监控 `records-lag-max` 指标
- 设置告警: `lag > 10000`

---

### Q4: 分区分配不均 (Uneven Partition Assignment)?

**现象**: 部分消费者负载高，部分消费者空闲

**解决方案**:
```java
// 1. 选择合适的分区分配策略
props.put("partition.assignment.strategy",
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");

// 2. 避免热点分区
// 错误的 Key 设计: 自增 ID
// 正确的 Key 设计: 使用 UUID 或哈希

// 3. 确保消费者数量与分区数匹配
if (consumerCount > partitionCount) {
    // 消费者多于分区，浪费资源
}
```

---

### Q5: Leader 选举频繁 (Frequent Leader Elections)?

**现象**: Broker 频繁上下线，Leader 切换日志增多

**解决方案**:
```properties
# server.properties
unclean.leader.election.enable = false
zookeeper.session.timeout.ms = 30000

# JVM 堆内存
KAFKA_HEAP_OPTS="-Xmx4G -Xms4G"

# 线程数
num.io.threads = 8
num.network.threads = 8
```

**预防措施**:
- 监控 `leader-elections` 指标
- 设置告警: `leader-elections > 5/小时`

---

### Q6: 内存不足 (Out of Memory)?

**现象**: Producer 阻塞，Consumer 无法拉取数据

**解决方案**:
```java
// 生产者端
props.put("buffer.memory", 134217728L);  // 128MB
props.put("max.block.ms", 10000);

// 消费者端
props.put("max.poll.records", 100);

// Broker 端
# JVM 堆内存
KAFKA_HEAP_OPTS="-Xmx4G -Xms4G"
```

**预防措施**:
- 监控 `buffer-memory-av` 指标
- 设置告警: `buffer-available < 10%`

---

### Q7: Producer 发送超时 (Producer Timeout)?

**现象**: `TimeoutException`, 消息发送失败

**解决方案**:
```java
// 1. 增加超时时间
props.put("request.timeout.ms", 30000);
props.put("delivery.timeout.ms", 120000);

// 2. 配置重试
props.put("retries", 3);
props.put("retry.backoff.ms", 1000);

// 3. 启用压缩
props.put("compression.type", "lz4");
```

---

### Q8: Consumer Rebalance 问题?

**现象**: 消费者频繁 rebalance，无法稳定消费

**解决方案**:
```java
// 1. 合理设置会话超时
props.put("session.timeout.ms", 30000);

// 2. 合理设置心跳间隔
props.put("heartbeat.interval.ms", 10000);

// 3. 增加最大 poll 间隔
props.put("max.poll.interval.ms", 300000);

// 4. 使用 CooperativeStickyAssignor
props.put("partition.assignment.strategy",
    "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
```

**预防措施**:
- 监控 `rebalance-count-per-hour` 指标
- 设置告警: `rebalance-count > 5/小时`

---

### Q9: Kafka 集群扩展问题?

**现象**: 新 Broker 加入后数据不均衡

**解决方案**:
```bash
# 1. 手动触发分区重分配
kafka-reassign-partitions.sh --reassignment-json-file reassign.json \
    --execute --bootstrap-server localhost:9092

# 2. 启用自动平衡
auto.leader.rebalance.enable = true
leader.imbalance.check.interval.seconds = 300

# 3. 使用 Preferred Replica Leader Election
kafka-preferred-replica-election.sh --bootstrap-server localhost:9092
```

---

### Q10: 消息顺序乱序 (Message Reordering)?

**现象**: 消息顺序与发送顺序不一致

**解决方案**:
```java
// 1. 使用相同 Key 发送到同一分区
producer.send(new ProducerRecord<>(
    topic,
    "userId:123",  // 使用 userId 作为 Key，保证同一用户消息有序
    userEventJson
));

// 2. 禁用重试或使用有序重试
props.put("retries", 0);

// 3. 使用单线程消费者
// 如果必须保证顺序，不能使用多线程消费同一分区
```

**预防措施**:
- 合理设计消息 Key
- 关键业务使用同步发送

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [Kafka 架构详解](01-architecture.md) | Kafka 核心架构和概念 |
| [Kafka Producer 指南](02-producer.md) | 生产者 API 和配置详解 |
| [Kafka Consumer 指南](03-consumer.md) | 消费者 API 和配置详解 |
| [Kafka Streams 指南](04-streams.md) | 流处理 API 详解 |
| [Kafka 故障排查](06-troubleshooting.md) | 常见问题和解决方案 |
