# Flink 运维指南

## 目录

- [集群部署](#集群部署)
- [作业管理](#作业管理)
- [监控告警](#监控告警)
- [性能调优](#性能调优)
- [故障恢复](#故障恢复)
- [安全配置](#安全配置)

---

## 集群部署

### 环境准备

```bash
# 系统要求
- 操作系统: Linux (CentOS 7+ / Ubuntu 18.04+)
- JDK: 11+ (推荐 OpenJDK 11 或 17)
- CPU: 8 核以上
- 内存: 32GB 以上
- 磁盘: SSD (建议 NVMe)

# 安装 JDK
yum install -y java-11-openjdk-devel
java -version

# 配置环境变量
cat >> /etc/profile << EOF
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk
export JRE_HOME=\$JAVA_HOME/jre
export PATH=\$JAVA_HOME/bin:\$PATH
EOF

source /etc/profile
```

### 集群配置

```yaml
# flink-conf.yaml - Flink 核心配置

# ============ 基础配置 ============
jobmanager.rpc.address: jobmanager-host
jobmanager.rpc.port: 6123
jobmanager.memory.process.size: 4096m
taskmanager.memory.process.size: 16384m
taskmanager.numberOfTaskSlots: 4
parallelism.default: 4

# ============ Web UI ============
rest.port: 8081
rest.address: 0.0.0.0

# ============ Checkpoint ============
execution.checkpointing.interval: 60s
execution.checkpointing.mode: EXACTLY_ONCE
execution.checkpointing.timeout: 600s
execution.checkpointing.min-pause: 5s
execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION

# ============ 状态后端 ============
state.backend: rocksdb
state.backend.incremental: true
state.checkpoints.storage: hdfs://namenode:8020/flink/checkpoints

# ============ 内存配置 ============
taskmanager.memory.managed: true
taskmanager.memory.managed.fraction: 0.4
taskmanager.memory.network.fraction: 0.1
taskmanager.memory.network.min: 64m
taskmanager.memory.task.off-heap: 0

# ============ 重启策略 ============
restart-strategy: fixed-delay
restart-strategy.fixed-delay.attempts: 3
restart-strategy.fixed-delay.delay: 30s

# ============ 安全配置 ============
security.ssl.internal.enabled: true
security.ssl.internal.keystore: /etc/ssl/flink/keystore.jks
security.ssl.internal.keystore-password: password
security.ssl.internal.key-password: password
security.ssl.internal.truststore: /etc/ssl/flink/truststore.jks
```

```yaml
# masters - JobManager 配置
jobmanager-host:8081

# workers - TaskManager 节点
taskmanager-1:8081
taskmanager-2:8081
taskmanager-3:8081
taskmanager-4:8081
```

### 启动脚本

```bash
#!/bin/bash
# bin/flink-daemon.sh - Flink 启动脚本

FLINK_HOME=/opt/flink

# 配置 JVM
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk

# JVM 堆内存
export JVM_HEAP_OPTS="-Xms4g -Xmx4g"

# Flink 内存
export FLINK_HEAP_OPTS="-Xms4g -Xmx4g"

# G1GC 垃圾回收器
JVM_ARGS="-XX:+UseG1GC \
    -XX:MaxGCPauseMillis=200 \
    -XX:+ParallelRefProcEnabled \
    -XX:G1HeapRegionSize=16m \
    -XX:InitiatingHeapOccupancyPercent=35 \
    -XX:MetaspaceSize=256m"

export JVM_ARGS

# 启动 Flink
$FLINK_HOME/bin/flink start-daemon.sh
```

### YARN 部署

```bash
#!/bin/bash
# YARN 集群提交

# Session 模式
flink-yarn-session \
    -d \  # detached 模式
    -jm 2048m \  # JobManager 内存
    -tm 4096m \  # TaskManager 内存
    -s 4 \       # 每个 TM 的 Slot 数
    -nm "Flink Job" \  # 应用名称
    -d

# Per-Job 模式
flink run \
    -m yarn-cluster \
    -yqu root.default \  # YARN 队列
    -yjm 2048m \
    -ytm 4096m \
    -ys 4 \
    /path/to/my-flink-job.jar

# Application 模式 (推荐)
flink run-application \
    -t yarn-application \
    -Djobmanager.memory.process.size=2048m \
    -Dtaskmanager.memory.process.size=4096m \
    -Dyarn.application.name=MyFlinkJob \
    /path/to/my-flink-job.jar
```

### Kubernetes 部署

```yaml
# flink-session-cli-service.yaml - Session Cluster
apiVersion: v1
kind: Service
metadata:
  name: flink-session-rest
spec:
  type: NodePort
  ports:
  - name: rest
    port: 8081
    targetPort: 8081
    nodePort: 30081
  selector:
    app: flink-session
    component: jobmanager
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-session-jobmanager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink-session
      component: jobmanager
  template:
    metadata:
      labels:
        app: flink-session
        component: jobmanager
    spec:
      containers:
      - name: jobmanager
        image: flink:1.18
        args: ["jobmanager"]
        ports:
        - containerPort: 8081
        - containerPort: 6123
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-session-taskmanager
spec:
  replicas: 3
  selector:
    matchLabels:
      app: flink-session
      component: taskmanager
  template:
    metadata:
      labels:
        app: flink-session
        component: taskmanager
    spec:
      containers:
      - name: taskmanager
        image: flink:1.18
        args: ["taskmanager"]
        ports:
        - containerPort: 6122
        env:
        - name: POD_NAME
          valueFrom:
            fieldRef:
              fieldPath: metadata.name
        resources:
          requests:
            memory: "8Gi"
            cpu: "4"
          limits:
            memory: "16Gi"
            cpu: "8"
```

```yaml
# flink-job-deployment.yaml - Per-Job Cluster
apiVersion: flink.apache.org/v1beta1
kind: FlinkDeployment
metadata:
  name: flink-job
spec:
  flinkVersion: v1_18
  image: my-flink-job:latest
  ingress:
    template: "flink.k8s.io/{{name}}(/|$)(.*)"
    className: nginx
  job:
    jarURI: local:///opt/flink/usrlib/my-flink-job.jar
    entryClass: com.example.MyFlinkJob
    args: ["--arg1", "value1"]
    parallelism: 4
    upgradeMode: savepoint
    savepointExpiration: 24h
  serviceAccount: flink
  jobManager:
    resource:
      memory: "2048m"
      cpu: 1
  taskManager:
    resource:
      memory: "8192m"
      cpu: 2
    replicas: 3
  logConfiguration:
    log4j-console.properties: |
      rootLogger.level = INFO
      rootLogger.appenderRef.file.ref = MainAppender
```

---

## 作业管理

### 作业提交

```bash
#!/bin/bash
# submit-job.sh - 作业提交脚本

JOB_JAR="/path/to/flink-job.jar"
PARALLELISM=4
SAVEPOINT_PATH=""

# 提交作业
submit_job() {
    echo "提交作业..."
    flink run \
        -d \  # detached 模式
        -p $PARALLELISM \
        -c com.example.MyFlinkJob \
        $JOB_JAR
}

# 从 Savepoint 恢复
restore_from_savepoint() {
    if [ -n "$SAVEPOINT_PATH" ]; then
        echo "从 Savepoint 恢复: $SAVEPOINT_PATH"
        flink run \
            -s $SAVEPOINT_PATH \
            -d \
            -p $PARALLELISM \
            -c com.example.MyFlinkJob \
            $JOB_JAR
    else
        submit_job
    fi
}

# 带参数提交
submit_with_args() {
    local args="$1"
    flink run \
        -d \
        -p $PARALLELISM \
        -c com.example.MyFlinkJob \
        $JOB_JAR \
        $args
}

# 带自定义配置提交
submit_with_config() {
    local config_file="$1"
    flink run \
        -d \
        -p $PARALLELISM \
        -yqu root.default \
        -yjm 2048m \
        -ytm 4096m \
        -yD key1=value1 \
        -yD key2=value2 \
        -c com.example.MyFlinkJob \
        $JOB_JAR
}
```

### 作业管理

```bash
#!/bin/bash
# job-management.sh - 作业管理

JOB_ID=$1

# 查看作业列表
list_jobs() {
    flink list
    flink list -a  # 包含历史作业
}

# 查看作业详情
describe_job() {
    if [ -z "$JOB_ID" ]; then
        echo "请指定作业 ID"
        return 1
    fi
    flink info $JOB_ID
}

# 取消作业
cancel_job() {
    if [ -z "$JOB_ID" ]; then
        echo "请指定作业 ID"
        return 1
    fi
    flink cancel $JOB_ID
}

# 触发 Savepoint 并取消
cancel_with_savepoint() {
    if [ -z "$JOB_ID" ]; then
        echo "请指定作业 ID"
        return 1
    fi
    local savepoint_path=${2:-/savepoints}
    flink cancel -s $savepoint_path $JOB_ID
}

# 停止作业
stop_job() {
    if [ -z "$JOB_ID" ]; then
        echo "请指定作业 ID"
        return 1
    fi
    flink stop $JOB_ID
}

# 从 Savepoint 恢复
restore_job() {
    local savepoint_path=$1
    local job_jar=$2

    flink run \
        -s $savepoint_path \
        -d \
        -c com.example.MyFlinkJob \
        $job_jar
}

# 修改并行度
scale_job() {
    if [ -z "$JOB_ID" ]; then
        echo "请指定作业 ID"
        return 1
    fi
    local parallelism=$2

    # 使用 savepoint 重新提交
    local savepoint_path=$(trigger_savepoint $JOB_ID /tmp/savepoints)
    cancel_job $JOB_ID

    flink run \
        -s $savepoint_path \
        -d \
        -p $parallelism \
        -c com.example.MyFlinkJob \
        $JOB_JAR
}

# 触发 Savepoint
trigger_savepoint() {
    local job_id=$1
    local target_dir=$2

    flink savepoint $job_id $target_dir
}
```

---

## 监控告警

### 监控指标

```java
/**
 * Flink 监控指标
 */
public class MonitoringMetrics {

    /**
     * 关键指标
     */
    public static class KeyMetrics {
        // Task 相关
        // numRecordsIn: 接收记录数
        // numRecordsOut: 发送记录数
        // numRecordsInPerSecond: 每秒接收数
        // numRecordsOutPerSecond: 每秒发送数
        // numBytesIn: 接收字节数
        // numBytesOut: 发送字节数
        // currentInputWatermark: 当前输入水印

        // Checkpoint 相关
        // numberOfCompletedCheckpoints: 已完成 Checkpoint 数
        // numberOfFailedCheckpoints: 失败 Checkpoint 数
        // lastCheckpointDuration: 最后 Checkpoint 耗时
        // lastCheckpointSize: 最后 Checkpoint 大小

        // 状态相关
        // numKeyedStateSize: Keyed 状态大小
        // numBroadcastStateSize: 广播状态大小
    }

    /**
     * 自定义指标
     */
    public static void customMetrics() {
        // 计数器
        env.getMetricGroup()
            .counter("myCounter")
            .inc();

        // 计量器
        env.getMetricGroup()
            .gauge("myGauge", () -> currentValue);

        // 直方图
        Histogram histogram = env.getMetricGroup()
            .histogram("myHistogram", new MyHistogram());

        // 仪表盘
        Meter meter = env.getMetricGroup()
            .meter("myMeter", new MyMeter());
    }
}
```

### Prometheus 监控

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'flink-jobmanager'
    metrics_path: /metrics
    static_configs:
      - targets: ['jobmanager:9249']

  - job_name: 'flink-taskmanager'
    metrics_path: /metrics
    static_configs:
      - targets: ['taskmanager1:9249', 'taskmanager2:9249', 'taskmanager3:9249']

  - job_name: 'flink-operator'
    kubernetes_sd_configs:
      - role: pod
        namespaces:
          names:
            - flink
    relabel_configs:
      - source_labels: [__meta_kubernetes_pod_label_app]
        target_label: app
```

```yaml
# flink-metrics.yaml - Flink Prometheus 配置
metrics:
  reporter:
    promgateway:
      class: org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporter
      host: prometheus-pushgateway
      port: 9091
      jobName: flink-metrics
      deleteOnShutdown: true
      interval: 60 SECONDS
```

### 告警规则

```yaml
# prometheus-alerts.yml
groups:
  - name: flink-alerts
    rules:
      # 作业失败
      - alert: FlinkJobFailed
        expr: flink_jobmanager_job_numberOfFailedJobVertices > 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "Flink 作业失败"
          description: "作业 {{ $labels.job_name }} 失败"

      # Checkpoint 失败
      - alert: FlinkCheckpointFailed
        expr: increase(flink_jobmanager_job_numberOfFailedCheckpoints[5m]) > 0
        for: 1m
        labels:
          severity: warning
        annotations:
          summary: "Checkpoint 失败"
          description: "作业 {{ $labels.job_name }} Checkpoint 失败"

      # 反压
      - alert: FlinkBackpressure
        expr: flink_taskmanager_job_task_operator_isBackPressured > 0
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "Task 反压"
          description: "Task {{ $labels.task_name }} 存在反压"

      # 延迟过高
      - alert: FlinkHighLatency
        expr: flink_jobmanager_job_currentInputWatermark - flink_taskmanager_job_task_operator_currentInputWatermark > 300000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "处理延迟过高"
          description: "延迟超过 5 分钟"

      # 资源不足
      - alert: FlinkHighMemory
        expr: taskmanager_Status.JVM.Memory.Heap.Used / taskmanager_Status.JVM.Memory.Heap.Max > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "TaskManager 内存使用率高"
          description: "内存使用率 {{ $value | humanize1024 }}B"
```

---

## 性能调优

### 并行度调优

```java
/**
 * 并行度配置
 */
public class ParallelismTuning {

    /**
     * 全局并行度
     */
    public static void globalParallelism() {
        // 设置默认并行度
        env.setParallelism(4);
    }

    /**
     * 算子级别并行度
     */
    public static void operatorParallelism() {
        DataStream<String> stream = env
            .fromElements("a", "b", "c")
            .map(String::toUpperCase)
            .setParallelism(2);  // 单个算子并行度

        stream
            .keyBy(s -> s)
            .process(new MyProcessFunction())
            .setParallelism(8);  // Keyed 算子并行度
    }

    /**
     * Slot 规划
     */
    public static void slotPlanning() {
        // TaskManager 配置
        // taskmanager.numberOfTaskSlots: 4

        // 集群规划
        // 3 个 TaskManager，每个 4 Slot = 12 Slot
        // 全局并行度 12 = 最大并行度

        // 如果作业并行度为 8
        // 12 Slot 中运行 8 Task
        // 剩余 4 Slot 空闲
    }
}
```

### 内存调优

```java
/**
 * 内存配置优化
 */
public class MemoryTuning {

    /**
     * TaskManager 内存
     */
    public static void taskManagerMemory() {
        // 总进程内存 = 16GB
        // - JVM Heap: 4GB (框架 + 算子)
        // - 托管内存: 6.4GB (40%)
        // - 网络内存: 1.6GB (10%)
        // - 直接内存: 约 4GB

        // 关键配置
        // taskmanager.memory.process.size: 16384m
        // taskmanager.memory.managed: true
        // taskmanager.memory.managed.fraction: 0.4
        // taskmanager.memory.network.fraction: 0.1
    }

    /**
     * RocksDB 内存
     */
    public static void rocksDBMemory() {
        // 托管内存分配给 RocksDB
        // 默认 RocksDB 使用托管内存的 50%

        // 配置
        // taskmanager.memory.managed: true
        // state.backend.rocksdb.memory.managed: true
        // state.backend.rocksdb.memory.fixed-per-slot: 256mb

        // 监控
        // rocksdb.blockCacheCapacity
        // rocksdb.estimateLiveDataSize
    }

    /**
     * 网络缓冲
     */
    public static void networkBuffer() {
        // 网络内存 = 1.6GB
        // 用于 Task 之间数据传输

        // 配置
        // taskmanager.memory.network.fraction: 0.1
        // taskmanager.memory.network.min: 64m
        // taskmanager.memory.network.max: 1g

        // 调优
        // 增加网络缓冲减少反压
        // 减少网络缓冲增加托管内存
    }
}
```

### Checkpoint 调优

```java
/**
 * Checkpoint 性能优化
 */
public class CheckpointTuning {

    public static void checkpointOptimization() {
        // 1. 合理间隔
        env.enableCheckpointing(60000);  // 60秒

        // 2. 非对齐 Checkpoint
        env.getCheckpointConfig()
            .enableUnalignedCheckpoints();

        // 3. 并发 Checkpoint
        env.getCheckpointConfig()
            .setMaxConcurrentCheckpoints(1);

        // 4. 超时时间
        env.getCheckpointConfig()
            .setCheckpointTimeout(600000);  // 10分钟

        // 5. 最小间隔
        env.getCheckpointConfig()
            .setMinPauseBetweenCheckpoints(5000);
    }
}
```

---

## 故障恢复

### 故障场景

```bash
#!/bin/bash
# recovery-scenarios.sh - 故障恢复场景

# ============ 场景1: TaskManager 故障 ============

# 1. 查看故障
flink list

# 2. 查看日志
tail -f /data/flink/log/flink-*-taskmanager-*.out

# 3. 重启 TaskManager
systemctl restart flink-taskmanager

# 4. 验证恢复
flink list

# ============ 场景2: JobManager 故障 ============

# 1. 停止集群
bin/stop-cluster.sh

# 2. 备份数据
hdfs dfs -mkdir /backup/flink/$(date +%Y%m%d)
hdfs dfs -cp /flink/checkpoints /backup/flink/$(date +%Y%m%d)/

# 3. 修复问题
# - 修复配置
# - 修复代码
# - 升级版本

# 4. 重启集群
bin/start-cluster.sh

# 5. 从 Savepoint 恢复
flink run -s hdfs:///savepoints/savepoint-xxx -d ...

# ============ 场景3: 作业失败 ============

# 1. 查看失败原因
flink info <job_id>

# 2. 查看异常日志
tail -1000 /data/flink/log/flink-*-jobmanager-*.out

# 3. 分析根因
# - 内存溢出?
# - 数据异常?
# - 依赖问题?

# 4. 修复并重新提交
flink run -s hdfs:///savepoints/savepoint-xxx -d new-job.jar

# ============ 场景4: Checkpoint 失败 ============

# 1. 检查 Checkpoint 存储
hdfs dfs -ls /flink/checkpoints

# 2. 查看 Checkpoint 失败日志
tail -f /data/flink/log/flink-*-jobmanager-*.out | grep -i checkpoint

# 3. 常见问题
# - 存储空间不足
# - 网络超时
# - RocksDB 性能

# 4. 解决方案
# - 清理旧 Checkpoint
# - 增加超时时间
# - 优化 RocksDB 配置
```

### Savepoint 恢复

```bash
#!/bin/bash
# savepoint-recovery.sh - Savepoint 恢复

SAVEPOINT_PATH=$1
JOB_JAR=$2

# 1. 触发 Savepoint (如果作业还在运行)
trigger_savepoint() {
    local job_id=$(get_running_job_id)
    if [ -n "$job_id" ]; then
        flink savepoint $job_id /savepoints
    fi
}

# 2. 从 Savepoint 恢复
restore_from_savepoint() {
    if [ ! -d "$SAVEPOINT_PATH" ]; then
        echo "Savepoint 不存在: $SAVEPOINT_PATH"
        return 1
    fi

    echo "从 Savepoint 恢复: $SAVEPOINT_PATH"

    flink run \
        -s $SAVEPOINT_PATH \
        -d \
        -p 4 \
        -c com.example.MyFlinkJob \
        $JOB_JAR
}

# 3. 跳过不可恢复状态
restore_with_skip() {
    flink run \
        -s $SAVEPOINT_PATH \
        --allowNonRestoredState \
        -d \
        -c com.example.MyFlinkJob \
        $JOB_JAR
}

# 4. 跨版本恢复
cross_version_restore() {
    # 升级 Flink 版本后
    # 使用新版 Flink 从旧版 Savepoint 恢复

    # 1. 确保新版本兼容旧状态
    # 2. 触发 Savepoint
    # 3. 升级 Flink
    # 4. 从 Savepoint 恢复
}
```

---

## 安全配置

### Kerberos 认证

```yaml
# flink-conf.yaml - Kerberos 配置
security.kerberos.login.use-ticket-cache: false
security.kerberos.login.keytab: /etc/security/keytabs/flink.keytab
security.kerberos.login.principal: flink@REALM

# HDFS Kerberos
security.kerberos.login.contexts: Client,KafkaClient
```

### SSL 加密

```yaml
# flink-conf.yaml - SSL 配置
# 内部通信加密
security.ssl.internal.enabled: true
security.ssl.internal.keystore: /etc/ssl/flink/keystore.jks
security.ssl.internal.keystore-password: password
security.ssl.internal.key-password: password
security.ssl.internal.truststore: /etc/ssl/flink/truststore.jks
security.ssl.internal.truststore-password: password

# REST API 加密
security.ssl.rest.enabled: true
security.ssl.rest.keystore: /etc/ssl/flink/rest-keystore.jks
security.ssl.rest.keystore-password: password
security.ssl.rest.key-password: password
security.ssl.rest.truststore: /etc/ssl/flink/rest-truststore.jks
security.ssl.rest.truststore-password: password
```

### 访问控制

```bash
#!/bin/bash
# 生成 SSL 证书

# 生成 keystore
keytool -genkeypair \
    -alias flink-internal \
    -keyalg RSA \
    -keystore keystore.jks \
    -storepass password \
    -keypass password \
    -dname "CN=flink, OU=BigData, O=Company, L=City, ST=Province, C=CN"

# 生成 CSR
keytool -certreq \
    -alias flink-internal \
    -keystore keystore.jks \
    -file flink.csr

# 签名证书
# 提交给 CA 签名

# 导入证书链
keytool -importcert \
    -alias ca \
    -file ca.crt \
    -keystore keystore.jks \
    -storepass password

keytool -importcert \
    -alias flink-internal \
    -file signed-cert.crt \
    -keystore keystore.jks \
    -storepass password

# 导出 truststore
keytool -exportcert \
    -alias flink-internal \
    -keystore keystore.jks \
    -file flink.crt \
    -storepass password

keytool -importcert \
    -alias flink-internal \
    -file flink.crt \
    -keystore truststore.jks \
    -storepass password
```

---

## 最佳实践

### 开发规范

```java
/**
 * Flink 开发最佳实践
 */
public class FlinkBestPractices {

    /**
     * 1. 合理设置并行度
     *
     * 原则:
     * - Source 并行度与分区数匹配
     * - 中间算子根据数据量和机器资源
     * - Sink 并行度与目标系统匹配
     */
    public static void parallelismBestPractices() {
        // Source 并行度
        // Kafka: 与 Topic 分区数相同
        // File: 与文件数量相关
        DataStream<String> source = env
            .addSource(new FlinkKafkaConsumer<>(...))
            .setParallelism(4);  // Kafka Topic 有 4 个分区

        // 中间算子
        // 根据处理复杂度和数据量调整
        DataStream<String> processed = source
            .map(new ComplexMapFunction())
            .setParallelism(8);  // CPU 密集型，增加并行度

        // Sink 并行度
        // 数据库: 不宜过高，避免连接数过多
        processed.addSink(new JdbcSink()).setParallelism(2);
    }

    /**
     * 2. 正确使用 KeyBy
     *
     * 原则:
     * - 选择合适的 Key
     * - 避免数据倾斜
     * - Key 类型要可序列化
     */
    public static void keyByBestPractices() {
        // Good: 使用业务 Key
        orders.keyBy(order -> order.getCustomerId());

        // Good: 使用复合 Key
        orders.keyBy(order -> Tuple2.of(order.getCustomerId(), order.getRegion()));

        // Avoid: 使用随机 Key
        orders.keyBy(order -> UUID.randomUUID().toString());

        // 处理数据倾斜
        // 方案1: 加盐打散
        orders.keyBy(order ->
            order.getCustomerId() + "-" + (int)(Math.random() * 10)
        );

        // 方案2: 本地预聚合
        orders.map(new CountFunction()).setParallelism(10)
            .keyBy(...)
            .process(new GlobalSumFunction());
    }

    /**
     * 3. 避免状态膨胀
     *
     * 原则:
     * - 及时清理不需要的状态
     * - 使用状态 TTL
     * - 选择合适的状态后端
     */
    public static void stateBestPractices() {
        // 使用状态 TTL 自动清理
        StateTtlConfig ttlConfig = StateTtlConfig
            .newBuilder(Time.hours(24))
            .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
            .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
            .build();

        ValueStateDescriptor<MyState> descriptor =
            new ValueStateDescriptor<>("my-state", MyState.class);
        descriptor.enableTimeToLive(ttlConfig);
    }

    /**
     * 4. 优化 Checkpoint 配置
     *
     * 原则:
     * - 合理设置 Checkpoint 间隔
     * - 使用增量 Checkpoint
     * - 配置合适的超时时间
     */
    public static void checkpointBestPractices() {
        env.enableCheckpointing(60000);  // 1分钟间隔

        env.getCheckpointConfig().setCheckpointingMode(
            CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointTimeout(600000);  // 10分钟超时

        // 使用增量 Checkpoint (RocksDB)
        RocksDBStateBackend rocksDB = new RocksDBStateBackend(
            "hdfs://...", true);
        rocksDB.enableIncrementalCheckpointing(true);

        // 非对齐 Checkpoint 减少反压影响
        env.getCheckpointConfig().enableUnalignedCheckpoints();
    }

    /**
     * 5. 合理使用窗口
     *
     * 原则:
     * - 避免使用过大的窗口
     * - 使用增量聚合减少内存
     * - 正确处理迟到数据
     */
    public static void windowBestPractices() {
        // 使用增量聚合
        orders
            .keyBy(...)
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .aggregate(new IncrementalAggregateFunction());

        // 处理迟到数据
        orders
            .keyBy(...)
            .window(TumblingEventTimeWindows.of(Time.hours(1)))
            .allowedLateness(Time.minutes(5))  // 允许迟到 5 分钟
            .sideOutputLateData(lateOutputTag)  // 收集严重迟到数据
            .sum(...);
    }
}
```

### 常见问题

```java
/**
 * Flink 常见问题与解决方案
 */
public class CommonIssues {

    /**
     * 问题1: Checkpoint 失败
     *
     * 可能原因:
     * - 状态后端存储空间不足
     * - Checkpoint 超时
     * - 状态数据过大
     *
     * 解决方案:
     * - 清理旧的 Checkpoint
     * - 增加超时时间
     * - 使用 RocksDB 增量 Checkpoint
     */
    public static void checkpointFailure() {
        // 清理 Checkpoint
        // hdfs dfs -rm -r /flink/checkpoints/*

        // 增加超时时间
        env.getCheckpointConfig().setCheckpointTimeout(600000);

        // 使用 RocksDB
        env.setStateBackend(new RocksDBStateBackend("hdfs://...", true));
    }

    /**
     * 问题2: 数据倾斜
     *
     * 表现:
     * - 部分 Task 处理数据量远大于其他
     * - 部分 Task 耗时过长
     *
     * 解决方案:
     * - 加盐打散
     * - 两阶段聚合
     * - 调整并行度
     */
    public static void dataSkew() {
        // 方案1: 加盐打散
        orders
            .flatMap((order, out) -> {
                String key = order.getCustomerId();
                int salt = ThreadLocalRandom.current().nextInt(10);
                out.collect(Tuple2.of(key + "-" + salt, order));
            })
            .keyBy(t -> t.f0)
            .sum(1)
            .map(t -> Tuple2.of(t.f0.split("-")[0], t.f1));
    }

    /**
     * 问题3: 背压 (Backpressure)
     *
     * 表现:
     * - Task 处理速度慢于上游
     * - 缓冲区占用率高
     *
     * 解决方案:
     * - 增加并行度
     * - 优化算子性能
     * - 增加网络缓冲
     */
    public static void backpressure() {
        // 增加网络缓冲
        env.getConfig().enableSysoutLogging();

        // 优化算子
        orders
            .map(new OptimizedMapFunction())  // 避免复杂计算
            .setParallelism(16);
    }

    /**
     * 问题4: 状态恢复失败
     *
     * 可能原因:
     * - Savepoint 损坏
     * - 状态类型不兼容
     * - 算子 ID 变化
     *
     * 解决方案:
     * - 使用 --allowNonRestoredState
     * - 重新触发 Savepoint
     * - 保持算子 ID 稳定
     */
    public static void stateRecoveryFailure() {
        // 使用 allowNonRestoredState
        // flink run -s <savepoint> --allowNonRestoredState ...
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
| [Flink 状态管理](04-state-checkpoint.md) | 状态管理和 Checkpoint |
| [Flink CEP](05-cep.md) | 复杂事件处理 |
