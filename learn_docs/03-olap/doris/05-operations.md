# Doris 运维指南

## 目录

- [集群部署](#集群部署)
- [节点管理](#节点管理)
- [高可用配置](#高可用配置)
- [监控告警](#监控告警)
- [备份恢复](#备份恢复)
- [故障处理](#故障处理)

---

## 集群部署

### 环境准备

```bash
#!/bin/bash
# 1. 创建用户
useradd -m doris
passwd doris

# 2. 创建目录
mkdir -p /data/doris/{fe,be,storage}
mkdir -p /var/log/doris

# 3. 安装 Java (JDK 8 或 11)
apt-get install openjdk-11-jdk
# 或
yum install java-11-openjdk-devel

# 4. 设置环境变量
cat >> /etc/profile << EOF
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
export PATH=\$JAVA_HOME/bin:\$PATH
EOF
source /etc/profile

# 5. 安装 MySQL 客户端
apt-get install mysql-client
# 或
yum install mysql

# 6. 关闭防火墙
systemctl stop firewalld
systemctl disable firewalld

# 7. 关闭 SELinux
setenforce 0
sed -i 's/SELINUX=enforcing/SELINUX=disabled/' /etc/selinux/config
```

### FE 部署

```bash
#!/bin/bash
# 1. 下载 Doris
wget https://archive.apache.org/dist/doris/2.0.13/apache-doris-2.0.13-bin.tar.gz
tar -xzf apache-doris-2.0.13-bin.tar.gz
cd apache-doris-2.0.13-bin

# 2. 配置 FE
cat > fe/conf/fe.conf << EOF
# 基础配置
priority_networks = 192.168.1.0/24
http_port = 8030
rpc_port = 9010
query_port = 9030
edit_log_port = 9010

# 目录配置
meta_dir = /data/doris/fe/meta
log_dir = /var/log/doris/fe

# JVM 配置
JAVA_HOME = /usr/lib/jvm/java-11-openjdk-amd64
JAVA_OPTS = "-Xmx4096m -XX:+UseG1GC"

# 元数据
FE leader
EOF

# 3. 启动 FE
./fe/bin/start_fe.sh --daemon

# 4. 检查状态
mysql -h 127.0.0.1 -P 9030 -u root
SHOW FRONTENDS;
```

### BE 部署

```bash
#!/bin/bash
# 1. 下载 Doris (与 FE 相同版本)
wget https://archive.apache.org/dist/doris/2.0.13/apache-doris-2.0.13-bin.tar.gz
tar -xzf apache-doris-2.0.13-bin.tar.gz
cd apache-doris-2.0.13-bin

# 2. 配置 BE
cat > be/conf/be.conf << EOF
# 基础配置
priority_networks = 192.168.1.0/24
be_port = 9050
webserver_port = 8040
heartbeat_service_port = 9050
brpc_port = 8060

# 目录配置
storage_root_path = /data/doris/be/storage,/data/doris/be/storage2
log_dir = /var/log/doris/be

# JVM 配置
JAVA_HOME = /usr/lib/jvm/java-11-openjdk-amd64
JAVA_OPTS = "-Xmx8192m -XX:+UseG1GC"

# CPU 核数
be_node_num_per_disk = 2
EOF

# 3. 启动 BE
./be/bin/start_be.sh --daemon

# 4. 添加 BE 到集群
mysql -h fe_host -P 9030 -u root
ALTER SYSTEM ADD BACKEND "be_host:9050";

# 5. 检查状态
SHOW BACKENDS;
```

### Broker 部署

```bash
#!/bin/bash
# 1. 下载 Doris Spark/Broker
wget https://archive.apache.org/dist/doris/2.0.13/apache-doris-2.0.13-bin.tar.gz
tar -xzf apache-doris-2.0.13-bin.tar.gz
cd apache-doris-2.0.13-bin

# 2. 配置 Broker
cat > external_support/inf_right/inf_right.conf << EOF
broker.name = hdfs_broker
broker.ipfs.username = hdfs
hadoop.security.authentication = simple
EOF

# 3. 启动 Broker
./external_support/inf_right/inf_right --daemon

# 4. 添加 Broker
mysql -h fe_host -P 9030 -u root
ALTER SYSTEM ADD BROKER hdfs_broker "broker_host:8000";
```

---

## 节点管理

### FE 管理

```sql
-- 1. 查看 FE 状态
SHOW FRONTENDS;

-- 2. 添加 Follower FE
ALTER SYSTEM ADD FOLLOWER "fe_host:9010";

-- 3. 添加 Observer FE
ALTER SYSTEM ADD OBSERVER "fe_host:9010";

-- 4. 删除 FE
ALTER SYSTEM DROP FOLLOWER "fe_host:9010";
ALTER SYSTEM DROP OBSERVER "fe_host:9010";

-- 5. 下线 FE (优雅停止)
-- 先停止 FE 进程
ps aux | grep DorisFe
kill -9 <pid>

-- 6. FE 故障转移
-- Follower 自动选举新的 Leader
-- 查看选举状态
SHOW PROC '/frontends';
```

### BE 管理

```sql
-- 1. 查看 BE 状态
SHOW BACKENDS;

-- 2. 添加 BE
ALTER SYSTEM ADD BACKEND "be_host:9050";

-- 3. 下线 BE (数据迁移)
ALTER SYSTEM DECOMMISSION BACKEND "be_host:9050";

-- 4. 监控下线进度
-- 检查 Tablet 迁移状态
SELECT * FROM information_schema.backend_active;

-- 5. 确认下线完成
-- 查看副本分布
ADMIN SHOW REPLICA DISTRIBUTION FROM table_name;

-- 6. 删除 BE (下线完成后)
ALTER SYSTEM DROP BACKEND "be_host:9050";

-- 7. 恢复 BE
ALTER SYSTEM ADD BACKEND "be_host:9050";

-- 8. 停止 BE
-- 先下线
ALTER SYSTEM DECOMMISSION BACKEND "be_host:9050";
-- 等待数据迁移完成
ps aux | grep DorisBe
kill -9 <pid>
```

### 副本管理

```sql
-- 1. 查看副本分布
ADMIN SHOW REPLICA DISTRIBUTION FROM table_name;

-- 2. 查看副本状态
ADMIN SHOW REPLICA STATUS FROM table_name;

-- 3. 检查 Tablet 健康度
SHOW PROC '/cluster_health/tablet_health';

-- 4. 修复损坏副本
-- 方式1: 自动修复
-- Doris 自动从其他副本拉取数据

-- 方式2: 手动修复
ADMIN REPAIR TABLE table_name;

-- 5. 手动触发副本均衡
ADMIN BALANCE TABLE table_name;

-- 6. 查看副本修复进度
SHOW PROC '/cluster_health/tablets/ratio';
```

### 均衡操作

```sql
-- 1. 自动均衡
-- Doris 自动进行副本均衡

-- 2. 手动触发均衡
ADMIN BALANCE ALL;

-- 3. 查看均衡状态
SHOW PROC '/cluster_health/balance';

-- 4. 设置均衡优先级
ADMIN SET REPLICA STATUS PROPERTIES ("均衡优先级" = "high");
```

---

## 高可用配置

### FE 高可用

```bash
# 1. FE 配置 (3 个节点)
# fe1.conf
priority_networks = 192.168.1.11/24
edit_log_port = 9010

# fe2.conf
priority_networks = 192.168.1.12/24
edit_log_port = 9010

# fe3.conf
priority_networks = 192.168.1.13/24
edit_log_port = 9010

# 2. 启动 FE
# 启动 fe1 为 Leader
./fe/bin/start_fe.sh --daemon

# 启动 fe2, fe3
./fe/bin/start_fe.sh --helper fe1:9010 --daemon

# 3. 添加到集群
mysql -h fe1 -P 9030 -u root
ALTER SYSTEM ADD FOLLOWER "fe2:9010";
ALTER SYSTEM ADD FOLLOWER "fe3:9010";
```

### BE 高可用

```sql
-- 1. 副本数设置
-- 建表时指定
CREATE TABLE users (
    user_id BIGINT NOT NULL,
    name VARCHAR(50)
) UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "3");

-- 2. 修改副本数
ALTER TABLE users SET ("replication_num" = "3");

-- 3. 多机房部署
-- FE: 3 节点
-- BE: 每机房至少 1 节点
-- 副本数: >= 机房数

-- 4. 跨机房均衡
-- 均衡时会考虑机房分布
ADMIN BALANCE ALL;
```

### 备份存储

```sql
-- 1. 创建备份仓库
CREATE REPOSITORY "hdfs_repo"
WITH BROKER hdfs_broker
PROPERTIES (
    "dfs.nameservices" = "mycluster",
    "dfs.ha.namenodes.mycluster" = "nn1,nn2",
    "dfs.namenode.rpc-address.mycluster.nn1" = "namenode1:8030",
    "dfs.namenode.rpc-address.mycluster.nn2" = "namenode2:8030",
    "hadoop.username" = "hdfs"
);

-- 2. 查看仓库
SHOW REPOSITORIES;

-- 3. 删除仓库
DROP REPOSITORY "hdfs_repo";
```

---

## 监控告警

### Prometheus 配置

```yaml
# prometheus.yml
global:
  scrape_interval: 15s

scrape_configs:
  - job_name: 'doris-fe'
    static_configs:
      - targets: ['fe1:8030', 'fe2:8030', 'fe3:8030']
    metrics_path: '/metrics'

  - job_name: 'doris-be'
    static_configs:
      - targets: ['be1:8040', 'be2:8040', 'be3:8040']
    metrics_path: '/metrics'

  - job_name: 'doris-be-brpc'
    static_configs:
      - targets: ['be1:8060', 'be2:8060', 'be3:8060']
    metrics_path: '/brpc_metrics'
```

### Grafana 仪表板

```json
// Doris FE 监控
{
  "panels": [
    {
      "title": "FE QPS",
      "type": "graph",
      "targets": [
        {
          "expr": "rate(doris_fe_query_total[5m])",
          "legendFormat": "{{job}}"
        }
      ]
    },
    {
      "title": "FE 内存使用",
      "type": "graph",
      "targets": [
        {
          "expr": "doris_fe_jvm_memory_max_bytes{area=\"heap\"}",
          "legendFormat": "{{instance}} max"
        },
        {
          "expr": "doris_fe_jvm_memory_used_bytes{area=\"heap\"}",
          "legendFormat": "{{instance}} used"
        }
      ]
    }
  ]
}
```

### 告警规则

```yaml
# alert.yml
groups:
  - name: doris
    rules:
      # FE 宕机
      - alert: DorisFE_DOWN
        expr: up{job="doris-fe"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "FE 节点宕机: {{ $labels.instance }}"

      # BE 宕机
      - alert: DorisBE_DOWN
        expr: up{job="doris-be"} == 0
        for: 1m
        labels:
          severity: critical
        annotations:
          summary: "BE 节点宕机: {{ $labels.instance }}"

      # BE 磁盘使用率
      - alert: DorisBE_DiskHigh
        expr: doris_be_storage_disk_used_capacity / doris_be_storage_disk_total_capacity > 0.85
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "BE 磁盘使用率过高: {{ $labels.instance }}"

      # 查询延迟高
      - alert: DorisQuerySlow
        expr: histogram_quantile(0.95, rate(doris_fe_query_latency_ms_bucket[5m])) > 10000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "95% 查询延迟超过 10s"

      # Tablet 损坏
      - alert: DorisTabletBad
        expr: doris_be_fragment_endpoint_count{status="bad"} > 0
        labels:
          severity: critical
        annotations:
          summary: "存在损坏的 Tablet"
```

### 日志查看

```bash
# FE 日志
tail -f /var/log/doris/fe/fe.log
tail -f /var/log/doris/fe/fe.out

# BE 日志
tail -f /var/log/doris/be/be.INFO
tail -f /var/log/doris/be/be.WARNING
tail -f /var/log/doris/be/be.ERROR

# 慢查询日志
tail -f /var/log/doris/fe/slow_query.log
```

---

## 备份恢复

### 全量备份

```sql
-- 1. 创建备份
BACKUP SNAPSHOT test_db.snapshot_20240115
TO "hdfs_repo"
PROPERTIES (
    "backup_job_timeout" = "3600",
    "replication_num" = "3"
);

-- 2. 查看备份状态
SHOW BACKUP FROM test_db;

-- 3. 取消备份
CANCEL BACKUP FROM test_db;

-- 4. 查看所有备份
SHOW SNAPSHOTS FROM "hdfs_repo";
```

### 增量备份

```sql
-- Doris 支持增量备份
-- 每次备份只备份新增数据

-- 创建增量备份
BACKUP SNAPSHOT test_db.snapshot_20240116
TO "hdfs_repo"
AS "incremental_backup";

-- 查看增量备份
SHOW SNAPSHOTS FROM "hdfs_repo";
```

### 恢复数据

```sql
-- 1. 查看备份
SHOW SNAPSHOTS FROM "hdfs_repo";

-- 2. 恢复表
RESTORE SNAPSHOT test_db.snapshot_20240115
FROM "hdfs_repo"
PROPERTIES (
    "backup_timestamp" = "2024-01-15-12-00-00",
    "replication_num" = "3"
);

-- 3. 恢复表到新名称
RESTORE SNAPSHOT test_db.snapshot_20240115
FROM "hdfs_repo"
AS "restored_table"
PROPERTIES ("backup_timestamp" = "2024-01-15-12-00-00");

-- 4. 恢复分区
RESTORE SNAPSHOT test_db.snapshot_20240115
FROM "hdfs_repo"
PARTITION (p202401)
AS "restored_partition"
PROPERTIES ("backup_timestamp" = "2024-01-15-12-00-00");

-- 5. 查看恢复任务
SHOW RESTORE FROM test_db;

-- 6. 取消恢复
CANCEL RESTORE FROM test_db;
```

### 数据迁移

```sql
-- 1. 导出数据
EXPORT TABLE test_db.users
TO "hdfs://namenode:9000/export/users"
PROPERTIES (
    "column_separator" = ",",
    "timeout" = "3600"
);

-- 2. 创建目标表
CREATE TABLE users_new (
    user_id BIGINT NOT NULL,
    name VARCHAR(50)
) UNIQUE KEY(user_id)
DISTRIBUTED BY HASH(user_id) BUCKETS 8
PROPERTIES ("replication_num" = "3");

-- 3. 导入数据
-- 使用 Stream Load 或 Broker Load
```

---

## 故障处理

### FE 故障

```sql
-- 1. FE Leader 故障
-- 自动选举新的 Leader
-- 客户端重连到新 Leader

-- 2. FE Follower 故障
-- 不影响查询
-- 需要尽快恢复

-- 3. 查看 FE 状态
SHOW FRONTENDS;

-- 4. 重建 FE
-- 如果元数据损坏
-- 1) 停止所有 FE
-- 2) 从其他 FE 复制元数据目录
-- 3) 启动 FE
```

### BE 故障

```bash
#!/bin/bash
# 1. BE 进程异常
# 查看日志
tail -f /var/log/doris/be/be.INFO

# 2. BE 磁盘满
# 清理临时文件
rm -rf /data/doris/be/storage/temp/*
rm -rf /data/doris/be/storage/snapshot/*

# 3. BE OOM
# 增加内存配置
# 修改 be.conf: JAVA_OPTS = "-Xmx16384m"

# 4. BE 核心转储
# 查看 core 文件
ls -lh /var/log/doris/be/
# 使用 gdb 分析
gdb ./lib/doris_be core.xxx
```

### 副本恢复

```sql
-- 1. 检查副本状态
ADMIN SHOW REPLICA STATUS FROM table_name;

-- 2. 手动修复副本
-- 删除损坏副本
ADMIN DELETE REPLICA FROM TABLE table_name PARTITION partition_name ON BE "be_host:9050";

-- 3. 添加新副本
ALTER TABLE table_name ADD REPLICA ON BE "new_be_host:9050";

-- 4. 强制修复
-- (慎用，可能丢失数据)
ADMIN REPAIR TABLE table_name;
```

### 数据倾斜

```sql
-- 1. 查看数据分布
ADMIN SHOW REPLICA DISTRIBUTION FROM table_name;

-- 2. 重新均衡
-- 增加分区数
ALTER TABLE table_name ADD PARTITION ...

-- 3. 修改分桶
-- 重建表
CREATE TABLE new_table
DISTRIBUTED BY HASH(key) BUCKETS 16
AS SELECT * FROM old_table;

-- 4. 分桶优化
-- 选择高基数字段
-- 调整分桶数
```

### 常见问题

```sql
-- Q1: 查询超时
-- A: 增加超时时间
SET query_timeout = 600;

-- Q2: 内存不足
-- A: 增加 BE 内存
-- A: 优化查询，减少 shuffle

-- Q3: 导入失败
-- A: 查看导入错误信息
SHOW LOAD;

-- Q4: 副本缺失
-- A: 检查 BE 状态
-- A: 手动添加副本

-- Q5: 磁盘空间不足
-- A: 清理历史数据
-- A: 扩容 BE
-- A: 启用自动清理
```

---

## 最佳实践

### 表设计最佳实践

```sql
-- ============================================
-- 1. 分桶键选择
-- ============================================

-- 选择原则:
-- - 高基数列 (唯一值多)
-- - 经常用于 JOIN 的列
-- - 经常用于 GROUP BY 的列
-- - 避免热点数据 (如用户ID按hash分布)

-- 好: 用户ID分桶
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2)
)
DISTRIBUTED BY HASH(user_id) BUCKETS 16;

-- 差: 状态分桶 (只有几个值，数据倾斜)
CREATE TABLE orders (
    order_id BIGINT,
    status VARCHAR(20),
    amount DECIMAL(10, 2)
)
DISTRIBUTED BY HASH(status) BUCKETS 10;

-- ============================================
-- 2. 分桶数设置
-- ============================================

-- 公式: 分桶数 ≈ BE节点数 × (每节点CPU核心数 / 2)
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    amount DECIMAL(10, 2)
)
DISTRIBUTED BY HASH(user_id) BUCKETS 80
PROPERTIES ("replication_num" = "3");

-- ============================================
-- 3. 分区设计
-- ============================================

-- 动态分区配置
CREATE TABLE logs (
    ts DATETIME,
    level VARCHAR(20),
    message TEXT
)
PARTITION BY RANGE(TO_DAYS(ts)) (
    PARTITION p_current VALUES LESS THAN (MAXVALUE)
)
PROPERTIES (
    "enable_dynamic_partition" = "true",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.history_partition_num" = "30",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.time_unit" = "DAY"
);

-- ============================================
-- 4. 副本数设置
-- ============================================

-- 生产环境建议
PROPERTIES ("replication_num" = "3");

-- 高可用配置
PROPERTIES (
    "replication_num" = "3",
    "replication_allocation" = "tag.location.default: 3"
);

-- ============================================
-- 5. 索引优化
-- ============================================

-- Bloom Filter 索引 (高基数字段精确查询)
CREATE TABLE orders (
    order_id BIGINT,
    user_id BIGINT,
    order_no VARCHAR(64),
    amount DECIMAL(10, 2)
)
UNIQUE KEY(order_id)
DISTRIBUTED BY HASH(order_id) BUCKETS 10
PROPERTIES (
    "replication_num" = "3",
    "bloom_filter_columns" = "order_no,user_id"
);

-- 稀疏索引 (Doris 2.0+)
PROPERTIES ("enable_light_schema_change" = "true");
```

### 查询优化最佳实践

```sql
-- ============================================
-- 1. 使用分区剪枝
-- 好: 使用分区键过滤
SELECT * FROM logs WHERE dt = '2024-01-10';

-- 差: 无法分区裁剪
SELECT * FROM logs
WHERE DATE_FORMAT(ts, '%Y-%m-%d') = '2024-01-10';

-- ============================================
-- 2. 使用分桶剪枝
SELECT * FROM orders WHERE user_id = 1001;

-- ============================================
-- 3. 避免 SELECT *
SELECT order_id, user_id, amount FROM orders WHERE user_id = 1001;

-- ============================================
-- 4. 创建合适的 Rollup
ALTER TABLE orders
ADD ROLLUP rollup_user_status (user_id, status, create_time);

ALTER TABLE orders
ADD ROLLUP rollup_daily (DATE(create_time), status);

-- ============================================
-- 5. Join 优化
-- 小表 Broadcast Join
SELECT /*+ BROADCAST(users) */ *
FROM orders o JOIN users u ON o.user_id = u.user_id
WHERE o.amount > 1000;

-- Bucket Shuffle Join
SELECT /*+ SHUFFLE(o) */ *
FROM orders o JOIN users u ON o.user_id = u.user_id
WHERE o.user_id > 1000;

-- Colocate Join
SELECT /*+ COLOCATE(t1) */ *
FROM orders t1 JOIN order_detail t2 ON t1.order_id = t2.order_id;

-- ============================================
-- 6. 解决数据倾斜
-- 检测倾斜
SELECT user_id, COUNT(*) AS cnt FROM orders
GROUP BY user_id ORDER BY cnt DESC LIMIT 10;

-- ============================================
-- 7. 使用物化视图
CREATE MATERIALIZED VIEW mv_user_stats AS
SELECT
    user_id,
    DATE(create_time) AS stat_date,
    COUNT(*) AS order_count,
    SUM(amount) AS total_amount
FROM orders GROUP BY user_id, DATE(create_time);

-- ============================================
-- 8. 配置查询参数
SET query_timeout = 300;
SET exec_mem_limit = 8589934592;
SET parallel_fragment_exec_instance_num = 4;
SET enable_bucket_shuffle_join = true;
```

### 资源管理最佳实践

```sql
-- ============================================
-- 1. 创建资源组
CREATE RESOURCE GROUP rg_high_priority (
    "cpu_core" = "8",
    "max_memory" = "16GB",
    "concurrency" = "4",
    "max_queue_size" = "20"
);

CREATE RESOURCE GROUP rg_low_priority (
    "cpu_core" = "2",
    "max_memory" = "4GB",
    "concurrency" = "2",
    "max_queue_size" = "50"
);

-- ============================================
-- 2. 设置用户资源组
CREATE USER 'user1'@'%' IDENTIFIED BY 'password'
PROPERTIES ("resource_group" = "rg_high_priority");

ALTER USER 'user1'@'%' PROPERTIES ("resource_group" = "rg_low_priority");

-- ============================================
-- 3. 查询内存管理
SET exec_mem_limit = 8GB;

-- ============================================
-- 4. 并发控制
-- 在 fe.conf 中配置: max_connections=1000

-- ============================================
-- 5. 查询队列
-- fe.conf 中配置: enable_query_queue=true, query_queue_size=100
SET query_queue_timeout = 300;
```

---

## 常见问题

### Q1: 查询慢?

```sql
-- 检查执行计划
EXPLAIN SELECT * FROM orders WHERE user_id = 1001;

-- 检查数据分布
SELECT user_id, COUNT(*) cnt FROM orders
GROUP BY user_id ORDER BY cnt DESC LIMIT 10;

-- 解决方案:
-- 1. 添加合适的 Rollup
ALTER TABLE orders ADD ROLLUP rollup_user (user_id, status);
-- 2. 使用分区键过滤
-- 3. 增加分桶数
-- 4. 解决数据倾斜
```

### Q2: 导入延迟高?

```sql
-- 检查集群负载
SHOW PROC '/cluster_load';

-- 查看导入任务状态
SHOW LOAD;

-- 解决方案:
-- 1. 调大批处理量 (batch.size)
-- 2. 增加导入并行度
-- 3. 增加 BE 节点
-- 4. 使用 SSD 存储
-- 5. 调整 Compaction 策略
```

### Q3: 数据倾斜?

```sql
-- 检查倾斜
SELECT user_id, COUNT(*) cnt FROM orders
GROUP BY user_id ORDER BY cnt DESC LIMIT 10;

-- 解决:
-- 1. 重新选择分桶键
-- 2. 增加分桶数
-- 3. 使用 Bucket Shuffle Join
SELECT /*+ SHUFFLE(t1) */ t1.*, t2.*
FROM orders t1 JOIN users t2 ON t1.user_id = t2.id;

-- 4. 大表分桶 + 小表广播
SELECT /*+ BROADCAST(small_table) */ *
FROM large_table t1 JOIN small_table t2 ON t1.id = t2.id;
```

### Q4: 内存不足?

```sql
-- 在 be.conf 中配置
-- memory_limit=80GB
-- mem_limit=0.8

-- 解决方案:
-- 1. 增加 BE 内存
-- 2. 调小单查询内存限制
SET exec_mem_limit = 4GB;

-- 3. 分批查询大数据量
SELECT * FROM large_table WHERE id BETWEEN 1 AND 1000000;
```

### Q5: 副本不一致?

```sql
-- 检查副本状态
SHOW PROC '/tablet';

-- 解决方案:
-- 1. 修复副本
ALTER SYSTEM repair REPLICA TABLET_ID;

-- 2. 重置副本
ALTER SYSTEM DROP REPLICA TABLET_ID;
ALTER SYSTEM ADD REPLICA TABLET_ID;

-- 3. 检查 BE 状态
SHOW PROC '/backends';
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | Doris 架构详解 |
| [02-data-model.md](02-data-model.md) | 数据模型详解 |
| [03-data-operations.md](03-data-operations.md) | 数据操作指南 |
| [04-sql-reference.md](04-sql-reference.md) | SQL 参考 |
| [README.md](README.md) | 索引文档 |
