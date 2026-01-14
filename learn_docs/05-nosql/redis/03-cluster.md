# Redis 集群部署

## 目录

- [主从复制](#主从复制)
- [哨兵模式](#哨兵模式)
- [集群模式](#集群模式)
- [部署配置](#部署配置)
- [故障转移](#故障转移)

---

## 主从复制

### 配置主从

```bash
# 从节点配置 (redis.conf)
replicaof 192.168.1.100 6379

# 或使用命令
redis-cli replicaof 192.168.1.100 6379

# 查看复制状态
redis-cli info replication
```

### 复制原理

```
┌─────────────────────────────────────────────────────────────────┐
│                    主从复制流程                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. 从节点连接主节点                                             │
│  2. 主节点执行 BGSAVE 生成 RDB                                   │
│  3. 主节点发送 RDB 到从节点                                       │
│  4. 从节点加载 RDB 数据                                          │
│  5. 主节点发送缓冲区增量数据                                      │
│  6. 保持同步                                                     │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 复制配置

```bash
# 复制相关配置
replicaof <master_ip> <master_port>
masterauth <password>           # 主节点密码
replica-serve-stale-data yes    # 从节点是否可读
replica-read-only yes           # 从节点只读
repl-diskless-sync yes          # 无盘复制
repl-backlog-size 64mb          # 复制缓冲区大小
min-replicas-to-write 1         # 最小从节点数
min-replicas-max-lag 10         # 最大延迟秒数
```

---

## 哨兵模式

### 哨兵配置

```bash
# sentinel.conf
# 监控的主节点
sentinel monitor mymaster 192.168.1.100 6379 2

# 密码配置
sentinel auth-pass mymaster <password>

# 判定主观下线的时长
sentinel down-after-milliseconds mymaster 30000

# 故障转移超时时间
sentinel failover-timeout mymaster 180000

# 并行同步数量
sentinel parallel-syncs mymaster 1

# 通知脚本
sentinel notification-script mymaster /path/to/notify.sh
sentinel client-reconfig-script mymaster /path/to/reconfig.sh
```

### 启动哨兵

```bash
# 启动哨兵
redis-sentinel /path/to/sentinel.conf

# 或
redis-server /path/to/sentinel.conf --sentinel
```

### Java 客户端连接

```java
// Jedis 连接哨兵
JedisSentinelPool pool = new JedisSentinelPool(
    "mymaster",
    Set.of("192.168.1.101:26379", "192.168.1.102:26379", "192.168.1.103:26379"),
    new GenericObjectPoolConfig<>(),
    "password"
);

Jedis jedis = pool.getResource();
```

---

## 集群模式

### 创建集群

```bash
# 使用 redis-cli 创建集群
redis-cli --cluster create \
  192.168.1.100:6379 \
  192.168.1.101:6379 \
  192.168.1.102:6379 \
  192.168.1.103:6379 \
  192.168.1.104:6379 \
  192.168.1.105:6379 \
  --cluster-replicas 1

# 或手动配置
redis-server --port 6379 --cluster-enabled yes --cluster-config-file nodes.conf
```

### 集群配置

```bash
# redis.conf
cluster-enabled yes
cluster-config-file nodes.conf
cluster-node-timeout 15000
cluster-require-full-coverage no
cluster-migration-barrier 1
```

### 集群操作

```bash
# 查看集群信息
redis-cli -c cluster info

# 查看节点
redis-cli -c cluster nodes

# 查看槽分布
redis-cli -c cluster slots

# 重新分配槽
redis-cli --cluster reshard 192.168.1.100:6379

# 检查集群
redis-cli --cluster check 192.168.1.100:6379
```

### Java 客户端

```java
// Jedis Cluster
Set<HostAndPort> nodes = Set.of(
    new HostAndPort("192.168.1.100", 6379),
    new HostAndPort("192.168.1.101", 6379),
    new HostAndPort("192.168.1.102", 6379)
);

JedisCluster jedis = new JedisCluster(nodes, 5000, 100, config);

// Spring Boot
@Bean
public RedisClusterConfiguration redisClusterConfiguration() {
    Map<String, Object> source = new HashMap<>();
    source.put("spring.redis.cluster.nodes", "192.168.1.100:6379,192.168.1.101:6379");
    source.put("spring.redis.cluster.timeout", "5000");
    source.put("spring.redis.cluster.max-redirects", "3");
    return new RedisClusterConfiguration(source);
}
```

---

## 部署配置

### 单机配置

```bash
# redis.conf
bind 0.0.0.0
port 6379
daemonize yes
pidfile /var/run/redis/redis.pid
logfile /var/log/redis/redis.log
dir /data/redis

# 内存配置
maxmemory 4gb
maxmemory-policy allkeys-lru

# 持久化
save 900 1
save 300 10
save 60 10000
appendonly yes
appendfsync everysec
```

### Docker 部署

```yaml
# docker-compose.yml
version: '3.8'
services:
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis-data:/data
      - redis-conf/redis.conf:/usr/local/etc/redis/redis.conf
    command: redis-server /usr/local/etc/redis/redis.conf

volumes:
  redis-data:
```

### K8s 部署

```yaml
# redis-statefulset.yaml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: redis
spec:
  serviceName: redis
  replicas: 6
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
      - name: redis
        image: redis:7-alpine
        ports:
        - containerPort: 6379
        volumeMounts:
        - name: data
          mountPath: /data
  volumeClaimTemplates:
  - metadata:
      name: data
    spec:
      accessModes: ["ReadWriteOnce"]
      resources:
        requests:
          storage: 10Gi
```

---

## 故障转移

### 手动故障转移

```bash
# 在从节点上执行
redis-cli -h <slave_ip> -p 6379 failover

# 强制故障转移 (主节点)
redis-cli -h <master_ip> -p 6379 failover takeover
```

### 自动故障转移流程

```
┌─────────────────────────────────────────────────────────────────┐
│                    故障转移流程                                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                 │
│  1. 哨兵检测到主节点主观下线                                      │
│  2. 多个哨兵确认主节点客观下线                                    │
│  3. 哨兵选举领头的哨兵                                           │
│  4. 领头哨兵选择从节点作为新主节点                                │
│  5. 领头哨兵向新主节点执行 SLAVEOF NO ONE                         │
│  6. 领头哨兵更新配置并通知其他哨兵                                │
│  7. 其他哨兵更新配置并指向新主节点                                │
│  8. 旧主节点上线后变为从节点                                      │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

### 故障转移后配置同步

```bash
# 手动重新配置从节点
redis-cli -h <new_slave> SLAVEOF <new_master> 6379

# 检查复制状态
redis-cli -h <new_slave> info replication
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [02-data-operations.md](02-data-operations.md) | 数据操作 |
| [04-optimization.md](04-optimization.md) | 性能优化 |
| [README.md](README.md) | 索引文档 |
