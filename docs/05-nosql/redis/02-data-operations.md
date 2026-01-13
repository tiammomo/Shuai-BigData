# Redis 数据操作

## 目录

- [Java 客户端](#java-客户端)
- [Spring 集成](#spring-集成)
- [管道操作](#管道操作)
- [事务操作](#事务操作)
- [Lua 脚本](#lua-脚本)

---

## Java 客户端

### Jedis

```java
// Jedis 连接
Jedis jedis = new Jedis("localhost", 6379);
jedis.auth("password");

// String 操作
jedis.set("key", "value");
String value = jedis.get("key");

// 批量操作
Pipeline pipeline = jedis.pipelined();
pipeline.set("k1", "v1");
pipeline.set("k2", "v2");
pipeline.get("k1");
pipeline.get("k2");
pipeline.syncAndReturnAll();

// 关闭连接
jedis.close();
```

### Lettuce (推荐)

```java
// Lettuce 连接池
RedisClient client = RedisClient.create("redis://localhost:6379");
StatefulRedisConnection<String, String> connection = client.connect();

// 同步操作
RedisCommands<String, String> commands = connection.sync();
commands.set("key", "value");
String value = commands.get("key");

// 异步操作
RedisAsyncCommands<String, String> asyncCommands = connection.async();
asyncCommands.set("key", "value");
RedisFuture<String> future = asyncCommands.get("key");

// 响应式操作
RedisReactiveCommands<String, String> reactive = connection.reactive();
reactive.set("key", "value").block();
```

---

## Spring 集成

### Spring Data Redis

```java
@Configuration
public class RedisConfig {

    @Bean
    public RedisTemplate<String, Object> redisTemplate(RedisConnectionFactory factory) {
        RedisTemplate<String, Object> template = new RedisTemplate<>();
        template.setConnectionFactory(factory);

        // JSON 序列化
        Jackson2JsonRedisSerializer<Object> serializer =
            new Jackson2JsonRedisSerializer<>(Object.class);
        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(serializer);
        template.setHashKeySerializer(new StringRedisSerializer());
        template.setHashValueSerializer(serializer);

        template.afterPropertiesSet();
        return template;
    }
}

// Service 使用
@Service
public class UserService {

    @Autowired
    private RedisTemplate<String, Object> redisTemplate;

    public void saveUser(User user) {
        redisTemplate.opsForValue().set("user:" + user.getId(), user);
    }

    public User getUser(Long id) {
        return (User) redisTemplate.opsForValue().get("user:" + id);
    }
}
```

### Spring Cache 集成

```java
@Configuration
@EnableCaching
public class CacheConfig {

    @Bean
    public RedisCacheManager cacheManager(RedisConnectionFactory factory) {
        return RedisCacheManager.builder(factory)
            .cacheDefaults(
                RedisCacheConfiguration.defaultCacheConfig()
                    .entryTtl(Duration.ofMinutes(10))
            )
            .build();
    }
}

// 使用缓存
@Service
public class UserService {

    @Cacheable(value = "users", key = "#id")
    public User getUser(Long id) {
        return userRepository.findById(id);
    }

    @CachePut(value = "users", key = "#user.id")
    public User updateUser(User user) {
        return userRepository.save(user);
    }

    @CacheEvict(value = "users", key = "#id")
    public void deleteUser(Long id) {
        userRepository.deleteById(id);
    }
}
```

---

## 管道操作

### 批量执行

```java
// 使用 Pipeline 批量操作
public void batchInsert(List<String> keys, List<String> values) {
    try (Pipeline pipeline = jedis.pipelined()) {
        for (int i = 0; i < keys.size(); i++) {
            pipeline.set(keys.get(i), values.get(i));
        }
        pipeline.sync();
    }
}

// 批量读取
public List<String> batchGet(List<String> keys) {
    try (Pipeline pipeline = jedis.pipelined()) {
        List<Response<String>> responses = new ArrayList<>();
        for (String key : keys) {
            responses.add(pipeline.get(key));
        }
        pipeline.sync();

        return responses.stream()
            .map(Response::get)
            .collect(Collectors.toList());
    }
}

// 原子性批量操作 (Lua 脚本)
public String batchSetAtomic(Map<String, String> data) {
    StringBuilder script = new StringBuilder("local result = {} for i,k in ipairs(ARGV) do table.insert(result, redis.call('SET', KEYS[i], k)) end return table.concat(result, ',')");

    List<String> keys = new ArrayList<>(data.keySet());
    List<String> values = new ArrayList<>(data.values());

    return jedis.eval(script.toString(), keys, values.toArray(new String[0])).toString();
}
```

---

## 事务操作

### 基础事务

```java
// 事务操作
public void transfer(Long fromId, Long toId, double amount) {
    jedis.watch("balance:" + fromId);  // 监控

    Transaction tx = jedis.multi();
    tx.decrBy("balance:" + fromId, (long) amount);
    tx.incrBy("balance:" + toId, (long) amount);

    List<Object> results = tx.exec();

    if (results == null) {
        // 事务失败，重试
        transfer(fromId, toId, amount);
    }
}
```

### 分布式锁

```java
public class RedisLock {

    private Jedis jedis;
    private String lockKey;
    private String lockValue;
    private int expireTime;

    public boolean tryLock() {
        String result = jedis.set(lockKey, lockValue, "NX", "EX", expireTime);
        return "OK".equals(result);
    }

    public void unlock() {
        // 使用 Lua 脚本确保只删除自己的锁
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        jedis.eval(script, Collections.singletonList(lockKey), Collections.singletonList(lockValue));
    }
}

// 使用示例
public void executeWithLock(Runnable task) {
    RedisLock lock = new RedisLock(jedis, "lock:task", UUID.randomUUID().toString(), 30);
    try {
        if (lock.tryLock()) {
            task.run();
        }
    } finally {
        lock.unlock();
    }
}
```

---

## Lua 脚本

### 基础使用

```java
// 执行 Lua 脚本
String script = "return redis.call('GET', KEYS[1])";
Object result = jedis.eval(script, Collections.singletonList("key"));

// 带参数的脚本
String script2 = "return redis.call('MGET', unpack(ARGV))";
jedis.eval(script2, Collections.emptyList(), "k1", "k2");

// 预编译脚本
String sha = jedis.scriptLoad(script);
Object result2 = jedis.evalsha(sha, Collections.emptyList());
```

### 常用脚本

```java
// 限流脚本
public boolean rateLimit(String key, int limit, int window) {
    String script =
        "local count = redis.call('INCR', KEYS[1]) " +
        "if count == 1 then redis.call('EXPIRE', KEYS[1], ARGV[1]) end " +
        "return count <= ARGV[2]";

    Object result = jedis.eval(
        script,
        Collections.singletonList(key),
        String.valueOf(window),
        String.valueOf(limit)
    );
    return "1".equals(result.toString()) || "OK".equals(result);
}

// 分布式锁 (带看门狗)
public boolean lockWithWatchdog(String key, String value, long timeout, TimeUnit unit) {
    String script =
        "if redis.call('SET', KEYS[1], ARGV[1], 'NX', 'EX', ARGV[2]) then " +
        "  redis.call('SET', KEYS[1]..':heartbeat', ARGV[1], 'EX', ARGV[2] * 3) " +
        "  return 1 " +
        "else " +
        "  return 0 " +
        "end";

    return "1".equals(jedis.eval(script, Collections.singletonList(key), value, String.valueOf(unit.toSeconds(timeout))));
}
```

---

## 相关文档

| 文档 | 说明 |
|------|------|
| [01-architecture.md](01-architecture.md) | 架构详解 |
| [03-cluster.md](03-cluster.md) | 集群部署 |
| [04-optimization.md](04-optimization.md) | 性能优化 |
| [README.md](README.md) | 索引文档 |
