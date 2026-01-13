package com.bigdata.example.redis;

import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisException;

import java.util.*;

/**
 * Redis API Examples - Jedis客户端
 */
public class RedisExample {

    private static final String HOST = "localhost";
    private static final int PORT = 6379;
    private static final int DB = 0;

    public static void main(String[] args) {
        // 1. 创建连接
        try (Jedis jedis = new Jedis(HOST, PORT)) {
            System.out.println("Connected to Redis!");

            // 基本操作
            stringOperations(jedis);
            listOperations(jedis);
            setOperations(jedis);
            hashOperations(jedis);
            sortedSetOperations(jedis);

            // 管道操作
            pipelineOperations(jedis);

            // 事务操作
            transactionOperations(jedis);

            // 分布式锁
            distributedLock(jedis);

            // 地理位置
            geoOperations(jedis);

            // 消息发布订阅
            pubsubOperations(jedis);

            System.out.println("Redis Examples Completed!");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * String操作
     */
    private static void stringOperations(Jedis jedis) {
        System.out.println("\n=== String Operations ===");

        // SET/GET
        jedis.set("key1", "value1");
        String value = jedis.get("key1");
        System.out.println("key1 = " + value);

        // SETNX (不存在则设置)
        jedis.setnx("key2", "value2");
        jedis.setnx("key2", "value2-updated");
        System.out.println("key2 = " + jedis.get("key2"));

        // SETEX (设置过期时间)
        jedis.setex("temp_key", 10, "will expire");
        System.out.println("temp_key TTL: " + jedis.ttl("temp_key"));

        // INCR/DECR
        jedis.set("counter", "0");
        jedis.incr("counter");
        jedis.incrBy("counter", 5);
        System.out.println("counter = " + jedis.get("counter"));

        // 批量操作
        jedis.mset("k1", "v1", "k2", "v2", "k3", "v3");
        List<String> values = jedis.mget("k1", "k2", "k3");
        System.out.println("mget: " + values);

        // GETSET
        String oldValue = jedis.getSet("key1", "newValue");
        System.out.println("oldValue: " + oldValue + ", newValue: " + jedis.get("key1"));
    }

    /**
     * List操作
     */
    private static void listOperations(Jedis jedis) {
        System.out.println("\n=== List Operations ===");

        // LPUSH/RPUSH
        jedis.lpush("mylist", "a", "b", "c");
        jedis.rpush("mylist", "d", "e");

        // LRANGE
        List<String> list = jedis.lrange("mylist", 0, -1);
        System.out.println("mylist: " + list);

        // LPOP/RPOP
        String leftPop = jedis.lpop("mylist");
        String rightPop = jedis.rpop("mylist");
        System.out.println("LPOP: " + leftPop + ", RPOP: " + rightPop);

        // LINDEX
        String element = jedis.lindex("mylist", 0);
        System.out.println("First element: " + element);

        // LLEN
        long length = jedis.llen("mylist");
        System.out.println("List length: " + length);

        // LTRIM
        jedis.ltrim("mylist", 0, 2);
        System.out.println("After trim: " + jedis.lrange("mylist", 0, -1));
    }

    /**
     * Set操作
     */
    private static void setOperations(Jedis jedis) {
        System.out.println("\n=== Set Operations ===");

        // SADD
        jedis.sadd("myset", "a", "b", "c", "a");
        System.out.println("myset: " + jedis.smembers("myset"));

        // SISMEMBER
        boolean exists = jedis.sismember("myset", "a");
        System.out.println("a in myset: " + exists);

        // SCARD
        long card = jedis.scard("myset");
        System.out.println("Set size: " + card);

        // SINTER (交集)
        jedis.sadd("set1", "a", "b", "c");
        jedis.sadd("set2", "b", "c", "d");
        Set<String> inter = jedis.sinter("set1", "set2");
        System.out.println("Intersection: " + inter);

        // SUNION (并集)
        Set<String> union = jedis.sunion("set1", "set2");
        System.out.println("Union: " + union);

        // SDIFF (差集)
        Set<String> diff = jedis.sdiff("set1", "set2");
        System.out.println("Difference: " + diff);

        // SPOP
        String pop = jedis.spop("myset");
        System.out.println("SPOP: " + pop);
    }

    /**
     * Hash操作
     */
    private static void hashOperations(Jedis jedis) {
        System.out.println("\n=== Hash Operations ===");

        // HSET/HGET
        jedis.hset("myhash", "field1", "value1");
        jedis.hset("myhash", "field2", "value2");
        String fieldValue = jedis.hget("myhash", "field1");
        System.out.println("field1 = " + fieldValue);

        // HMSET/HMGET
        Map<String, String> hashMap = new HashMap<>();
        hashMap.put("name", "Alice");
        hashMap.put("age", "25");
        jedis.hset("user:1", hashMap);

        List<String> values = jedis.hmget("user:1", "name", "age");
        System.out.println("User: " + values);

        // HGETALL
        Map<String, String> all = jedis.hgetAll("user:1");
        System.out.println("All fields: " + all);

        // HINCRBY
        jedis.hincrBy("user:1", "age", 1);
        System.out.println("Age after incr: " + jedis.hget("user:1", "age"));

        // HEXISTS
        boolean exists = jedis.hexists("user:1", "name");
        System.out.println("name exists: " + exists);

        // HDEL
        jedis.hdel("user:1", "name");
        System.out.println("After HDEL: " + jedis.hgetAll("user:1"));
    }

    /**
     * Sorted Set操作
     */
    private static void sortedSetOperations(Jedis jedis) {
        System.out.println("\n=== Sorted Set Operations ===");

        // ZADD
        jedis.zadd("leaderboard", 100, "Alice");
        jedis.zadd("leaderboard", 200, "Bob");
        jedis.zadd("leaderboard", 150, "Charlie");

        // ZRANGE
        List<String> topPlayers = jedis.zrevrange("leaderboard", 0, 2);
        System.out.println("Top 3: " + topPlayers);

        // ZSCORE
        double score = jedis.zscore("leaderboard", "Alice");
        System.out.println("Alice's score: " + score);

        // ZINCRBY
        jedis.zincrby("leaderboard", 50, "Alice");
        System.out.println("Alice's new score: " + jedis.zscore("leaderboard", "Alice"));

        // ZCOUNT
        long count = jedis.zcount("leaderboard", 100, 200);
        System.out.println("Players with score 100-200: " + count);

        // ZRANK
        long rank = jedis.zrank("leaderboard", "Alice");
        System.out.println("Alice's rank: " + rank);

        // ZREMRANGEBYRANK
        jedis.zremrangeByRank("leaderboard", 0, 0); // 移除最低分
        System.out.println("After removing lowest: " + jedis.zrangeWithScores("leaderboard", 0, -1));
    }

    /**
     * 管道操作 (批量执行)
     */
    private static void pipelineOperations(Jedis jedis) {
        System.out.println("\n=== Pipeline Operations ===");

        try (Pipeline pipeline = jedis.pipelined()) {
            for (int i = 0; i < 100; i++) {
                pipeline.set("key-" + i, "value-" + i);
            }
            pipeline.sync(); // 执行所有命令

            System.out.println("Batch insert completed");
        }

        // 带响应
        try (Pipeline pipeline = jedis.pipelined()) {
            Response<Long> setResponse = pipeline.set("a", "1");
            Response<Long> getResponse = pipeline.get("a");

            pipeline.sync();

            System.out.println("SET response: " + setResponse.get());
            System.out.println("GET response: " + getResponse.get());
        }
    }

    /**
     * 事务操作
     */
    private static void transactionOperations(Jedis jedis) {
        System.out.println("\n=== Transaction Operations ===");

        Transaction tx = jedis.multi();
        try {
            tx.set("tx-key1", "tx-value1");
            tx.set("tx-key2", "tx-value2");
            tx.incr("tx-counter");

            List<Object> results = tx.exec();
            System.out.println("Transaction results: " + results);
        } catch (JedisException e) {
            tx.discard();
            System.out.println("Transaction failed: " + e.getMessage());
        }
    }

    /**
     * 分布式锁
     */
    private static void distributedLock(Jedis jedis) {
        System.out.println("\n=== Distributed Lock ===");

        String lockKey = "my-lock";
        String lockValue = UUID.randomUUID().toString();

        // 获取锁 (10秒过期)
        String result = jedis.set(lockKey, lockValue,
                SetParams.setParams().nx().ex(10));

        if ("OK".equals(result)) {
            System.out.println("Lock acquired: " + lockKey);

            try {
                // 模拟业务逻辑
                Thread.sleep(2000);
            } finally {
                // 释放锁 (使用Lua脚本确保原子性)
                String script = "if redis.call('get', KEYS[1]) == ARGV[1] then " +
                        "return redis.call('del', KEYS[1]) else return 0 end";
                jedis.eval(script,
                        Collections.singletonList(lockKey),
                        Collections.singletonList(lockValue));
                System.out.println("Lock released");
            }
        } else {
            System.out.println("Failed to acquire lock");
        }
    }

    /**
     * 地理位置操作
     */
    private static void geoOperations(Jedis jedis) {
        System.out.println("\n=== Geo Operations ===");

        // 添加位置 (经纬度)
        jedis.geoadd("cities", 116.397428, 39.90923, "Beijing");
        jedis.geoadd("cities", 121.473701, 31.230416, "Shanghai");
        jedis.geoadd("cities", 113.264434, 23.129062, "Guangzhou");

        // 计算距离
        double distance = jedis.geodist("cities", "Beijing", "Shanghai", "km");
        System.out.println("Beijing -> Shanghai distance: " + distance + " km");

        // 查找附近的位置
        List<GeoRadiusResponse> nearby = jedis.georadius("cities", 116.397428, 39.90923,
                500, GeoUnit.KM);
        System.out.println("Cities near Beijing (500km): " + nearby);

        // 获取位置经纬度
        List<Point> points = jedis.geopos("cities", "Beijing", "Shanghai");
        System.out.println("Beijing position: " + points.get(0));
    }

    /**
     * 发布订阅
     */
    private static void pubsubOperations(Jedis jedis) {
        System.out.println("\n=== Publish/Subscribe ===");

        // 注意: 发布订阅需要单独连接
        // Subscriber subscriber = new Subscriber();
        // jedis.subscribe(subscriber, "channel1", "channel2");

        // 发布消息
        long subscribers = jedis.publish("channel1", "Hello subscribers!");
        System.out.println("Message sent to " + subscribers + " subscribers");
    }
}
