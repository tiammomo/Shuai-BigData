package com.bigdata.example.common;

import java.io.Serializable;
import java.util.Random;

/**
 * 常用工具类集合
 */
public class Utils {

    private static final Random RANDOM = new Random();

    /**
     * 生成随机字符串
     */
    public static String randomString(int length) {
        String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(RANDOM.nextInt(chars.length())));
        }
        return sb.toString();
    }

    /**
     * 生成随机数字字符串
     */
    public static String randomNumeric(int length) {
        String chars = "0123456789";
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(RANDOM.nextInt(chars.length())));
        }
        return sb.toString();
    }

    /**
     * 格式化时间戳
     */
    public static String formatTimestamp(long timestamp, String pattern) {
        java.text.SimpleDateFormat sdf = new java.text.SimpleDateFormat(pattern);
        return sdf.format(new java.util.Date(timestamp));
    }

    /**
     * 解析JSON字符串为对象
     */
    public static <T> T parseJson(String json, Class<T> clazz) {
        return com.alibaba.fastjson.JSON.parseObject(json, clazz);
    }

    /**
     * 对象转JSON字符串
     */
    public static String toJson(Object obj) {
        return com.alibaba.fastjson.JSON.toJSONString(obj);
    }

    /**
     * 对象转JSON字符串(格式化)
     */
    public static String toJsonPretty(Object obj) {
        return com.alibaba.fastjson.JSON.toJSONString(obj, true);
    }

    /**
     * 安全地执行Callable
     */
    public static <T> T safeCall(java.util.concurrent.Callable<T> callable, T defaultValue) {
        try {
            return callable.call();
        } catch (Exception e) {
            return defaultValue;
        }
    }

    /**
     * 重试机制
     */
    public static <T> T retry(int maxRetries, long sleepMs,
                              java.util.concurrent.Callable<T> callable) throws Exception {
        Exception lastException = null;
        for (int i = 0; i < maxRetries; i++) {
            try {
                return callable.call();
            } catch (Exception e) {
                lastException = e;
                if (i < maxRetries - 1) {
                    Thread.sleep(sleepMs);
                }
            }
        }
        throw lastException;
    }

    /**
     * 线程休眠(可中断)
     */
    public static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * 格式化字节大小
     */
    public static String formatBytes(long bytes) {
        if (bytes < 1024) return bytes + " B";
        int exp = (int) (Math.log(bytes) / Math.log(1024));
        String pre = "KMGTPE".charAt(exp - 1) + "B";
        return String.format("%.1f %s", bytes / Math.pow(1024, exp), pre);
    }

    /**
     * 关闭Closeable对象
     */
    public static void closeQuietly(AutoCloseable closeable) {
        if (closeable != null) {
            try {
                closeable.close();
            } catch (Exception e) {
                // ignore
            }
        }
    }
}

/**
 * 泛型Pair类
 */
class Pair<K, V> implements Serializable {
    private K key;
    private V value;

    public Pair(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() { return key; }
    public V getValue() { return value; }

    public static <K, V> Pair<K, V> of(K key, V value) {
        return new Pair<>(key, value);
    }

    @Override
    public String toString() {
        return "Pair{" + key + "=" + value + "}";
    }
}
