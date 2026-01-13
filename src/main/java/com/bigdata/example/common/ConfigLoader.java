package com.bigdata.example.common;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * 配置文件加载工具类
 */
public class ConfigLoader {

    private static Properties properties;
    private static JSONObject jsonConfig;

    /**
     * 从properties文件加载配置
     */
    public static Properties loadProperties(String filePath) {
        properties = new Properties();
        try (InputStream is = Files.newInputStream(Paths.get(filePath))) {
            properties.load(is);
            return properties;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load properties file: " + filePath, e);
        }
    }

    /**
     * 从JSON文件加载配置
     */
    public static JSONObject loadJson(String filePath) {
        try {
            String content = new String(Files.readAllBytes(Paths.get(filePath)));
            jsonConfig = JSON.parseObject(content);
            return jsonConfig;
        } catch (IOException e) {
            throw new RuntimeException("Failed to load JSON file: " + filePath, e);
        }
    }

    /**
     * 获取String类型配置
     */
    public static String getString(String key, String defaultValue) {
        return properties != null ? properties.getProperty(key, defaultValue) : defaultValue;
    }

    /**
     * 获取int类型配置
     */
    public static int getInt(String key, int defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Integer.parseInt(value) : defaultValue;
    }

    /**
     * 获取long类型配置
     */
    public static long getLong(String key, long defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Long.parseLong(value) : defaultValue;
    }

    /**
     * 获取double类型配置
     */
    public static double getDouble(String key, double defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Double.parseDouble(value) : defaultValue;
    }

    /**
     * 获取boolean类型配置
     */
    public static boolean getBoolean(String key, boolean defaultValue) {
        String value = properties.getProperty(key);
        return value != null ? Boolean.parseBoolean(value) : defaultValue;
    }

    /**
     * 从JSON获取配置
     */
    public static Object getJson(String key) {
        return jsonConfig != null ? jsonConfig.get(key) : null;
    }

    public static String getJsonString(String key) {
        return jsonConfig != null ? jsonConfig.getString(key) : null;
    }

    public static Integer getJsonInt(String key) {
        return jsonConfig != null ? jsonConfig.getInteger(key) : null;
    }

    /**
     * 初始化配置文件 (放在resources目录下)
     */
    public static void initFromResource(String fileName) {
        try (InputStream is = ConfigLoader.class.getClassLoader().getResourceAsStream(fileName)) {
            if (is == null) {
                throw new RuntimeException("Resource not found: " + fileName);
            }
            properties = new Properties();
            properties.load(is);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load resource: " + fileName, e);
        }
    }
}
