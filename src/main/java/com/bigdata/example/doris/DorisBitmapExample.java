package com.bigdata.example.doris;

import java.sql.*;
import java.util.Properties;

/**
 * Doris Bitmap 使用示例
 *
 * Doris Bitmap功能:
 * - to_bitmap(): 将整数转换为Bitmap
 * - bitmap_union(): 聚合函数，计算并集
 * - bitmap_intersect(): 计算交集
 * - bitmap_count(): 统计Bitmap中位数
 * - bitmap_hash(): 计算哈希值生成Bitmap
 * - bitmap_empty(): 创建空Bitmap
 * - 子查询中使用Bitmap进行去重统计
 */
public class DorisBitmapExample {

    // Doris FE配置
    private static final String DORIS_FE_NODES = "localhost:9030";
    private static final String DORIS_DATABASE = "test_db";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    public static void main(String[] args) {
        String url = "jdbc:mysql://" + DORIS_FE_NODES + "/" + DORIS_DATABASE +
                "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";

        Properties props = new Properties();
        props.setProperty("user", DORIS_USER);
        props.setProperty("password", DORIS_PASSWORD);

        try (Connection conn = DriverManager.getConnection(url, props)) {
            System.out.println("========================================");
            System.out.println("    Doris Bitmap 使用示例");
            System.out.println("========================================\n");

            // 1. 创建Bitmap表
            createBitmapTable(conn);

            // 2. 基础Bitmap函数使用
            basicBitmapFunctions(conn);

            // 3. UV统计 - 页面访问去重
            uvStatistics(conn);

            // 4. 多维度UV统计
            multiDimensionUV(conn);

            // 5. Bitmap交并集运算
            bitmapOperations(conn);

            // 6. Bitmap子查询用法
            bitmapSubquery(conn);

            // 7. 实时UV计算 (增量)
            realtimeUV(conn);

            // 8. 留存分析
            retentionAnalysis(conn);

            // 9. 漏斗分析
            funnelAnalysis(conn);

            // 10. Bitmap与Join结合
            bitmapWithJoin(conn);

            System.out.println("\n========================================");
            System.out.println("    Doris Bitmap 示例执行完成!");
            System.out.println("========================================");

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 1. 创建Bitmap类型表
     *
     * Bitmap使用场景:
     * - 高基数去重 (百万级唯一值)
     * - UV统计 (独立访客数)
     * - 用户行为分析
     * - 标签圈选
     */
    private static void createBitmapTable(Connection conn) throws SQLException {
        System.out.println("【1】创建Bitmap类型表");
        System.out.println("----------------");

        // 创建用户访问日志表 (使用Aggregate模型 + Bitmap)
        String visitLogSql = "CREATE TABLE IF NOT EXISTS user_visit_log (" +
                "  date DATE," +
                "  user_id BIGINT," +
                "  page_id VARCHAR(50)," +
                "  action_type VARCHAR(20)," +
                "  device_id VARCHAR(100)" +
                ") " +
                "DISTRIBUTED BY HASH(user_id) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";

        conn.createStatement().execute(visitLogSql);
        System.out.println("用户访问日志表创建成功");

        // 创建Bitmap聚合表 (用于UV统计)
        String uvTableSql = "CREATE TABLE IF NOT EXISTS daily_uv_stats (" +
                "  date DATE," +
                "  site_id VARCHAR(20)," +
                "  uv_count BIGINT SUM," +
                "  user_bitmap BITMAP_UNION(user_id)" +
                ") " +
                "AGGREGATE KEY(date, site_id) " +
                "DISTRIBUTED BY HASH(site_id) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";

        conn.createStatement().execute(uvTableSql);
        System.out.println("UV统计表创建成功");

        // 创建设备级UV表
        String deviceUvSql = "CREATE TABLE IF NOT EXISTS device_uv_stats (" +
                "  date DATE," +
                "  platform VARCHAR(20),"  // iOS, Android, Web
                ") " +
                "DISTRIBUTED BY HASH(platform) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";

        conn.createStatement().execute(deviceUvSql);
        System.out.println("设备UV统计表创建成功\n");
    }

    /**
     * 2. 基础Bitmap函数
     *
     * 主要函数:
     * - to_bitmap(value): 将BIGINT转换为Bitmap
     * - bitmap_empty(): 创建空Bitmap
     * - bitmap_hash(value): 计算哈希值生成Bitmap
     * - bitmap_count(bitmap): 统计Bitmap中位数数量
     */
    private static void basicBitmapFunctions(Connection conn) throws SQLException {
        System.out.println("【2】基础Bitmap函数");
        System.out.println("----------------");

        // to_bitmap: 将整数转换为Bitmap
        String toBitmapSql = "SELECT " +
                "to_bitmap(1001) AS bitmap_1001, " +
                "bitmap_count(to_bitmap(1001)) AS count_1, " +
                "bitmap_count(bitmap_empty()) AS empty_count";

        executeAndPrint(conn, toBitmapSql, "to_bitmap函数");

        // bitmap_hash: 基于哈希的Bitmap (适合字符串转Bitmap)
        String hashSql = "SELECT " +
                "bitmap_count(bitmap_hash('user_abc')) AS hash_count, " +
                "bitmap_count(bitmap_hash('user_abc')) AS hash_count_2";

        executeAndPrint(conn, hashSql, "bitmap_hash函数");

        // 多值转Bitmap并统计
        String multiSql = "SELECT " +
                "bitmap_count(to_bitmap(1) | to_bitmap(2) | to_bitmap(3)) AS multi_count";

        executeAndPrint(conn, multiSql, "多值Bitmap");

        System.out.println("Bitmap特点:");
        System.out.println("  - 存储空间: 相比COUNT(DISTINCT)节省80%+");
        System.out.println("  - 计算效率: 位运算，速度快");
        System.out.println("  - 适用场景: 百万级高基数去重\n");
    }

    /**
     * 3. UV统计 - 页面访问去重
     *
     * 业务场景: 统计每天每个页面的独立访客数(UV)
     *
     * 传统方式: SELECT COUNT(DISTINCT user_id) FROM ...
     * Bitmap方式: SELECT bitmap_count(bitmap_union(user_bitmap)) FROM ...
     */
    private static void uvStatistics(Connection conn) throws SQLException {
        System.out.println("【3】UV统计 - 页面访问去重");
        System.out.println("----------------");

        // 插入测试数据
        String insertSql = "INSERT INTO user_visit_log (date, user_id, page_id, action_type, device_id) VALUES " +
                "('2024-01-10', 1001, 'home', 'view', 'device_001')," +
                "('2024-01-10', 1002, 'home', 'view', 'device_002')," +
                "('2024-01-10', 1001, 'product', 'view', 'device_001')," +
                "('2024-01-10', 1003, 'product', 'view', 'device_003')," +
                "('2024-01-10', 1001, 'home', 'view', 'device_001')," +
                "('2024-01-10', 1002, 'cart', 'view', 'device_002')";

        conn.createStatement().execute(insertSql);
        System.out.println("测试数据插入成功");

        // 方式一: 使用COUNT(DISTINCT) - 传统方式
        String distinctSql = "SELECT page_id, COUNT(DISTINCT user_id) AS uv " +
                "FROM user_visit_log " +
                "WHERE date = '2024-01-10' " +
                "GROUP BY page_id";

        System.out.println("\n传统方式 COUNT(DISTINCT):");
        executeAndPrint(conn, distinctSql, "UV统计");

        // 方式二: 使用Bitmap聚合
        String bitmapSql = "SELECT " +
                "page_id, " +
                "bitmap_count(bitmap_union(to_bitmap(user_id))) AS uv " +
                "FROM user_visit_log " +
                "WHERE date = '2024-01-10' " +
                "GROUP BY page_id";

        System.out.println("\nBitmap方式 bitmap_union:");
        executeAndPrint(conn, bitmapSql, "UV统计");

        System.out.println("\n两种方式对比:");
        System.out.println("  - COUNT(DISTINCT): 简单直接，但大基数时性能差");
        System.out.println("  - Bitmap: 适合增量聚合，预聚合存储\n");
    }

    /**
     * 4. 多维度UV统计
     *
     * 场景: 按日期、站点、平台等多个维度统计UV
     */
    private static void multiDimensionUV(Connection conn) throws SQLException {
        System.out.println("【4】多维度UV统计");
        System.out.println("----------------");

        // 创建多维度UV表
        String createSql = "CREATE TABLE IF NOT EXISTS multi_dim_uv (" +
                "  date DATE," +
                "  site_id VARCHAR(20)," +
                "  platform VARCHAR(20)," +
                "  channel VARCHAR(20)," +
                "  user_bitmap BITMAP_UNION(to_bitmap(user_id))" +
                ") " +
                "AGGREGATE KEY(date, site_id, platform, channel) " +
                "DISTRIBUTED BY HASH(site_id) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";

        conn.createStatement().execute(createSql);

        // 插入数据时自动聚合
        String insertSql = "INSERT INTO multi_dim_uv (date, site_id, platform, channel, user_id) " +
                "SELECT date, 'site_a', 'iOS', 'appstore', user_id FROM user_visit_log " +
                "UNION ALL " +
                "SELECT date, 'site_a', 'Android', 'googleplay', user_id + 1000 FROM user_visit_log";

        conn.createStatement().execute(insertSql);
        System.out.println("多维度数据插入成功");

        // 查询各维度UV
        String querySql = "SELECT " +
                "date, " +
                "site_id, " +
                "platform, " +
                "channel, " +
                "bitmap_count(user_bitmap) AS uv " +
                "FROM multi_dim_uv " +
                "GROUP BY date, site_id, platform, channel";

        executeAndPrint(conn, querySql, "多维度UV统计");

        // 查询总UV (跨平台去重)
        String totalUvSql = "SELECT " +
                "date, " +
                "site_id, " +
                "bitmap_count(user_bitmap) AS total_uv " +
                "FROM multi_dim_uv " +
                "GROUP BY date, site_id";

        System.out.println("\n跨平台总UV:");
        executeAndPrint(conn, totalUvSql, "总UV");

        System.out.println("\n多维度分析价值:");
        System.out.println("  - 支持任意维度组合查询");
        System.out.println("  - 预聚合减少计算量");
        System.out.println("  - 支持下钻分析\n");
    }

    /**
     * 5. Bitmap交并集运算
     *
     * 业务场景:
     * - 交集: 访问过A页面又访问过B页面的用户
     * - 并集: 访问过A或B页面的用户总数
     * - 差集: 访问过A但没访问过B的用户
     */
    private static void bitmapOperations(Connection conn) throws SQLException {
        System.out.println("【5】Bitmap交并集运算");
        System.out.println("----------------");

        // 计算交集: 同时访问home和product的用户
        String intersectSql = "SELECT " +
                "bitmap_count(bitmap_intersect(" +
                "  (SELECT bitmap_union(to_bitmap(user_id)) FROM user_visit_log WHERE page_id = 'home'), " +
                "  (SELECT bitmap_union(to_bitmap(user_id)) FROM user_visit_log WHERE page_id = 'product')" +
                ")) AS both_home_product";

        executeAndPrint(conn, intersectSql, "交集 - 同时访问home和product的用户");

        // 计算并集: 访问过任一页面的用户总数
        String unionSql = "SELECT " +
                "bitmap_count(bitmap_union(" +
                "  (SELECT to_bitmap(user_id) FROM user_visit_log WHERE page_id = 'home'), " +
                "  (SELECT to_bitmap(user_id) FROM user_visit_log WHERE page_id = 'product')" +
                ")) AS total_users";

        executeAndPrint(conn, unionSql, "并集 - 访问任一页面的用户");

        // 使用子查询计算交集
        String subQueryIntersect = "SELECT " +
                "COUNT(DISTINCT CASE WHEN page_id = 'home' THEN user_id END) AS home_users, " +
                "COUNT(DISTINCT CASE WHEN page_id = 'product' THEN user_id END) AS product_users, " +
                "COUNT(DISTINCT CASE WHEN page_id IN ('home', 'product') THEN user_id END) AS either_users " +
                "FROM user_visit_log";

        executeAndPrint(conn, subQueryIntersect, "子查询方式统计");

        // Bitmap聚合后计算
        String aggIntersect = "WITH home_users AS (" +
                "  SELECT bitmap_union(to_bitmap(user_id)) AS bitmap FROM user_visit_log WHERE page_id = 'home'" +
                "), product_users AS (" +
                "  SELECT bitmap_union(to_bitmap(user_id)) AS bitmap FROM user_visit_log WHERE page_id = 'product'" +
                ") " +
                "SELECT " +
                "  (SELECT bitmap_count(bitmap) FROM home_users) AS home_uv, " +
                "  (SELECT bitmap_count(bitmap) FROM product_users) AS product_uv, " +
                "  (SELECT bitmap_count(bitmap_union(h.bitmap, p.bitmap)) FROM home_users h, product_users p) AS total_uv";

        System.out.println("\nBitmap聚合方式:");
        executeAndPrint(conn, aggIntersect, "聚合计算");

        System.out.println("\n交并集应用场景:");
        System.out.println("  - 交集: 共同好友、共同兴趣、转化路径");
        System.out.println("  - 并集: 覆盖用户、累计UV");
        System.out.println("  - 差集: 流失用户、新增用户\n");
    }

    /**
     * 6. Bitmap子查询用法
     *
     * 子查询中预计算Bitmap，然后在主查询中使用
     */
    private static void bitmapSubquery(Connection conn) throws SQLException {
        System.out.println("【6】Bitmap子查询用法");
        System.out.println("----------------");

        // 在子查询中计算各渠道的Bitmap
        String subquerySql = "SELECT " +
                "channel, " +
                "bitmap_count(user_bitmap) AS channel_uv " +
                "FROM (" +
                "  SELECT " +
                "    channel, " +
                "    bitmap_union(to_bitmap(user_id)) AS user_bitmap " +
                "  FROM user_visit_log " +
                "  GROUP BY channel" +
                ") t " +
                "ORDER BY channel_uv DESC";

        executeAndPrint(conn, subquerySql, "子查询分组统计");

        // 使用EXISTS + Bitmap
        String existsSql = "SELECT COUNT(DISTINCT user_id) AS active_users " +
                "FROM user_visit_log " +
                "WHERE EXISTS (" +
                "  SELECT 1 FROM user_visit_log t2 " +
                "  WHERE t2.user_id = user_visit_log.user_id " +
                "  AND t2.page_id = 'product'" +
                ")";

        executeAndPrint(conn, existsSql, "活跃用户(访问过product页面)");

        // 嵌套Bitmap计算
        String nestedSql = "SELECT " +
                "date, " +
                "bitmap_count(" +
                "  bitmap_union(" +
                "    CASE WHEN page_id = 'home' THEN to_bitmap(user_id) ELSE bitmap_empty() END" +
                "  )" +
                ") AS home_visitors " +
                "FROM user_visit_log " +
                "GROUP BY date";

        executeAndPrint(conn, nestedSql, "嵌套Bitmap计算");

        System.out.println("\n子查询技巧:");
        System.out.println("  - 先在子查询中聚合Bitmap");
        System.out.println("  - 主查询中使用预聚合结果");
        System.out.println("  - 减少重复计算\n");
    }

    /**
     * 7. 实时UV计算 (增量计算)
     *
     * 场景: 每小时/每天增量更新UV，支持实时看板
     */
    private static void realtimeUV(Connection conn) throws SQLException {
        System.out.println("【7】实时UV计算 (增量)");
        System.out.println("----------------");

        // 创建增量UV表
        String createSql = "CREATE TABLE IF NOT EXISTS incremental_uv (" +
                "  date DATE," +
                "  hour INT," +
                "  user_bitmap BITMAP_UNION(to_bitmap(user_id))," +
                "  last_update DATETIME" +
                ") " +
                "AGGREGATE KEY(date, hour) " +
                "DISTRIBUTED BY HASH(date) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";

        conn.createStatement().execute(createSql);

        // 模拟增量数据
        String insertSql = "INSERT INTO incremental_uv (date, hour, user_id, last_update) VALUES " +
                "('2024-01-10', 10, 2001, NOW())," +
                "('2024-01-10', 10, 2002, NOW())," +
                "('2024-01-10', 10, 2001, NOW())," +
                "('2024-01-10', 11, 2003, NOW())," +
                "('2024-01-10', 11, 2001, NOW())";

        conn.createStatement().execute(insertSql);
        System.out.println("增量数据插入成功");

        // 查询每小时UV
        String hourlySql = "SELECT " +
                "date, " +
                "hour, " +
                "bitmap_count(user_bitmap) AS hourly_uv, " +
                "last_update " +
                "FROM incremental_uv " +
                "ORDER BY date, hour";

        executeAndPrint(conn, hourlySql, "每小时UV");

        // 查询日累计UV (跨小时去重)
        String dailySql = "SELECT " +
                "date, " +
                "bitmap_count(user_bitmap) AS daily_uv " +
                "FROM (" +
                "  SELECT date, bitmap_union(user_bitmap) AS user_bitmap " +
                "  FROM incremental_uv " +
                "  WHERE date = '2024-01-10' " +
                "  GROUP BY date" +
                ") t";

        System.out.println("\n日累计UV (跨小时去重):");
        executeAndPrint(conn, dailySql, "日UV");

        // 查询最近N小时的UV趋势
        String trendSql = "SELECT " +
                "hour, " +
                "bitmap_count(user_bitmap) AS uv, " +
                "bitmap_count(user_bitmap) - LAG(bitmap_count(user_bitmap), 1, 0) OVER (ORDER BY hour) AS diff " +
                "FROM incremental_uv " +
                "WHERE date = '2024-01-10' " +
                "ORDER BY hour";

        executeAndPrint(conn, trendSql, "UV变化趋势");

        System.out.println("\n实时UV优势:");
        System.out.println("  - 支持秒级更新");
        System.out.println("  - 增量计算，减少资源消耗");
        System.out.println("  - 跨时段去重准确\n");
    }

    /**
     * 8. 留存分析 (Retention Analysis)
     *
     * 场景: 计算用户次日留存、7日留存等
     * 使用Bitmap计算两个时间段的交集
     */
    private static void retentionAnalysis(Connection conn) throws SQLException {
        System.out.println("【8】留存分析");
        System.out.println("----------------");

        // 创建留存分析表
        String createSql = "CREATE TABLE IF NOT EXISTS user_retention (" +
                "  date DATE," +
                "  user_id BIGINT," +
                "  active_status VARCHAR(20)" +
                ") " +
                "DISTRIBUTED BY HASH(user_id) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";

        conn.createStatement().execute(createSql);

        // 插入测试数据: 连续活跃的用户
        String insertSql = "INSERT INTO user_retention (date, user_id, active_status) VALUES " +
                "('2024-01-08', 3001, 'active')," +
                "('2024-01-09', 3001, 'active')," +
                "('2024-01-08', 3002, 'active')," +
                "('2024-01-09', 3002, 'inactive')," +  // 流失用户
                "('2024-01-08', 3003, 'active')," +
                "('2024-01-09', 3003, 'active')";

        conn.createStatement().execute(insertSql);
        System.out.println("留存测试数据插入成功");

        // 计算次日留存 (Day 1 Retention)
        String retentionSql = "WITH day1_users AS (" +
                "  SELECT to_bitmap(user_id) AS bitmap " +
                "  FROM user_retention WHERE date = '2024-01-08'" +
                "), day2_users AS (" +
                "  SELECT to_bitmap(user_id) AS bitmap " +
                "  FROM user_retention WHERE date = '2024-01-09' AND active_status = 'active'" +
                ") " +
                "SELECT " +
                "  (SELECT bitmap_count(bitmap) FROM day1_users) AS day1_users," +
                "  (SELECT bitmap_count(bitmap) FROM day2_users) AS day2_active," +
                "  (SELECT bitmap_count(bitmap_intersect(d1.bitmap, d2.bitmap)) " +
                "   FROM day1_users d1, day2_users d2) AS retained";

        executeAndPrint(conn, retentionSql, "次日留存计算");

        // 计算留存率
        String rateSql = "WITH base_users AS (" +
                "  SELECT COUNT(DISTINCT user_id) AS cnt FROM user_retention WHERE date = '2024-01-08'" +
                "), retained_users AS (" +
                "  SELECT COUNT(DISTINCT r.user_id) AS cnt " +
                "  FROM user_retention r " +
                "  WHERE r.date = '2024-01-09' " +
                "  AND r.active_status = 'active' " +
                "  AND EXISTS (SELECT 1 FROM user_retention r2 WHERE r2.user_id = r.user_id AND r2.date = '2024-01-08')" +
                ") " +
                "SELECT " +
                "  (SELECT cnt FROM base_users) AS total_day1," +
                "  (SELECT cnt FROM retained_users) AS retained," +
                "  ROUND((SELECT cnt FROM retained_users) * 100.0 / (SELECT cnt FROM base_users), 2) AS retention_rate";

        System.out.println("\n留存率:");
        executeAndPrint(conn, rateSql, "留存率计算");

        System.out.println("\n留存分析价值:");
        System.out.println("  - 衡量产品粘性");
        System.out.println("  - 评估运营活动效果");
        System.out.println("  - 发现流失用户特征\n");
    }

    /**
     * 9. 漏斗分析 (Funnel Analysis)
     *
     * 场景: 计算用户从 浏览 -> 加购 -> 下单 -> 支付 的转化率
     */
    private static void funnelAnalysis(Connection conn) throws SQLException {
        System.out.println("【9】漏斗分析");
        System.out.println("----------------");

        // 创建漏斗数据表
        String createSql = "CREATE TABLE IF NOT EXISTS funnel_events (" +
                "  date DATE," +
                "  user_id BIGINT," +
                "  event_type VARCHAR(20),"  // view, cart, order, pay
                ") " +
                "DISTRIBUTED BY HASH(user_id) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";

        conn.createStatement().execute(createSql);

        // 插入漏斗测试数据
        String insertSql = "INSERT INTO funnel_events (date, user_id, event_type) VALUES " +
                "('2024-01-10', 4001, 'view')," +
                "('2024-01-10', 4001, 'cart')," +
                "('2024-01-10', 4001, 'order')," +
                "('2024-01-10', 4001, 'pay')," +
                "('2024-01-10', 4002, 'view')," +
                "('2024-01-10', 4002, 'cart')," +
                "('2024-01-10', 4002, 'order')," +
                "('2024-01-10', 4003, 'view')," +
                "('2024-01-10', 4004, 'view')," +
                "('2024-01-10', 4004, 'cart')";

        conn.createStatement().execute(insertSql);
        System.out.println("漏斗测试数据插入成功");

        // 传统方式计算漏斗
        String funnelSql = "SELECT " +
                "COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS view_users," +
                "COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS cart_users," +
                "COUNT(DISTINCT CASE WHEN event_type = 'order' THEN user_id END) AS order_users," +
                "COUNT(DISTINCT CASE WHEN event_type = 'pay' THEN user_id END) AS pay_users " +
                "FROM funnel_events " +
                "WHERE date = '2024-01-10'";

        executeAndPrint(conn, funnelSql, "漏斗转化数据");

        // 使用Bitmap计算各步骤用户
        String bitmapFunnel = "SELECT " +
                "bitmap_count(view_bitmap) AS view_count, " +
                "bitmap_count(cart_bitmap) AS cart_count, " +
                "bitmap_count(order_bitmap) AS order_count, " +
                "bitmap_count(pay_bitmap) AS pay_count " +
                "FROM (" +
                "  SELECT " +
                "    bitmap_union(CASE WHEN event_type = 'view' THEN to_bitmap(user_id) ELSE NULL END) AS view_bitmap," +
                "    bitmap_union(CASE WHEN event_type = 'cart' THEN to_bitmap(user_id) ELSE NULL END) AS cart_bitmap," +
                "    bitmap_union(CASE WHEN event_type = 'order' THEN to_bitmap(user_id) ELSE NULL END) AS order_bitmap," +
                "    bitmap_union(CASE WHEN event_type = 'pay' THEN to_bitmap(user_id) ELSE NULL END) AS pay_bitmap " +
                "  FROM funnel_events " +
                "  WHERE date = '2024-01-10'" +
                ") t";

        executeAndPrint(conn, bitmapFunnel, "Bitmap漏斗统计");

        // 计算转化率
        String conversionSql = "SELECT " +
                "ROUND(cart_count * 100.0 / NULLIF(view_count, 0), 2) AS view_to_cart_rate," +
                "ROUND(order_count * 100.0 / NULLIF(cart_count, 0), 2) AS cart_to_order_rate," +
                "ROUND(pay_count * 100.0 / NULLIF(order_count, 0), 2) AS order_to_pay_rate," +
                "ROUND(pay_count * 100.0 / NULLIF(view_count, 0), 2) AS overall_rate " +
                "FROM (" +
                "  SELECT " +
                "    COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS view_count," +
                "    COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS cart_count," +
                "    COUNT(DISTINCT CASE WHEN event_type = 'order' THEN user_id END) AS order_count," +
                "    COUNT(DISTINCT CASE WHEN event_type = 'pay' THEN user_id END) AS pay_count " +
                "  FROM funnel_events " +
                "  WHERE date = '2024-01-10'" +
                ") t";

        System.out.println("\n转化率:");
        executeAndPrint(conn, conversionSql, "转化率计算");

        System.out.println("\n漏斗分析价值:");
        System.out.println("  - 发现转化瓶颈");
        System.out.println("  - 优化用户路径");
        System.out.println("  - 提升整体转化率\n");
    }

    /**
     * 10. Bitmap与Join结合使用
     *
     * 场景: 将用户标签与行为数据Join后进行Bitmap聚合
     */
    private static void bitmapWithJoin(Connection conn) throws SQLException {
        System.out.println("【10】Bitmap与Join结合");
        System.out.println("----------------");

        // 创建用户标签表
        String tagTableSql = "CREATE TABLE IF NOT EXISTS user_tags (" +
                "  user_id BIGINT," +
                "  age_group VARCHAR(20)," +
                "  gender VARCHAR(10)," +
                "  interest VARCHAR(50)" +
                ") " +
                "DISTRIBUTED BY HASH(user_id) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";

        conn.createStatement().execute(tagTableSql);

        // 插入用户标签数据
        String tagInsertSql = "INSERT INTO user_tags (user_id, age_group, gender, interest) VALUES " +
                "(1001, '18-24', 'female', 'fashion')," +
                "(1002, '25-34', 'male', 'tech')," +
                "(1003, '18-24', 'female', 'tech')," +
                "(1004, '35-44', 'male', 'sports')," +
                "(1005, '25-34', 'female', 'fashion')";

        conn.createStatement().execute(tagInsertSql);
        System.out.println("用户标签数据插入成功");

        // Join后按标签维度统计UV
        String joinSql = "SELECT " +
                "t.age_group, " +
                "t.gender, " +
                "COUNT(DISTINCT v.user_id) AS uv " +
                "FROM user_visit_log v " +
                "INNER JOIN user_tags t ON v.user_id = t.user_id " +
                "WHERE v.date = '2024-01-10' " +
                "GROUP BY t.age_group, t.gender " +
                "ORDER BY uv DESC";

        executeAndPrint(conn, joinSql, "按用户标签统计UV");

        // 使用Bitmap聚合 (更高效)
        String bitmapJoinSql = "SELECT " +
                "t.age_group, " +
                "t.gender, " +
                "bitmap_count(bitmap_union(to_bitmap(v.user_id))) AS uv " +
                "FROM user_visit_log v " +
                "INNER JOIN user_tags t ON v.user_id = t.user_id " +
                "WHERE v.date = '2024-01-10' " +
                "GROUP BY t.age_group, t.gender";

        executeAndPrint(conn, bitmapJoinSql, "Bitmap按标签统计UV");

        // 多标签组合分析
        String multiTagSql = "SELECT " +
                "t.interest, " +
                "COUNT(DISTINCT v.user_id) AS uv, " +
                "COUNT(*) AS total_visits " +
                "FROM user_visit_log v " +
                "INNER JOIN user_tags t ON v.user_id = t.user_id " +
                "GROUP BY t.interest " +
                "ORDER BY uv DESC";

        System.out.println("\n按兴趣标签分析:");
        executeAndPrint(conn, multiTagSql, "兴趣标签分析");

        System.out.println("\nBitmap + Join应用场景:");
        System.out.println("  - 用户画像分析");
        System.out.println("  - 标签圈选用户");
        System.out.println("  - 人群特征分析\n");
    }

    /**
     * 辅助方法: 执行SQL并打印结果
     */
    private static void executeAndPrint(Connection conn, String sql, String label) throws SQLException {
        System.out.println("\n" + label + ":");
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();

            // 打印列名
            StringBuilder header = new StringBuilder("  ");
            for (int i = 1; i <= columnCount; i++) {
                header.append(String.format("%-15s", metaData.getColumnName(i)));
            }
            System.out.println(header);

            // 打印分隔线
            System.out.println("  " + "-".repeat(columnCount * 15));

            // 打印数据行
            while (rs.next()) {
                StringBuilder row = new StringBuilder("  ");
                for (int i = 1; i <= columnCount; i++) {
                    Object value = rs.getObject(i);
                    row.append(String.format("%-15s", value != null ? value.toString() : "NULL"));
                }
                System.out.println(row);
            }
        } catch (SQLException e) {
            System.out.println("  查询执行失败: " + e.getMessage());
        }
    }
}
