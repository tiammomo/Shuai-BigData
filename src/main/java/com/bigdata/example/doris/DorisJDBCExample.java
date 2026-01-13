package com.bigdata.example.doris;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Doris JDBC API Examples - 通过MySQL协议连接Doris
 */
public class DorisJDBCExample {

    // Doris FE配置
    private static final String DORIS_FE_NODES = "localhost:9030";
    private static final String DORIS_DATABASE = "test_db";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";

    public static void main(String[] args) {
        // 创建连接
        String url = "jdbc:mysql://" + DORIS_FE_NODES + "/" + DORIS_DATABASE +
                "?useUnicode=true&characterEncoding=UTF-8&useSSL=false";

        Properties props = new Properties();
        props.setProperty("user", DORIS_USER);
        props.setProperty("password", DORIS_PASSWORD);

        try (Connection conn = DriverManager.getConnection(url, props)) {
            System.out.println("Connected to Doris successfully!");

            // 1. 创建表
            createTable(conn);

            // 2. 插入数据
            insertData(conn);

            // 3. 批量插入
            batchInsert(conn);

            // 4. 查询数据
            queryData(conn);

            // 5. 更新数据
            updateData(conn);

            // 6. 删除数据
            deleteData(conn);

            // 7. 聚合查询
            aggregateQuery(conn);

            // 8. Join查询
            joinQuery(conn);

            // 9. 子查询
            subQuery(conn);

            // 10. 导出数据
            exportData(conn);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 1. 创建Doris表 (支持多种分桶方式)
     */
    private static void createTable(Connection conn) throws SQLException {
        String sql = "CREATE TABLE IF NOT EXISTS test_table (" +
                "  id BIGINT," +
                "  name VARCHAR(100)," +
                "  age INT," +
                "  score DOUBLE," +
                "  create_time DATETIME," +
                "  dt DATE" +
                ") " +
                // 单分区 (Unique模型)
                "DISTRIBUTED BY HASH(id) BUCKETS 10 " +
                "PROPERTIES (" +
                "  'replication_num' = '1'," +
                "  'enable_unique_key_merge_on_write' = 'true'" +
                ")";

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sql);
            System.out.println("Table created successfully!");
        }

        // 创建Duplicate模型表 (适合日志分析)
        String duplicateSql = "CREATE TABLE IF NOT EXISTS logs_table (" +
                "  id BIGINT," +
                "  level VARCHAR(20)," +
                "  message TEXT," +
                "  ts DATETIME" +
                ") " +
                "DISTRIBUTED BY HASH(id) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";

        // 创建Aggregate模型表 (适合统计报表)
        String aggregateSql = "CREATE TABLE IF NOT EXISTS stats_table (" +
                "  date DATE," +
                "  user_id BIGINT," +
                "  pv BIGINT SUM," +
                "  uv BIGINT SUM" +
                ") " +
                "DISTRIBUTED BY HASH(user_id) BUCKETS 10 " +
                "PARTITION BY RANGE(date) (" +
                "  PARTITION p202401 VALUES [('2024-01-01'), ('2024-02-01'))," +
                "  PARTITION p202402 VALUES [('2024-02-01'), ('2024-03-01'))" +
                ") " +
                "PROPERTIES ('replication_num' = '1')";
    }

    /**
     * 2. 插入数据
     */
    private static void insertData(Connection conn) throws SQLException {
        String sql = "INSERT INTO test_table (id, name, age, score, create_time, dt) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, 1L);
            pstmt.setString(2, "Alice");
            pstmt.setInt(3, 25);
            pstmt.setDouble(4, 95.5);
            pstmt.setTimestamp(5, Timestamp.valueOf("2024-01-10 10:30:00"));
            pstmt.setDate(6, Date.valueOf("2024-01-10"));

            int rows = pstmt.executeUpdate();
            System.out.println("Inserted " + rows + " row(s)");
        }
    }

    /**
     * 3. 批量插入 (高效)
     */
    private static void batchInsert(Connection conn) throws SQLException {
        conn.setAutoCommit(false);

        String sql = "INSERT INTO test_table (id, name, age, score, create_time, dt) " +
                "VALUES (?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            for (int i = 0; i < 100; i++) {
                pstmt.setLong(1, i + 1);
                pstmt.setString(2, "User" + i);
                pstmt.setInt(3, 20 + (i % 10));
                pstmt.setDouble(4, 60.0 + (i % 40));
                pstmt.setTimestamp(5, new Timestamp(System.currentTimeMillis()));
                pstmt.setDate(6, new Date(System.currentTimeMillis()));
                pstmt.addBatch();
            }

            pstmt.executeBatch();
            conn.commit();
            System.out.println("Batch insert completed!");
        } catch (SQLException e) {
            conn.rollback();
            throw e;
        } finally {
            conn.setAutoCommit(true);
        }
    }

    /**
     * 4. 查询数据
     */
    private static void queryData(Connection conn) throws SQLException {
        String sql = "SELECT id, name, age, score FROM test_table WHERE id <= ? ORDER BY id";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, 10L);

            try (ResultSet rs = pstmt.executeQuery()) {
                while (rs.next()) {
                    System.out.printf("id=%d, name=%s, age=%d, score=%.2f%n",
                            rs.getLong("id"),
                            rs.getString("name"),
                            rs.getInt("age"),
                            rs.getDouble("score"));
                }
            }
        }
    }

    /**
     * 5. 更新数据 (Unique模型支持)
     */
    private static void updateData(Connection conn) throws SQLException {
        String sql = "UPDATE test_table SET score = ? WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setDouble(1, 100.0);
            pstmt.setLong(2, 1L);

            int rows = pstmt.executeUpdate();
            System.out.println("Updated " + rows + " row(s)");
        }
    }

    /**
     * 6. 删除数据
     */
    private static void deleteData(Connection conn) throws SQLException {
        String sql = "DELETE FROM test_table WHERE id = ?";

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setLong(1, 1L);

            int rows = pstmt.executeUpdate();
            System.out.println("Deleted " + rows + " row(s)");
        }
    }

    /**
     * 7. 聚合查询
     */
    private static void aggregateQuery(Connection conn) throws SQLException {
        String sql = "SELECT name, COUNT(*) as cnt, AVG(score) as avg_score, " +
                "SUM(score) as total_score " +
                "FROM test_table " +
                "GROUP BY name " +
                "HAVING COUNT(*) > 1 " +
                "ORDER BY cnt DESC " +
                "LIMIT 10";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf("name=%s, count=%d, avg=%.2f, total=%.2f%n",
                        rs.getString("name"),
                        rs.getInt("cnt"),
                        rs.getDouble("avg_score"),
                        rs.getDouble("total_score"));
            }
        }
    }

    /**
     * 8. Join查询
     */
    private static void joinQuery(Connection conn) throws SQLException {
        // 先创建用户表
        String createUsers = "CREATE TABLE IF NOT EXISTS users (" +
                "  id BIGINT, " +
                "  name VARCHAR(100), " +
                "  dept VARCHAR(50)" +
                ") DISTRIBUTED BY HASH(id) BUCKETS 10 " +
                "PROPERTIES ('replication_num' = '1')";
        conn.createStatement().execute(createUsers);

        String sql = "SELECT t.id, t.name, u.dept, t.score " +
                "FROM test_table t " +
                "INNER JOIN users u ON t.id = u.id " +
                "WHERE t.score > 60";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.printf("id=%d, name=%s, dept=%s, score=%.2f%n",
                        rs.getLong("id"),
                        rs.getString("name"),
                        rs.getString("dept"),
                        rs.getDouble("score"));
            }
        }
    }

    /**
     * 9. 子查询
     */
    private static void subQuery(Connection conn) throws SQLException {
        String sql = "SELECT * FROM test_table " +
                "WHERE id IN (SELECT id FROM users WHERE dept = 'Engineering') " +
                "AND score > (SELECT AVG(score) FROM test_table)";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            while (rs.next()) {
                System.out.println("id=" + rs.getLong("id") + ", name=" + rs.getString("name"));
            }
        }
    }

    /**
     * 10. 导出数据 (SELECT INTO OUTFILE)
     */
    private static void exportData(Connection conn) throws SQLException {
        String sql = "SELECT id, name, score " +
                "FROM test_table " +
                "INTO OUTFILE 'hdfs://namenode:8020/export/test.csv' " +
                "FORMAT AS CSV " +
                "PROPERTIES (" +
                "  'broker.name' = 'broker_name'," +
                "  'fs.defaultFS' = 'hdfs://namenode:8020'," +
                "  'hadoop.username' = 'hdfs'" +
                ")";

        try (Statement stmt = conn.createStatement()) {
            boolean result = stmt.execute(sql);
            System.out.println("Export query executed: " + result);
        }
    }

    /**
     * 11. 使用Schema Change
     */
    private static void schemaChange(Connection conn) throws SQLException {
        // 添加列
        String addCol = "ALTER TABLE test_table ADD COLUMN email VARCHAR(100) AFTER name";
        conn.createStatement().execute(addCol);

        // 删除列
        String dropCol = "ALTER TABLE test_table DROP COLUMN email";
        conn.createStatement().execute(dropCol);

        // 修改列类型
        String modifyCol = "ALTER TABLE test_table MODIFY COLUMN name VARCHAR(200)";
        conn.createStatement().execute(modifyCol);

        // 重命名表
        String renameTable = "RENAME TABLE test_table TO test_table_new";
        conn.createStatement().execute(renameTable);
    }

    /**
     * 12. 创建物化视图
     */
    private static void createMaterializedView(Connection conn) throws SQLException {
        String mvSql = "CREATE MATERIALIZED VIEW mv_user_stats " +
                "AS " +
                "SELECT name, COUNT(*) as cnt, SUM(score) as total_score " +
                "FROM test_table " +
                "GROUP BY name";

        conn.createStatement().execute(mvSql);
        System.out.println("Materialized view created!");
    }
}
