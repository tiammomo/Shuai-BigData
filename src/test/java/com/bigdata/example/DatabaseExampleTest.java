package com.bigdata.example;

import org.junit.Test;

import java.sql.*;

import static org.junit.Assert.*;

/**
 * 数据库连接示例测试类 (MySQL/ClickHouse/Doris)
 */
public class DatabaseExampleTest {

    /**
     * 测试 MySQL JDBC 连接
     */
    @Test
    public void testMySQLConnection() throws SQLException {
        String url = "jdbc:mysql://localhost:3306/test";
        String user = "root";
        String password = "root";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            assertNotNull(conn);
            assertTrue(conn.isValid(5));

            // 测试基本查询
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("test"));
        }
    }

    /**
     * 测试 ClickHouse JDBC 连接
     */
    @Test
    public void testClickHouseConnection() throws SQLException {
        String url = "jdbc:clickhouse://localhost:8123/test";
        String user = "root";
        String password = "root";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            assertNotNull(conn);
            assertTrue(conn.isValid(5));

            // 测试基本查询
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("test"));
        }
    }

    /**
     * 测试 Doris JDBC 连接
     */
    @Test
    public void testDorisConnection() throws SQLException {
        String url = "jdbc:mysql://localhost:9030/test";
        String user = "root";
        String password = "";

        try (Connection conn = DriverManager.getConnection(url, user, password)) {
            assertNotNull(conn);
            assertTrue(conn.isValid(5));

            // 测试基本查询
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT 1 as test");
            assertTrue(rs.next());
            assertEquals(1, rs.getInt("test"));
        }
    }

    /**
     * 测试 PreparedStatement
     */
    @Test
    public void testPreparedStatement() throws SQLException {
        String url = "jdbc:h2:mem:test";

        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            // 创建测试表
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE users (id INT, name VARCHAR(100))");

            // 插入数据
            String insertSQL = "INSERT INTO users (id, name) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                pstmt.setInt(1, 1);
                pstmt.setString(2, "John");
                pstmt.executeUpdate();
            }

            // 查询验证
            String selectSQL = "SELECT * FROM users WHERE id = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(selectSQL)) {
                pstmt.setInt(1, 1);

                ResultSet rs = pstmt.executeQuery();
                assertTrue(rs.next());
                assertEquals(1, rs.getInt("id"));
                assertEquals("John", rs.getString("name"));
            }
        }
    }

    /**
     * 测试批处理操作
     */
    @Test
    public void testBatchProcessing() throws SQLException {
        String url = "jdbc:h2:mem:test";

        try (Connection conn = DriverManager.getConnection(url, "sa", "")) {
            // 创建测试表
            Statement stmt = conn.createStatement();
            stmt.execute("CREATE TABLE orders (id INT, amount DECIMAL(10,2))");

            // 批处理插入
            conn.setAutoCommit(false);
            String insertSQL = "INSERT INTO orders (id, amount) VALUES (?, ?)";
            try (PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {
                for (int i = 1; i <= 100; i++) {
                    pstmt.setInt(1, i);
                    pstmt.setDouble(2, i * 10.5);
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
            }
            conn.commit();

            // 验证数量
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM orders");
            assertTrue(rs.next());
            assertEquals(100, rs.getInt(1));
        }
    }
}
