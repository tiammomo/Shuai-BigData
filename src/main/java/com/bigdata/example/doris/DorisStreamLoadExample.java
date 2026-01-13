package com.bigdata.example.doris;

import org.apache.http.HttpHeaders;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.*;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Random;

/**
 * Doris Stream Load Examples - 高效导入数据
 */
public class DorisStreamLoadExample {

    private static final String DORIS_FE = "localhost";
    private static final int DORIS_FE_HTTP_PORT = 8030;
    private static final String DORIS_DB = "test_db";
    private static final String DORIS_USER = "root";
    private static final String DORIS_PASSWORD = "";
    private static final String TABLE = "test_table";

    public static void main(String[] args) throws Exception {
        // 1. Basic Stream Load
        basicStreamLoad();

        // 2. Stream Load with JSON
        streamLoadWithJSON();

        // 3. Stream Load with CSV
        streamLoadWithCSV();

        // 4. Stream Load with Headers
        streamLoadWithHeaders();

        // 5. Sync Stream Load (Wait for result)
        syncStreamLoad();

        // 6. Batch Stream Load
        batchStreamLoad();
    }

    /**
     * 1. 基础Stream Load
     */
    private static void basicStreamLoad() throws Exception {
        String label = "load_" + System.currentTimeMillis();
        String data = "1,Alice,25,95.5\n2,Bob,30,88.0\n3,Charlie,35,92.3";

        String url = String.format(
                "http://%s:%d/api/%s/%s/_stream_load?label=%s",
                DORIS_FE, DORIS_FE_HTTP_PORT, DORIS_DB, TABLE, label
        );

        String response = doHttpPut(url, data, createBasicAuthHeader());
        System.out.println("Basic Stream Load Response: " + response);
    }

    /**
     * 2. Stream Load with JSON格式
     */
    private static void streamLoadWithJSON() throws Exception {
        String label = "load_json_" + System.currentTimeMillis();

        StringBuilder jsonData = new StringBuilder();
        jsonData.append("[\n");
        jsonData.append("  {\"id\":1, \"name\":\"Alice\", \"age\":25, \"score\":95.5},\n");
        jsonData.append("  {\"id\":2, \"name\":\"Bob\", \"age\":30, \"score\":88.0},\n");
        jsonData.append("  {\"id\":3, \"name\":\"Charlie\", \"age\":35, \"score\":92.3}\n");
        jsonData.append("]");

        String url = String.format(
                "http://%s:%d/api/%s/%s/_stream_load?label=%s&format=json&read_json_by_line=false",
                DORIS_FE, DORIS_FE_HTTP_PORT, DORIS_DB, TABLE, label
        );

        String response = doHttpPut(url, jsonData.toString(), createBasicAuthHeader());
        System.out.println("JSON Stream Load Response: " + response);
    }

    /**
     * 3. Stream Load with CSV格式
     */
    private static void streamLoadWithCSV() throws Exception {
        String label = "load_csv_" + System.currentTimeMillis();

        // CSV数据
        StringBuilder csvData = new StringBuilder();
        csvData.append("id,name,age,score\n"); // Header
        csvData.append("1,Alice,25,95.5\n");
        csvData.append("2,Bob,30,88.0\n");
        csvData.append("3,Charlie,35,92.3\n");

        // URL编码参数
        String url = String.format(
                "http://%s:%d/api/%s/%s/_stream_load?label=%s&format=csv&column_separator=,&skip_header=1",
                DORIS_FE, DORIS_FE_HTTP_PORT, DORIS_DB, TABLE,
                URLEncoder.encode(label, StandardCharsets.UTF_8.name())
        );

        String response = doHttpPut(url, csvData.toString(), createBasicAuthHeader());
        System.out.println("CSV Stream Load Response: " + response);
    }

    /**
     * 4. Stream Load with Headers (更多配置)
     */
    private static void streamLoadWithHeaders() throws Exception {
        String label = "load_headers_" + System.currentTimeMillis();
        String data = "4,David,40,78.5\n5,Eve,28,85.0";

        String url = String.format(
                "http://%s:%d/api/%s/%s/_stream_load?label=%s",
                DORIS_FE, DORIS_FE_HTTP_PORT, DORIS_DB, TABLE, label
        );

        // 额外的Header
        String headers = "column_separator:,\n" +
                "line_delimiter:\\n\n" +
                "max_filter_ratio:0.1\n" +
                "strict_mode:false\n" +
                "timezone:Asia/Shanghai";

        String response = doHttpPut(url, data, createBasicAuthHeader(), headers);
        System.out.println("Stream Load with Headers Response: " + response);
    }

    /**
     * 5. 同步Stream Load (等待结果)
     */
    private static void syncStreamLoad() throws Exception {
        String label = "load_sync_" + System.currentTimeMillis();
        String data = "6,Frank,45,91.0\n7,Grace,32,89.5";

        String url = String.format(
                "http://%s:%d/api/%s/%s/_stream_load?label=%s",
                DORIS_FE, DORIS_FE_HTTP_PORT, DORIS_DB, TABLE, label
        );

        // 同步模式会返回结果
        String response = doHttpPut(url, data, createBasicAuthHeader());
        System.out.println("Sync Stream Load Response: " + response);

        // 解析响应检查是否成功
        org.json.JSONObject jsonResponse = new org.json.JSONObject(response);
        if ("Success".equals(jsonResponse.getString("Status"))) {
            System.out.println("Load successful! Number of loaded rows: " +
                    jsonResponse.getJSONObject("LoadResult").getInt("number_loaded_rows"));
        } else {
            System.out.println("Load failed: " + jsonResponse.optString("Message"));
        }
    }

    /**
     * 6. 批量Stream Load (大文件分批)
     */
    private static void batchStreamLoad() throws Exception {
        // 假设有大文件，分批导入
        String filePath = "/path/to/large_file.csv";
        int batchSize = 10000; // 每批10000行

        String labelPrefix = "load_batch_" + System.currentTimeMillis();
        int batchNum = 0;

        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            String line;
            StringBuilder batch = new StringBuilder();
            int lineCount = 0;

            while ((line = reader.readLine()) != null) {
                batch.append(line).append("\n");
                lineCount++;

                if (lineCount >= batchSize) {
                    batchNum++;
                    String label = labelPrefix + "_" + batchNum;
                    String url = String.format(
                            "http://%s:%d/api/%s/%s/_stream_load?label=%s",
                            DORIS_FE, DORIS_FE_HTTP_PORT, DORIS_DB, TABLE, label
                    );

                    String response = doHttpPut(url, batch.toString(), createBasicAuthHeader());
                    System.out.println("Batch " + batchNum + " Response: " + response);

                    // 清空批次
                    batch.setLength(0);
                    lineCount = 0;

                    // 避免太快，短暂sleep
                    Thread.sleep(100);
                }
            }

            // 处理最后一批
            if (lineCount > 0) {
                batchNum++;
                String label = labelPrefix + "_" + batchNum;
                String url = String.format(
                        "http://%s:%d/api/%s/%s/_stream_load?label=%s",
                        DORIS_FE, DORIS_FE_HTTP_PORT, DORIS_DB, TABLE, label
                );

                String response = doHttpPut(url, batch.toString(), createBasicAuthHeader());
                System.out.println("Last Batch " + batchNum + " Response: " + response);
            }
        }
    }

    /**
     * 7. 从HDFS导入
     */
    private static void loadFromHDFS() throws Exception {
        String sql = "LOAD DATA " +
                "INFILE 'hdfs://namenode:8020/data/test.csv' " +
                "INTO TABLE " + TABLE + " " +
                "FORMAT AS 'csv' " +
                "PROPERTIES (" +
                "  'column_separator' = ','," +
                "  'skip_header' = '1'" +
                ")";

        // 通过MySQL客户端执行
        System.out.println("HDFS Load SQL: " + sql);
    }

    /**
     * 8. 从Kafka实时导入 (Routine Load)
     */
    private static void createRoutineLoad() throws Exception {
        // Routine Load用于从Kafka持续导入数据
        String sql = "CREATE ROUTINE LOAD routine_load_test ON " + TABLE + " " +
                "PROPERTIES ( " +
                "  'desired_concurrent_number' = '3'," +
                "  'max_batch_interval' = '20'," +
                "  'max_batch_rows' = '200000'," +
                "  'max_filter_ratio' = '0.1'" +
                ") " +
                "FROM KAFKA ( " +
                "  'kafka_broker_list' = 'localhost:9092'," +
                "  'kafka_topic' = 'test_topic'," +
                "  'kafka_partitions' = '0,1,2'," +
                "  'kafka_offsets' = 'OFFSET_BEGINNING'" +
                ")";

        System.out.println("Routine Load SQL: " + sql);
    }

    // ==================== 辅助方法 ====================

    private static String createBasicAuthHeader() {
        String auth = DORIS_USER + ":" + DORIS_PASSWORD;
        return "Authorization: Basic " + Base64.getEncoder().encodeToString(auth.getBytes());
    }

    private static String doHttpPut(String url, String data, String... headers) throws Exception {
        try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
            HttpPut httpPut = new HttpPut(url);
            httpPut.setEntity(new StringEntity(data, StandardCharsets.UTF_8));
            httpPut.setHeader(HttpHeaders.CONTENT_TYPE, "text/plain; charset=utf-8");
            httpPut.setHeader("Expect", "100-continue");

            for (String header : headers) {
                String[] parts = header.split(":");
                if (parts.length == 2) {
                    httpPut.setHeader(parts[0].trim(), parts[1].trim());
                }
            }

            try (CloseableHttpResponse response = httpClient.execute(httpPut)) {
                return EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
            }
        }
    }

    /**
     * 生成测试数据
     */
    private static String generateTestData(int count) {
        StringBuilder sb = new StringBuilder();
        Random random = new Random();

        for (int i = 0; i < count; i++) {
            sb.append(i + 1).append(",")
                    .append("User").append(i).append(",")
                    .append(random.nextInt(50) + 18).append(",")
                    .append(random.nextDouble() * 100).append("\n");
        }

        return sb.toString();
    }
}
