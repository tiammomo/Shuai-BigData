package com.bigdata.example.flume;

import org.apache.flume.Event;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.api.RpcClient;
import org.apache.flume.api.RpcClientFactory;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.avro.AvroFlumeEvent;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Flume SDK Examples - 使用Flume SDK发送数据
 */
public class FlumeExample {

    // Flume Agent配置
    private static final String HOST = "localhost";
    private static final int PORT = 41414;

    public static void main(String[] args) {
        // 创建RPC客户端
        RpcClient client = RpcClientFactory.getDefaultInstance(
                HOST, PORT, 1000);

        try {
            System.out.println("Flume Examples Starting...");

            // 1. 发送单个Event
            sendSingleEvent(client);

            // 2. 发送批量Events
            sendBatchEvents(client);

            // 3. 发送带Headers的Event
            sendEventWithHeaders(client);

            // 4. 使用Thrift客户端
            thriftClientExample();

            System.out.println("Flume Examples Completed!");

        } finally {
            client.close();
        }
    }

    /**
     * 发送单个Event
     */
    private static void sendSingleEvent(RpcClient client) {
        System.out.println("\n=== Send Single Event ===");

        // 创建Event
        String body = "This is a test message from Flume SDK";
        Event event = EventBuilder.withBody(body, StandardCharsets.UTF_8);

        try {
            client.append(event);
            System.out.println("Event sent successfully: " + body);
        } catch (EventDeliveryException e) {
            System.err.println("Failed to send event: " + e.getMessage());
        }
    }

    /**
     * 发送批量Events
     */
    private static void sendBatchEvents(RpcClient client) {
        System.out.println("\n=== Send Batch Events ===");

        List<Event> events = new ArrayList<>();
        for (int i = 1; i <= 10; i++) {
            String body = "Batch message #" + i;
            Event event = EventBuilder.withBody(body, StandardCharsets.UTF_8);
            events.add(event);
        }

        try {
            client.appendBatch(events);
            System.out.println("Batch of " + events.size() + " events sent successfully");
        } catch (EventDeliveryException e) {
            System.err.println("Failed to send batch: " + e.getMessage());
        }
    }

    /**
     * 发送带Headers的Event
     */
    private static void sendEventWithHeaders(RpcClient client) {
        System.out.println("\n=== Send Event with Headers ===");

        // 创建带Headers的Event
        Map<String, String> headers = new HashMap<>();
        headers.put("topic", "test-topic");
        headers.put("partition", "0");
        headers.put("timestamp", String.valueOf(System.currentTimeMillis()));
        headers.put("type", "log");

        String body = "Message with headers";
        Event event = EventBuilder.withBody(body, StandardCharsets.UTF_8, headers);

        try {
            client.append(event);
            System.out.println("Event with headers sent successfully");
            System.out.println("Headers: " + headers);
        } catch (EventDeliveryException e) {
            System.err.println("Failed to send event: " + e.getMessage());
        }
    }

    /**
     * Thrift客户端示例
     */
    private static void thriftClientExample() {
        System.out.println("\n=== Thrift Client Example ===");

        // 使用Thrift协议连接Flume
        // 需要配置 Flume agent 启用 Thrift source
        // agent.sources.thrift-source.type = thrift
        // agent.sources.thrift-source.bind = localhost
        // agent.sources.thrift-source.port = 41414

        RpcClient thriftClient = RpcClientFactory.getThriftInstance(
                HOST, PORT, 1000);

        try {
            // 发送事件
            String body = "Message via Thrift";
            Event event = EventBuilder.withBody(body, StandardCharsets.UTF_8);

            thriftClient.append(event);
            System.out.println("Thrift event sent successfully");
        } catch (EventDeliveryException e) {
            System.err.println("Thrift send failed: " + e.getMessage());
        } finally {
            thriftClient.close();
        }
    }

    /**
     * 异步发送示例
     */
    private static void asyncSendExample() {
        System.out.println("\n=== Async Send Example ===");

        // 创建异步客户端
        RpcClient asyncClient = RpcClientFactory.getAsyncDefaultInstance(
                HOST, PORT, 1000);

        try {
            for (int i = 0; i < 100; i++) {
                String body = "Async message #" + i;
                Event event = EventBuilder.withBody(body, StandardCharsets.UTF_8);

                // 异步发送，不会阻塞
                asyncClient.append(event);
            }

            // 等待发送完成
            Thread.sleep(2000);
            System.out.println("Async batch sent");

        } catch (Exception e) {
            System.err.println("Async send failed: " + e.getMessage());
        } finally {
            asyncClient.close();
        }
    }

    /**
     * 可靠性配置示例
     */
    private static void reliableClientExample() {
        System.out.println("\n=== Reliable Client Example ===");

        // 创建可靠客户端 (保证消息不丢失)
        RpcClient reliableClient = RpcClientFactory.getDefaultInstance(
                HOST, PORT, 1000,
                RpcClientConfigurationConstants.DEFAULT_BATCH_SIZE,
                RpcClientConfigurationConstants.DEFAULT_CONNECT_TIMEOUT,
                RpcClientConfigurationConstants.DEFAULT_REQUEST_TIMEOUT);

        try {
            // 启用批量模式
            // 配置在 Flume agent 端
            // agent.sinks.hdfs-sink.hdfs.batchSize = 100

            String body = "Reliable message";
            Event event = EventBuilder.withBody(body, StandardCharsets.UTF_8);

            reliableClient.append(event);
            System.out.println("Reliable event sent");

        } catch (EventDeliveryException e) {
            System.err.println("Reliable send failed: " + e.getMessage());
        } finally {
            reliableClient.close();
        }
    }
}
