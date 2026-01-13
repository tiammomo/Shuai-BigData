package com.bigdata.example.flume;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Flume Configuration Examples - Flume配置文件生成
 */
public class FlumeConfigurationExample {

    public static void main(String[] args) throws IOException {
        System.out.println("Flume Configuration Examples");

        // 1. 配置文件示例: 日志采集到HDFS
        generateHDFSConfiguration();

        // 2. 配置文件示例: Kafka到Doris
        generateKafkaToDorisConfiguration();

        // 3. 配置文件示例: Tail采集到Kafka
        generateTailToKafkaConfiguration();

        // 4. 配置文件示例: 多Sink负载均衡
        generateLoadBalancingConfiguration();

        // 5. 配置文件示例: 复制与路由
        generateReplicationConfiguration();

        System.out.println("Configuration files generated!");
    }

    /**
     * 生成日志采集到HDFS的配置
     */
    private static void generateHDFSConfiguration() {
        String config =
"# Flume Agent Configuration: log-to-hdfs.conf\n" +
"# 采集日志文件并写入HDFS\n\n" +
"a1.sources = r1\n" +
"a1.sinks = k1\n" +
"a1.channels = c1\n\n" +
"# Source: Exec监控日志文件\n" +
"a1.sources.r1.type = exec\n" +
"a1.sources.r1.command = tail -F /var/log/app.log\n" +
"a1.sources.r1.shell = /bin/bash -c\n" +
"a1.sources.r1.channels = c1\n\n" +
"# Interceptor: 添加时间戳\n" +
"a1.sources.r1.interceptors = i1\n" +
"a1.sources.r1.interceptors.i1.type = timestamp\n" +
"a1.sources.r1.interceptors.i1.preserveExisting = true\n\n" +
"# Channel: Memory Channel\n" +
"a1.channels.c1.type = memory\n" +
"a1.channels.c1.capacity = 10000\n" +
"a1.channels.c1.transactionCapacity = 1000\n" +
"a1.channels.c1.byteCapacityBufferPercentage = 20\n\n" +
"# Sink: HDFS Sink\n" +
"a1.sinks.k1.type = hdfs\n" +
"a1.sinks.k1.hdfs.path = hdfs://namenode:8020/flume/logs/%Y/%m/%d\n" +
"a1.sinks.k1.hdfs.filePrefix = logs-\n" +
"a1.sinks.k1.hdfs.fileSuffix = .log\n" +
"a1.sinks.k1.hdfs.rollSize = 134217728  # 128MB\n" +
"a1.sinks.k1.hdfs.rollInterval = 300    # 5分钟\n" +
"a1.sinks.k1.hdfs.rollCount = 0\n" +
"a1.sinks.k1.hdfs.useLocalTimeStamp = true\n" +
"a1.sinks.k1.hdfs.batchSize = 100\n" +
"a1.sinks.k1.hdfs.fileType = DataStream\n" +
"a1.sinks.k1.hdfs.writeFormat = Text\n" +
"a1.sinks.k1.channel = c1\n\n" +
"# 运行命令: flume-ng agent -n a1 -c conf -f log-to-hdfs.conf";

        writeToFile("conf/log-to-hdfs.conf", config);
        System.out.println("Generated: log-to-hdfs.conf");
    }

    /**
     * 生成Kafka到Doris的配置
     */
    private static void generateKafkaToDorisConfiguration() {
        String config =
"# Flume Agent Configuration: kafka-to-doris.conf\n" +
"# 采集Kafka数据并写入Doris\n\n" +
"a1.sources = r1\n" +
"a1.sinks = k1\n" +
"a1.channels = c1\n\n" +
"# Source: Kafka Source\n" +
"a1.sources.r1.type = org.apache.flume.source.kafka.KafkaSource\n" +
"a1.sources.r1.batchSize = 100\n" +
"a1.sources.r1.batchDurationMillis = 1000\n" +
"a1.sources.r1.allowTokenize = false\n" +
"a1.sources.r1.topics = my_topic\n" +
"a1.sources.r1.kafka.bootstrap.servers = localhost:9092\n" +
"a1.sources.r1.kafka.consumer.group.id = flume-consumer\n" +
"a1.sources.r1.kafka.consumer.auto.offset.reset = earliest\n" +
"a1.sources.r1.channels = c1\n\n" +
"# Channel: Kafka Channel (可选，直接连接Kafka Sink)\n" +
"a1.channels.c1.type = memory\n" +
"a1.channels.c1.capacity = 10000\n" +
"a1.channels.c1.transactionCapacity = 1000\n\n" +
"# Sink: HTTP Sink (Doris Stream Load)\n" +
"a1.sinks.k1.type = org.apache.flume.sink.http.HTTPStreamSink\n" +
"a1.sinks.k1.endpoint = http://doris-fe:8030/api/my_db/my_table/_load\n" +
"a1.sinks.k1.connectTimeout = 5000\n" +
"a1.sinks.k1.requestTimeout = 30000\n" +
"a1.sinks.k1.contentType = application/json\n" +
"a1.sinks.k1.batchSize = 100\n" +
"a1.sinks.k1.channel = c1\n\n" +
"# 运行命令: flume-ng agent -n a1 -c conf -f kafka-to-doris.conf";

        writeToFile("conf/kafka-to-doris.conf", config);
        System.out.println("Generated: kafka-to-doris.conf");
    }

    /**
     * 生成Tail采集到Kafka的配置
     */
    private static void generateTailToKafkaConfiguration() {
        String config =
"# Flume Agent Configuration: tail-to-kafka.conf\n" +
"# 采集日志文件并发送到Kafka\n\n" +
"a1.sources = r1\n" +
"a1.sinks = k1\n" +
"a1.channels = c1\n\n" +
"# Source: Spooling Directory Source (推荐，生产环境更可靠)\n" +
"a1.sources.r1.type = spooldir\n" +
"a1.sources.r1.spooldir = /data/flume/spool\n" +
"a1.sources.r1.fileSuffix = .COMPLETED\n" +
"a1.sources.r1.deletePolicy = immediate\n" +
"a1.sources.r1.trackDirs = true\n" +
"a1.sources.r1.channels = c1\n\n" +
"# Interceptor: 添加自定义头信息\n" +
"a1.sources.r1.interceptors = i1 i2\n" +
"a1.sources.r1.interceptors.i1.type = timestamp\n" +
"a1.sources.r1.interceptors.i2.type = static\n" +
"a1.sources.r1.interceptors.i2.type = static\n" +
"a1.sources.r1.interceptors.i2.override = logtype=application\n\n" +
"# Channel: File Channel (保证数据不丢失)\n" +
"a1.channels.c1.type = file\n" +
"a1.channels.c1.dataDirs = /data/flume/file-channel/data\n" +
"a1.channels.c1.checkpointDir = /data/flume/file-channel/checkpoint\n" +
"a1.channels.c1.capacity = 100000\n" +
"a1.channels.c1.transactionCapacity = 1000\n\n" +
"# Sink: Kafka Sink\n" +
"a1.sinks.k1.type = org.apache.flume.sink.kafka.KafkaSink\n" +
"a1.sinks.k1.kafka.bootstrap.servers = localhost:9092\n" +
"a1.sinks.k1.kafka.topic = flume-logs\n" +
"a1.sinks.k1.kafka.producer.acks = all\n" +
"a1.sinks.k1.kafka.producer.batch.size = 16384\n" +
"a1.sinks.k1.kafka.producer.linger.ms = 10\n" +
"a1.sinks.k1.channel = c1";

        writeToFile("conf/tail-to-kafka.conf", config);
        System.out.println("Generated: tail-to-kafka.conf");
    }

    /**
     * 生成负载均衡配置
     */
    private static void generateLoadBalancingConfiguration() {
        String config =
"# Flume Agent Configuration: load-balancing.conf\n" +
"# 多Sink负载均衡\n\n" +
"a1.sources = r1\n" +
"a1.sinks = k1 k2 k3\n" +
"a1.channels = c1\n\n" +
"# Source\n" +
"a1.sources.r1.type = syslogtcp\n" +
"a1.sources.r1.port = 5140\n" +
"a1.sources.r1.channels = c1\n\n" +
"# Channel\n" +
"a1.channels.c1.type = memory\n" +
"a1.channels.c1.capacity = 1000\n" +
"a1.channels.c1.transactionCapacity = 100\n\n" +
"# Sink Group: 负载均衡Sink组\n" +
"a1.sinkgroups = g1\n" +
"a1.sinkgroups.g1.sinks = k1 k2 k3\n" +
"a1.sinkgroups.g1.processor.type = load_balance\n" +
"a1.sinkgroups.g1.processor.backoff = true\n" +
"a1.sinkgroups.g1.processor.selector = round_robin  # 或 random\n\n" +
"# Sink 1: HDFS\n" +
"a1.sinks.k1.type = hdfs\n" +
"a1.sinks.k1.hdfs.path = hdfs://namenode:8020/flume/data1\n" +
"a1.sinks.k1.hdfs.fileType = DataStream\n" +
"a1.sinks.k1.channel = c1\n\n" +
"# Sink 2: HDFS\n" +
"a1.sinks.k2.type = hdfs\n" +
"a1.sinks.k2.hdfs.path = hdfs://namenode:8020/flume/data2\n" +
"a1.sinks.k2.hdfs.fileType = DataStream\n" +
"a1.sinks.k2.channel = c1\n\n" +
"# Sink 3: HDFS\n" +
"a1.sinks.k3.type = hdfs\n" +
"a1.sinks.k3.hdfs.path = hdfs://namenode:8020/flume/data3\n" +
"a1.sinks.k3.hdfs.fileType = DataStream\n" +
"a1.sinks.k3.channel = c1";

        writeToFile("conf/load-balancing.conf", config);
        System.out.println("Generated: load-balancing.conf");
    }

    /**
     * 生成复制与路由配置
     */
    private static void generateReplicationConfiguration() {
        String config =
"# Flume Agent Configuration: replication.conf\n" +
"# 数据复制到多个Sink (复制模式)\n\n" +
"a1.sources = r1\n" +
"a1.sinks = k1 k2\n" +
"a1.channels = c1 c2\n\n" +
"# Source\n" +
"a1.sources.r1.type = http\n" +
"a1.sources.r1.port = 8888\n" +
"a1.sources.r1.channels = c1 c2\n\n" +
"# Interceptor: 选择性路由\n" +
"a1.sources.r1.interceptors = i1\n" +
"a1.sources.r1.interceptors.i1.type = static\n" +
"a1.sources.r1.interceptors.i1.override = sourceType=web\n\n" +
"# Channel 1\n" +
"a1.channels.c1.type = memory\n" +
"a1.channels.c1.capacity = 1000\n\n" +
"# Channel 2\n" +
"a1.channels.c2.type = memory\n" +
"a1.channels.c2.capacity = 1000\n\n" +
"# Sink 1: 写入HDFS\n" +
"a1.sinks.k1.type = hdfs\n" +
"a1.sinks.k1.hdfs.path = hdfs://namenode:8020/flume/hdfs-data\n" +
"a1.sinks.k1.hdfs.fileType = DataStream\n" +
"a1.sinks.k1.channel = c1\n\n" +
"# Sink 2: 写入Kafka\n" +
"a1.sinks.k2.type = org.apache.flume.sink.kafka.KafkaSink\n" +
"a1.sinks.k2.kafka.bootstrap.servers = localhost:9092\n" +
"a1.sinks.k2.kafka.topic = flume-topic\n" +
"a1.sinks.k2.channel = c2";

        writeToFile("conf/replication.conf", config);
        System.out.println("Generated: replication.conf");
    }

    /**
     * 自定义Sink示例代码 (Java)
     */
    public static String getCustomSinkTemplate() {
        return
"/**\n" +
" * Custom Flume Sink Example\n" +
" * 自定义Flume Sink实现\n" +
" */\n" +
"public class MyCustomSink extends AbstractSink implements Configurable {\n" +
"\n" +
"    private String myProperty;\n" +
"\n" +
"    @Override\n" +
"    public void configure(Context context) {\n" +
"        // 读取配置\n" +
"        myProperty = context.getString(\"myProperty\", \"defaultValue\");\n" +
"    }\n" +
"\n" +
"    @Override\n" +
"    public Status process() throws EventDeliveryException {\n" +
"        Channel channel = getChannel();\n" +
"        Transaction tx = channel.getTransaction();\n" +
"\n" +
"        try {\n" +
"            tx.begin();\n" +
"\n" +
"            Event event = channel.take();\n" +
"            if (event == null) {\n" +
"                return Status.BACKOFF;\n" +
"            }\n" +
"\n" +
"            // 处理Event\n" +
"            String body = new String(event.getBody(), StandardCharsets.UTF_8);\n" +
"            Map<String, String> headers = event.getHeaders();\n" +
"\n" +
"            // 发送到目标系统\n" +
"            sendToDestination(body, headers);\n" +
"\n" +
"            tx.commit();\n" +
"            return Status.READY;\n" +
"\n" +
"        } catch (Exception e) {\n" +
"            tx.rollback();\n" +
"            return Status.BACKOFF;\n" +
"        } finally {\n" +
"            tx.close();\n" +
"        }\n" +
"    }\n" +
"\n" +
"    private void sendToDestination(String body, Map<String, String> headers) {\n" +
"        // 实现发送到目标系统的逻辑\n" +
"    }\n" +
"}\n";
    }

    /**
     * 自定义Source示例代码 (Java)
     */
    public static String getCustomSourceTemplate() {
        return
"/**\n" +
" * Custom Flume Source Example\n" +
" * 自定义Flume Source实现\n" +
" */\n" +
"public class MyCustomSource extends AbstractSource implements EventDrivenSource, Runnable {\n" +
"\n" +
"    private volatile boolean running = false;\n" +
"    private ExecutorService executor;\n" +
"\n" +
"    @Override\n" +
"    public void configure(Context context) {\n" +
"        // 配置读取\n" +
"    }\n" +
"\n" +
"    @Override\n" +
"    public void start() {\n" +
"        running = true;\n" +
"        executor = Executors.newSingleThreadExecutor();\n" +
"        executor.submit(this);\n" +
"    }\n" +
"\n" +
"    @Override\n" +
"    public void stop() {\n" +
"        running = false;\n" +
"        executor.shutdown();\n" +
"    }\n" +
"\n" +
"    @Override\n" +
"    public void run() {\n" +
"        while (running && !Thread.interrupted()) {\n" +
"            try {\n" +
"                // 从数据源采集数据\n" +
"                String data = fetchData();\n" +
"\n" +
"                // 创建Event\n" +
"                Event event = EventBuilder.withBody(\n" +
"                    data.getBytes(StandardCharsets.UTF_8), getHeaders());\n" +
"\n" +
"                // 发送到Channel\n" +
"                getChannelProcessor().processEvent(event);\n" +
"\n" +
"            } catch (Exception e) {\n" +
"                // 错误处理\n" +
"            }\n" +
"        }\n" +
"    }\n" +
"\n" +
"    private String fetchData() {\n" +
"        // 实现数据采集逻辑\n" +
"        return null;\n" +
"    }\n" +
"\n" +
"    private Map<String, String> getHeaders() {\n" +
"        Map<String, String> headers = new HashMap<>();\n" +
"        headers.put(\"timestamp\", String.valueOf(System.currentTimeMillis()));\n" +
"        return headers;\n" +
"    }\n" +
"}\n";
    }

    private static void writeToFile(String filename, String content) {
        try {
            new File("conf").mkdirs();
            try (FileWriter writer = new FileWriter("conf/" + filename)) {
                writer.write(content);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
