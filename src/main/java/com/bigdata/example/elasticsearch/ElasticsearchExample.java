package com.bigdata.example.elasticsearch;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.elasticsearch._types.SortOrder;
import co.elastic.clients.elasticsearch._types.query_dsl.*;
import co.elastic.clients.elasticsearch.core.*;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.indices.CreateIndexRequest;
import co.elastic.clients.elasticsearch.indices.DeleteIndexRequest;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.alibaba.fastjson.JSON;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;

import java.io.IOException;
import java.util.*;

/**
 * Elasticsearch API Examples - 使用官方Java Client (ES 8.x)
 */
public class ElasticsearchExample {

    private static final String ES_HOST = "localhost";
    private static final int ES_PORT = 9200;
    private static final String INDEX_NAME = "test_index";

    public static void main(String[] args) throws Exception {
        // 1. 创建ES客户端
        RestClient restClient = RestClient.builder(
                new HttpHost(ES_HOST, ES_PORT, "http")
        ).build();

        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new co.elastic.clients.json.jackson.JacksonJsonpMapper());

        ElasticsearchClient client = new ElasticsearchClient(transport);

        try {
            System.out.println("Connected to Elasticsearch!");

            // 2. 索引管理
            manageIndex(client);

            // 3. CRUD操作
            performCRUD(client);

            // 4. 批量操作
            bulkOperations(client);

            // 5. 查询操作
            queryOperations(client);

            // 6. 聚合查询
            aggregationOperations(client);

            // 7. 滚动查询
            scrollSearch(client);

            // 8. 异步操作
            asyncOperations(client);

        } finally {
            restClient.close();
            System.out.println("Elasticsearch Examples Completed!");
        }
    }

    /**
     * 创建ES客户端
     */
    private static ElasticsearchClient createClient() {
        RestClient restClient = RestClient.builder(
                new HttpHost(ES_HOST, ES_PORT, "http")
        ).build();

        ElasticsearchTransport transport = new RestClientTransport(
                restClient, new co.elastic.clients.json.jackson.JacksonJsonpMapper());

        return new ElasticsearchClient(transport);
    }

    /**
     * 索引管理
     */
    private static void manageIndex(ElasticsearchClient client) throws IOException {
        // 检查索引是否存在
        boolean exists = client.indices().exists(e -> e.index(INDEX_NAME)).value();
        System.out.println("Index exists: " + exists);

        // 删除旧索引
        if (exists) {
            client.indices().delete(DeleteIndexRequest.of(d -> d.index(INDEX_NAME)));
            System.out.println("Deleted index: " + INDEX_NAME);
        }

        // 创建索引
        client.indices().create(CreateIndexRequest.of(c -> c
                .index(INDEX_NAME)
                .settings(s -> s
                        .numberOfShards("1")
                        .numberOfReplicas("0")
                        .refreshInterval(t -> t.time("1s"))
                )
                .mappings(m -> m
                        .properties("title", p -> p.text(t -> t.analyzer("standard")))
                        .properties("content", p -> p.text(t -> t.analyzer("standard")))
                        .properties("age", p -> p.integer(i -> i))
                        .properties("price", p -> p.double_(d -> d))
                        .properties("created_at", p -> p.date(d -> d.format("yyyy-MM-dd||epoch_millis")))
                        .properties("tags", p -> p.keyword(k -> k))
                )
        ));
        System.out.println("Created index: " + INDEX_NAME);

        // 创建带别名的索引
        // client.indices().create(c -> c.index("new_index").aliases("my_alias", a -> a.isWriteIndex(true)));
    }

    /**
     * CRUD操作
     */
    private static void performCRUD(ElasticsearchClient client) throws IOException {
        // === CREATE ===
        String documentId = "doc-001";
        Map<String, Object> doc = new HashMap<>();
        doc.put("title", "Elasticsearch Guide");
        doc.put("content", "Learn Elasticsearch from scratch");
        doc.put("age", 25);
        doc.put("price", 99.99);
        doc.put("created_at", "2024-01-15");
        doc.put("tags", Arrays.asList("tech", "database"));

        IndexResponse indexResponse = client.index(i -> i
                .index(INDEX_NAME)
                .id(documentId)
                .document(doc)
        );
        System.out.println("Indexed document: " + indexResponse.result());

        // === READ ===
        GetResponse<Map> getResponse = client.get(g -> g
                        .index(INDEX_NAME)
                        .id(documentId),
                Map.class
        );

        if (getResponse.found()) {
            Map<String, Object> result = getResponse.source();
            System.out.println("Retrieved document: " + result.get("title"));
        } else {
            System.out.println("Document not found: " + documentId);
        }

        // === UPDATE ===
        UpdateResponse<Map> updateResponse = client.update(u -> u
                        .index(INDEX_NAME)
                        .id(documentId)
                        .doc(doc),
                Map.class
        );
        System.out.println("Updated document, result: " + updateResponse.result());

        // === DELETE ===
        DeleteResponse deleteResponse = client.delete(d -> d
                .index(INDEX_NAME)
                .id(documentId)
        );
        System.out.println("Deleted document, result: " + deleteResponse.result());
    }

    /**
     * 批量操作
     */
    private static void bulkOperations(ElasticsearchClient client) throws IOException {
        // 准备批量数据
        List<BulkOperation> operations = new ArrayList<>();

        for (int i = 1; i <= 10; i++) {
            Map<String, Object> doc = new HashMap<>();
            doc.put("title", "Document " + i);
            doc.put("content", "Content of document " + i);
            doc.put("age", 20 + (i % 10));
            doc.put("price", i * 10.5);
            doc.put("created_at", "2024-01-" + String.format("%02d", i));
            doc.put("tags", Arrays.asList("tag" + i, "category-" + (i % 3)));

            operations.add(BulkOperation.of(b -> b
                    .index(IndexOperation.of(idx -> idx
                            .index(INDEX_NAME)
                            .id("bulk-" + String.format("%03d", i))
                            .document(doc)
                    ))
            ));
        }

        // 执行批量操作
        BulkResponse response = client.bulk(b -> b.operations(operations));

        if (response.errors()) {
            System.out.println("Bulk operation had errors");
            response.items().stream()
                    .filter(item -> item.error() != null)
                    .forEach(item -> System.out.println("Error: " + item.error().reason()));
        } else {
            System.out.println("Bulk indexed " + response.items().size() + " documents");
        }

        // 刷新索引
        client.indices().refresh(r -> r.index(INDEX_NAME));
    }

    /**
     * 查询操作
     */
    private static void queryOperations(ElasticsearchClient client) throws IOException {
        // 准备测试数据
        prepareTestData(client);

        // 1. 全文搜索
        System.out.println("\n=== Full Text Search ===");
        SearchResponse<Map> searchResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .query(q -> q
                                .match(m -> m
                                        .field("content")
                                        .query("content")
                                )
                        )
                        .size(5),
                Map.class
        );

        for (Hit<Map> hit : searchResponse.hits().hits()) {
            System.out.println("Found: " + hit.source().get("title"));
        }

        // 2. 精确匹配
        System.out.println("\n=== Term Query ===");
        SearchResponse<Map> termResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .query(q -> q
                                .term(t -> t
                                        .field("tags.keyword")
                                        .value("tag1")
                                )
                        ),
                Map.class
        );

        for (Hit<Map> hit : termResponse.hits().hits()) {
            System.out.println("Found: " + hit.source().get("title"));
        }

        // 3. 范围查询
        System.out.println("\n=== Range Query ===");
        SearchResponse<Map> rangeResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .query(q -> q
                                .range(r -> r
                                        .field("age")
                                        .gte(JsonData.of(25))
                                )
                        ),
                Map.class
        );

        for (Hit<Map> hit : rangeResponse.hits().hits()) {
            System.out.println("Found: " + hit.source().get("title") +
                    ", age: " + hit.source().get("age"));
        }

        // 4. 布尔查询
        System.out.println("\n=== Bool Query ===");
        SearchResponse<Map> boolResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .query(q -> q
                                .bool(b -> b
                                        .must(m -> m
                                                .match(mt -> mt.field("content").query("Document"))
                                        )
                                        .filter(f -> f
                                                .range(r -> r.field("age").gte(JsonData.of(25)))
                                        )
                                )
                        ),
                Map.class
        );

        for (Hit<Map> hit : boolResponse.hits().hits()) {
            System.out.println("Found: " + hit.source().get("title"));
        }

        // 5. 多字段搜索
        System.out.println("\n=== Multi Match Query ===");
        SearchResponse<Map> multiMatchResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .query(q -> q
                                .multiMatch(mm -> mm
                                        .query("Document")
                                        .fields("title", "content")
                                        .fuzziness("AUTO")
                                )
                        ),
                Map.class
        );

        for (Hit<Map> hit : multiMatchResponse.hits().hits()) {
            System.out.println("Found: " + hit.source().get("title") +
                    ", score: " + hit.score());
        }

        // 6. 分页和排序
        System.out.println("\n=== Pagination and Sort ===");
        SearchResponse<Map> pageResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .from(0)
                        .size(3)
                        .sort(so -> so
                                .field(f -> f.field("age").order(SortOrder.Desc))
                        ),
                Map.class
        );

        for (Hit<Map> hit : pageResponse.hits().hits()) {
            System.out.println("Found: " + hit.source().get("title") +
                    ", age: " + hit.source().get("age"));
        }
    }

    /**
     * 聚合查询
     */
    private static void aggregationOperations(ElasticsearchClient client) throws IOException {
        // 1. 指标聚合 - 平均值
        System.out.println("\n=== Avg Aggregation ===");
        SearchResponse<Map> avgResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .size(0)
                        .aggregations("avg_age", a -> a.avg(avg -> avg.field("age"))),
                Map.class
        );

        double avgAge = avgResponse.aggregations().get("avg_age").avg().value();
        System.out.println("Average age: " + avgAge);

        // 2. 桶聚合 - Terms
        System.out.println("\n=== Terms Aggregation ===");
        SearchResponse<Map> termsResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .size(0)
                        .aggregations("age_buckets", a -> a
                                .terms(t -> t.field("age").size(10))
                        ),
                Map.class
        );

        termsResponse.aggregations().get("age_buckets").sterms().buckets().array()
                .forEach(bucket -> System.out.println("Age: " + bucket.key().stringValue() +
                        ", count: " + bucket.docCount()));

        // 3. 嵌套聚合
        System.out.println("\n=== Nested Aggregation ===");
        SearchResponse<Map> nestedResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .size(0)
                        .aggregations("tags", a -> a
                                .terms(t -> t.field("tags.keyword").size(10))
                                .aggregations("avg_price", sub -> sub.avg(avg -> avg.field("price")))
                        ),
                Map.class
        );

        nestedResponse.aggregations().get("tags").sterms().buckets().array()
                .forEach(bucket -> System.out.println("Tag: " + bucket.key().stringValue() +
                        ", doc_count: " + bucket.docCount() +
                        ", avg_price: " + bucket.aggregations().get("avg_price").avg().value()));

        // 4. 日期直方图聚合
        // SearchResponse<Map> dateHistResponse = client.search(s -> s
        //         .index(INDEX_NAME)
        //         .size(0)
        //         .aggregations("daily", a -> a
        //                 .dateHistogram(dh -> dh
        //                         .field("created_at")
        //                         .calendarInterval(CalendarInterval.Day)
        //                 )
        //         ),
        //         Map.class);

        // 5. 复合聚合
        System.out.println("\n=== Composite Aggregation ===");
        SearchResponse<Map> compositeResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .size(0)
                        .aggregations("price_ranges", a -> a
                                .composite(c -> c
                                        .size(100)
                                        .sources(src -> src
                                                .terms("by_tag", t -> t
                                                        .field("tags.keyword")
                                                        .size(10)
                                                )
                                        )
                                )
                        ),
                Map.class
        );
    }

    /**
     * 滚动查询 (用于大数据量)
     */
    private static void scrollSearch(ElasticsearchClient client) throws IOException {
        System.out.println("\n=== Scroll Search ===");

        // 初始搜索
        SearchResponse<Map> initialResponse = client.search(s -> s
                        .index(INDEX_NAME)
                        .scroll(sc -> sc.timeout("1m"))
                        .size(100),
                Map.class
        );

        String scrollId = initialResponse.scrollId();
        int totalHits = (int) initialResponse.hits().total().value();
        int processed = 0;

        while (processed < totalHits) {
            for (Hit<Map> hit : initialResponse.hits().hits()) {
                processed++;
                if (processed <= 5) {
                    System.out.println("Doc " + processed + ": " + hit.source().get("title"));
                }
            }

            // 获取下一批
            initialResponse = client.scroll(sc -> sc.scrollId(scrollId).scrollTimeout("1m"), Map.class);
            scrollId = initialResponse.scrollId();
        }

        // 清除滚动上下文
        client.clearScroll(cs -> cs.scrollId(scrollId));
        System.out.println("Total processed: " + totalHits);
    }

    /**
     * 异步操作
     */
    private static void asyncOperations(ElasticsearchClient client) throws Exception {
        System.out.println("\n=== Async Operations ===");

        // 异步索引
        co.elastic.clients.elasticsearch.core.IndexRequest<Map> request =
                co.elastic.clients.elasticsearch.core.IndexRequest.of(i -> i
                        .index(INDEX_NAME)
                        .id("async-doc")
                        .document(Collections.singletonMap("title", "Async Document"))
                );

        // 同步执行
        IndexResponse response = client.index(request);
        System.out.println("Async index result: " + response.result());

        // 异步执行示例 (使用Java CompletableFuture)
        // CompletableFuture.supplyAsync(() -> {
        //     try {
        //         return client.index(request);
        //     } catch (IOException e) {
        //         throw new RuntimeException(e);
        //     }
        // }).thenAccept(result -> System.out.println("Completed: " + result.result()));
    }

    /**
     * 准备测试数据
     */
    private static void prepareTestData(ElasticsearchClient client) throws IOException {
        // 索引已存在则跳过
        boolean exists = client.indices().exists(e -> e.index(INDEX_NAME)).value();
        if (!exists) {
            manageIndex(client);
        }

        // 刷新索引
        client.indices().refresh(r -> r.index(INDEX_NAME));
    }
}
