package com.bigdata.example.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;

import java.util.List;
import java.util.concurrent.*;

/**
 * ZooKeeper Recipes Examples - 高级用法
 */
public class ZookeeperRecipesExample {

    private static final String ZK_SERVERS = "localhost:2181";
    private static final String BASE_PATH = "/bigdata/recipes";

    public static void main(String[] args) throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.builder()
                .connectString(ZK_SERVERS)
                .sessionTimeoutMs(30000)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();

        try {
            client.start();

            // 1. 异步API
            asyncOperations(client);

            // 2. 节点容器(Container)
            containerNodes(client);

            // 3. TTL节点
            ttlNodes(client);

            // 4. 命名服务
            namingService(client);

            // 5. 服务发现
            serviceDiscovery(client);

            // 6. 配置管理
            configurationManagement(client);

            // 7. 组成员管理
            groupMembership(client);

        } finally {
            client.close();
        }
    }

    /**
     * 异步操作
     */
    private static void asyncOperations(CuratorFramework client) throws Exception {
        String asyncPath = BASE_PATH + "/async";

        CountDownLatch latch = new CountDownLatch(1);

        // 异步创建节点
        BackgroundCallback callback = (client1, event) -> {
            System.out.println("Async result: " + event.getType() +
                    ", path: " + event.getPath() +
                    ", code: " + event.getResultCode());
            latch.countDown();
        };

        client.create()
                .withMode(CreateMode.PERSISTENT)
                .inBackground(callback)
                .forPath(asyncPath, "async data".getBytes());

        latch.await(10, TimeUnit.SECONDS);
        System.out.println("Async operation completed!");
    }

    /**
     * 容器节点 (Container Nodes)
     * Container节点在子节点被删除后自动清理
     */
    private static void containerNodes(CuratorFramework client) throws Exception {
        String containerPath = BASE_PATH + "/container";

        // 创建容器节点
        if (client.checkExists().forPath(containerPath) == null) {
            client.create()
                    .withMode(CreateMode.CONTAINER)
                    .forPath(containerPath);
            System.out.println("Created container node: " + containerPath);
        }

        // 往容器添加临时子节点
        for (int i = 0; i < 3; i++) {
            client.create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(containerPath + "/child-" + i);
        }

        // 列出容器子节点
        List<String> children = client.getChildren().forPath(containerPath);
        System.out.println("Container children: " + children);

        // 删除子节点后，容器会自动被清理
        System.out.println("Container nodes are automatically cleaned up");
    }

    /**
     * TTL节点 (Time-To-Live)
     * 带生存时间的持久节点，超时后自动删除
     */
    private static void ttlNodes(CuratorFramework client) throws Exception {
        String ttlPath = BASE_PATH + "/ttl";

        // 注意: TTL需要配置启用，这里仅作示例
        // 实际使用需要设置 -Dzookeeper.extendedTypesEnabled=true
        try {
            client.create()
                    .withMode(CreateMode.PERSISTENT_WITH_TTL)
                    .forPath(ttlPath, "ttl data".getBytes());
            System.out.println("Created TTL node: " + ttlPath);
            System.out.println("Node will be deleted after TTL expires");
        } catch (Exception e) {
            System.out.println("TTL not enabled in server: " + e.getMessage());
        }
    }

    /**
     * 命名服务 (Sequential节点)
     */
    private static void namingService(CuratorFramework client) throws Exception {
        String sequentialPath = BASE_PATH + "/sequential";

        // 创建顺序节点
        for (int i = 0; i < 5; i++) {
            String path = client.create()
                    .withMode(CreateMode.PERSISTENT_SEQUENTIAL)
                    .forPath(sequentialPath + "/task-");
            System.out.println("Created sequential path: " + path);
        }

        // 获取所有顺序节点
        List<String> tasks = client.getChildren()
                .forPath(sequentialPath);
        System.out.println("All tasks: " + tasks);
    }

    /**
     * 服务发现
     */
    private static void serviceDiscovery(CuratorFramework client) throws Exception {
        String servicePath = BASE_PATH + "/services";

        // 注册服务
        String serviceName = "my-service";
        String instancePath = servicePath + "/" + serviceName + "/instance-";

        String registeredPath = client.create()
                .withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                .forPath(instancePath, "{\"host\":\"localhost\",\"port\":8080}".getBytes());
        System.out.println("Service registered: " + registeredPath);

        // 发现服务
        List<String> instances = client.getChildren()
                .forPath(servicePath + "/" + serviceName);
        System.out.println("Service instances: " + instances);

        // 获取服务详情
        for (String instance : instances) {
            byte[] data = client.getData()
                    .forPath(servicePath + "/" + serviceName + "/" + instance);
            System.out.println("Instance data: " + new String(data));
        }
    }

    /**
     * 配置管理
     */
    private static void configurationManagement(CuratorFramework client) throws Exception {
        String configPath = BASE_PATH + "/config";

        // 创建设配置节点
        if (client.checkExists().forPath(configPath) == null) {
            client.create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(configPath, "key1=value1".getBytes());
        }

        // 更新配置
        client.setData().forPath(configPath, "key1=value1\nkey2=value2".getBytes());

        // 读取配置
        byte[] configData = client.getData().forPath(configPath);
        System.out.println("Config: " + new String(configData));

        // 监听配置变化
        System.out.println("Watching config changes...");
    }

    /**
     * 组成员管理
     */
    private static void groupMembership(CuratorFramework client) throws Exception {
        String groupPath = BASE_PATH + "/group";

        // 创建组成员节点
        for (int i = 0; i < 3; i++) {
            String memberPath = client.create()
                    .withMode(CreateMode.EPHEMERAL)
                    .forPath(groupPath + "/member-" + i, ("Member" + i).getBytes());
            System.out.println("Added member: " + memberPath);
        }

        // 获取组成员列表
        List<String> members = client.getChildren().forPath(groupPath);
        System.out.println("Group members: " + members);

        // 获取成员数量
        int memberCount = members.size();
        System.out.println("Member count: " + memberCount);

        // 检查成员是否存在
        String checkPath = groupPath + "/member-0";
        if (client.checkExists().forPath(checkPath) != null) {
            System.out.println("Member-0 exists");
        }
    }

    /**
     * 事务操作
     */
    private static void transactionExample(CuratorFramework client) throws Exception {
        String txPath1 = BASE_PATH + "/tx1";
        String txPath2 = BASE_PATH + "/tx2";

        try {
            // 事务操作 (全部成功或全部失败)
            client.inTransaction()
                    .create().withMode(CreateMode.PERSISTENT).forPath(txPath1)
                    .and()
                    .create().withMode(CreateMode.PERSISTENT).forPath(txPath2)
                    .and()
                    .setData().forPath(txPath1, "data1".getBytes())
                    .and()
                    .commit();

            System.out.println("Transaction committed successfully");
        } catch (Exception e) {
            System.out.println("Transaction failed: " + e.getMessage());
        }

        // 清理
        try {
            client.delete().forPath(txPath1);
            client.delete().forPath(txPath2);
        } catch (Exception ignored) {}
    }

    /**
     * 错误处理
     */
    private static void errorHandling(CuratorFramework client) throws Exception {
        String testPath = BASE_PATH + "/test";

        try {
            // 创建节点
            client.create().forPath(testPath, "data".getBytes());
        } catch (org.apache.zookeeper.KeeperException.NodeExistsException e) {
            System.out.println("Node already exists: " + e.getPath());
        } catch (org.apache.zookeeper.KeeperException.NoNodeException e) {
            System.out.println("Node not found: " + e.getPath());
        } catch (Exception e) {
            System.out.println("Error: " + e.getMessage());
        }
    }
}
