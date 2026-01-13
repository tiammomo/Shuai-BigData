package com.bigdata.example.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data ACL;
import org.apache.zookeeper.data.Id;
import org.apache.zookeeper.server.auth.DigestAuthenticationProvider;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * ZooKeeper API Examples - 使用Curator框架
 */
public class ZookeeperExample {

    private static final String ZK_SERVERS = "localhost:2181";
    private static final int SESSION_TIMEOUT_MS = 30000;
    private static final int CONNECTION_TIMEOUT_MS = 10000;
    private static final String BASE_PATH = "/bigdata";

    public static void main(String[] args) throws Exception {
        // 1. 创建客户端
        CuratorFramework client = createClient();

        try {
            client.start();
            System.out.println("Connected to ZooKeeper!");

            // 等待连接成功
            if (!client.blockUntilConnected(10, TimeUnit.SECONDS)) {
                throw new RuntimeException("Failed to connect to ZooKeeper");
            }

            // 2. 基本CRUD操作
            basicOperations(client);

            // 3. 临时节点与Watch
            ephemeralAndWatch(client);

            // 4. 分布式锁
            distributedLock(client);

            // 5. Leader选举
            leaderElection(client);

            // 6. 分布式计数器
            distributedCounter(client);

            // 7. 屏障(Barrier)
            barrier(client);

        } finally {
            client.close();
            System.out.println("ZooKeeper Examples Completed!");
        }
    }

    /**
     * 创建ZooKeeper客户端
     */
    private static CuratorFramework createClient() {
        return CuratorFrameworkFactory.builder()
                .connectString(ZK_SERVERS)
                .sessionTimeoutMs(SESSION_TIMEOUT_MS)
                .connectionTimeoutMs(CONNECTION_TIMEOUT_MS)
                .retryPolicy(new ExponentialBackoffRetry(1000, 3))
                .build();
    }

    /**
     * 基本CRUD操作
     */
    private static void basicOperations(CuratorFramework client) throws Exception {
        // 创建根节点
        if (client.checkExists().forPath(BASE_PATH) == null) {
            client.create()
                    .creatingParentsIfNeeded()
                    .withMode(CreateMode.PERSISTENT)
                    .withACL(Collections.emptyList())
                    .forPath(BASE_PATH);
            System.out.println("Created base path: " + BASE_PATH);
        }

        // 创建子节点
        String nodePath = BASE_PATH + "/test-node";
        String data = "Hello ZooKeeper!";

        if (client.checkExists().forPath(nodePath) == null) {
            client.create()
                    .withMode(CreateMode.PERSISTENT)
                    .forPath(nodePath, data.getBytes());
            System.out.println("Created node: " + nodePath);
        }

        // 获取数据
        byte[] nodeData = client.getData().forPath(nodePath);
        System.out.println("Node data: " + new String(nodeData));

        // 更新数据
        String newData = "Updated data!";
        client.setData().forPath(nodePath, newData.getBytes());
        System.out.println("Updated node data");

        // 获取子节点列表
        List<String> children = client.getChildren().forPath(BASE_PATH);
        System.out.println("Children of " + BASE_PATH + ": " + children);

        // 删除节点
        client.delete().forPath(nodePath);
        System.out.println("Deleted node: " + nodePath);
    }

    /**
     * 临时节点与Watch机制
     */
    private static void ephemeralAndWatch(CuratorFramework client) throws Exception {
        String ephemeralPath = BASE_PATH + "/ephemeral-node";

        // 创建临时节点
        client.create()
                .withMode(CreateMode.EPHEMERAL)
                .forPath(ephemeralPath, "ephemeral data".getBytes());
        System.out.println("Created ephemeral node: " + ephemeralPath);

        // 设置Watch监听
        CountDownLatch latch = new CountDownLatch(1);
        client.getData()
                .usingWatcher((org.apache.curator.framework.api.Watcher) event -> {
                    System.out.println("Watch event: " + event.getType());
                    latch.countDown();
                })
                .forPath(ephemeralPath);

        // 更新数据触发Watch
        client.setData().forPath(ephemeralPath, "trigger watch".getBytes());

        // 等待Watch触发
        latch.await(5, TimeUnit.SECONDS);
        System.out.println("Watch triggered!");

        // 删除临时节点
        client.delete().forPath(ephemeralPath);
        System.out.println("Ephemeral node deleted (session end)");
    }

    /**
     * 分布式锁
     */
    private static void distributedLock(CuratorFramework client) throws Exception {
        String lockPath = BASE_PATH + "/locks";
        String lockName = "my-resource-lock";

        // 创建锁路径
        if (client.checkExists().forPath(lockPath) == null) {
            client.create().withMode(CreateMode.PERSISTENT).forPath(lockPath);
        }

        // 创建可重入锁
        org.apache.curator.framework.recipes.locks.InterProcessMutex lock =
                new org.apache.curator.framework.recipes.locks.InterProcessMutex(
                        client, lockPath + "/" + lockName);

        try {
            // 获取锁 (等待最多30秒)
            boolean acquired = lock.acquire(30, TimeUnit.SECONDS);
            if (acquired) {
                System.out.println("Lock acquired for: " + lockName);

                // 模拟业务操作
                Thread.sleep(2000);

                // 释放锁
                lock.release();
                System.out.println("Lock released");
            }
        } catch (Exception e) {
            System.out.println("Failed to acquire lock: " + e.getMessage());
        }
    }

    /**
     * Leader选举
     */
    private static void leaderElection(CuratorFramework client) throws Exception {
        String leaderPath = BASE_PATH + "/leader-election";

        // 创建Leader选举路径
        if (client.checkExists().forPath(leaderPath) == null) {
            client.create().withMode(CreateMode.EPHEMERAL_SEQUENTIAL)
                    .forPath(leaderPath + "/candidate-");
        }

        // 创建LeaderLatch
        org.apache.curator.framework.recipes.leader.LeaderLatch leaderLatch =
                new org.apache.curator.framework.recipes.leader.LeaderLatch(
                        client, leaderPath, "participant-" + System.currentTimeMillis());

        leaderLatch.start();

        // 等待成为Leader
        if (leaderLatch.await(10, TimeUnit.SECONDS)) {
            if (leaderLatch.hasLeadership()) {
                System.out.println("This node is the Leader!");

                // 模拟Leader工作
                Thread.sleep(5000);
            }
        } else {
            System.out.println("This node is not the Leader");
        }

        leaderLatch.close();
    }

    /**
     * 分布式计数器
     */
    private static void distributedCounter(CuratorFramework client) throws Exception {
        String counterPath = BASE_PATH + "/counter";

        // 创建计数器
        if (client.checkExists().forPath(counterPath) == null) {
            client.create().withMode(CreateMode.PERSISTENT).forPath(counterPath, "0".getBytes());
        }

        // 使用DistributedAtomicLong
        org.apache.curator.framework.recipes.atomic.DistributedAtomicLong counter =
                new org.apache.curator.framework.recipes.atoms.DistributedAtomicLong(
                        client, counterPath,
                        new ExponentialBackoffRetry(1000, 3));

        // 初始化
        counter.forceSet(0L);

        // 原子递增
        for (int i = 0; i < 5; i++) {
            org.apache.curator.framework.recipes.atomic.AtomicValue<Long> result =
                    counter.increment();
            System.out.println("Counter value: " + result.postValue());
        }

        // 获取当前值
        System.out.println("Final counter value: " + counter.get().postValue());
    }

    /**
     * 分布式Barrier
     */
    private static void barrier(CuratorFramework client) throws Exception {
        String barrierPath = BASE_PATH + "/barrier";

        // 创建Barrier
        if (client.checkExists().forPath(barrierPath) != null) {
            client.delete().deletingChildrenIfNeeded().forPath(barrierPath);
        }

        org.apache.curator.framework.recipes.barriers.DistributedBarrier barrier =
                new org.apache.curator.framework.recipes.barriers.DistributedBarrier(
                        client, barrierPath);

        barrier.setBarrier();

        System.out.println("Waiting for barrier to be removed...");

        // 在另一个线程/进程中调用 barrier.removeBarrier() 来释放
        // 这里演示超时等待
        try {
            barrier.waitOnBarrier(30, TimeUnit.SECONDS);
            System.out.println("Barrier released!");
        } catch (Exception e) {
            System.out.println("Barrier wait timeout");
        }

        barrier.close();
    }

    /**
     * ACL权限控制
     */
    private static void aclOperations(CuratorFramework client) throws Exception {
        String aclPath = BASE_PATH + "/acl-node";

        // 生成digest权限
        String digest = DigestAuthenticationProvider.generateDigest("admin:admin123");
        Id digestId = new Id("digest", digest);
        ACL acl = new ACL(org.apache.zookeeper.server.auth.DigestAuthenticationProvider
                .generateDigest("admin:admin123"), org.apache.zookeeper.ZooDefs.Perms.ALL);

        // 创建带ACL的节点
        client.create()
                .withMode(CreateMode.PERSISTENT)
                .withACL(Collections.singletonList(acl))
                .forPath(aclPath, "protected data".getBytes());
        System.out.println("Created node with ACL: " + aclPath);
    }
}
