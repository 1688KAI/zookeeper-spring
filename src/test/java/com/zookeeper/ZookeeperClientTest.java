package com.zookeeper;

import lombok.extern.slf4j.Slf4j;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.junit.Before;
import org.junit.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
@SpringBootTest
public class ZookeeperClientTest {

    private static final String ZK_ADDRESS = "localhost:2181,localhost:2182,localhost:2183,localhost:2184";

    private static final int SESSION_TIMEOUT = 5000;

    private static ZooKeeper zooKeeper;

    private static final String ZK_NODE = "/zk-node";


    @Before
    public void init() throws IOException, InterruptedException {
        final CountDownLatch countDownLatch = new CountDownLatch(1);
        zooKeeper = new ZooKeeper(ZK_ADDRESS, SESSION_TIMEOUT, event -> {
            if (event.getState() == Watcher.Event.KeeperState.SyncConnected &&
                    event.getType() == Watcher.Event.EventType.None) {
                countDownLatch.countDown();
                log.info("连接成功！");
            }
        });
        log.info("连接中....");
        countDownLatch.await();
    }


    /**
     * 创建持久化节点
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void createTest() throws KeeperException, InterruptedException {
        String path = zooKeeper.create(ZK_NODE, "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        log.info("created path: {}", path);
    }

    /**
     * 异步创建节点
     *
     * @throws InterruptedException
     */
    @Test
    public void createAsycTest() throws InterruptedException {
        zooKeeper.create(ZK_NODE, "data".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT,
                (rc, path, ctx, name) -> log.info("rc  {},path {},ctx {},name {}", rc, path, ctx, name), "context");
        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }

    /**
     * 修改节点数据
     *
     * @throws KeeperException
     * @throws InterruptedException
     */
    @Test
    public void setTest() throws KeeperException, InterruptedException {

        Stat stat = new Stat();
        byte[] data = zooKeeper.getData(ZK_NODE, false, stat);
        log.info("修改前: {}", new String(data));
        zooKeeper.setData(ZK_NODE, "changed!".getBytes(), stat.getVersion());
        byte[] dataAfter = zooKeeper.getData(ZK_NODE, false, stat);
        log.info("修改后: {}", new String(dataAfter));
    }


    private static CuratorFramework curatorFramework;

    @Before
    public void initRetryPolicy() {
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(ZK_ADDRESS)
                .sessionTimeoutMs(5000)  // 会话超时时间
                .connectionTimeoutMs(5000) // 连接超时时间
                .retryPolicy(retryPolicy)
                .namespace("base") // 包含隔离名称
                .build();
        curatorFramework.start();
    }

    /**
     * 创建节点
     *
     * @throws Exception
     */
    @Test
    public void testCreate() throws Exception {
//        String path = curatorFramework.create().forPath("/curator-node");
        String path = curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath("/curator-node", "some-data".getBytes());
        log.info("curator create node :{}  successfully.", path);
    }

    /**
     * 穿件多级节点
     *
     * @throws Exception
     */
    @Test
    public void testCreateWithParent() throws Exception {
        String pathWithParent = "/node-parent/sub-node-1";
        String path = curatorFramework.create().creatingParentsIfNeeded().forPath(pathWithParent);
        log.info("curator create node :{}  successfully.", path);
    }


    /**
     * 获取节点
     *
     * @throws Exception
     */
    @Test
    public void testGetData() throws Exception {
        byte[] bytes = curatorFramework.getData().forPath("/curator-node");
        log.info("get data from  node :{}  successfully.", new String(bytes));
    }

    /**
     * 更新节点
     *
     * @throws Exception
     */
    @Test
    public void testSetData() throws Exception {
        curatorFramework.setData().forPath("/curator-node", "changed!".getBytes());
        byte[] bytes = curatorFramework.getData().forPath("/curator-node");
        log.info("get data from  node /curator-node :{}  successfully.", new String(bytes));
    }


    /**
     * 删除节点
     *
     * @throws Exception
     */
    @Test
    public void testDelete() throws Exception {
        String pathWithParent = "/node-parent";
        curatorFramework.delete().guaranteed().deletingChildrenIfNeeded().forPath(pathWithParent);
    }


    @Test
    public void test() throws Exception {
        curatorFramework.getData().inBackground((item1, item2) -> {
            log.info(" background: {}", item2);
        }).forPath(ZK_NODE);

        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }

    @Test
    public void test2() throws Exception {
        ExecutorService executorService = Executors.newSingleThreadExecutor();

        curatorFramework.getData().inBackground((item1, item2) -> {
            log.info(" background: {}", item2);
        }, executorService).forPath(ZK_NODE);

        TimeUnit.SECONDS.sleep(Integer.MAX_VALUE);
    }


    public static final String NODE_CACHE = "/node-cache";

    @Test
    public void testNodeCacheTest() throws Exception {

        createIfNeed(NODE_CACHE);
        NodeCache nodeCache = new NodeCache(curatorFramework, NODE_CACHE);
        nodeCache.getListenable().addListener(new NodeCacheListener() {
            @Override
            public void nodeChanged() throws Exception {
                log.info("{} path nodeChanged: ", NODE_CACHE);
                printNodeData();
            }
        });

        nodeCache.start();
    }

    private void createIfNeed(String nodeCache) throws Exception {
        String path = curatorFramework.create().withMode(CreateMode.PERSISTENT).forPath(  nodeCache, "some-data".getBytes());

    }


    public void printNodeData() throws Exception {
        byte[] bytes = curatorFramework.getData().forPath(NODE_CACHE);
        log.info("data: {}", new String(bytes));
    }


    public static final String PATH="/path-cache";

    @Test
    public void testPathCache() throws Exception {

        createIfNeed(PATH);
        PathChildrenCache pathChildrenCache = new PathChildrenCache(curatorFramework, PATH, true);
        pathChildrenCache.getListenable().addListener(new PathChildrenCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
                log.info("event:  {}",event);
            }
        });

        // 如果设置为true则在首次启动时就会缓存节点内容到Cache中
        pathChildrenCache.start(true);
    }


    public static final String TREE_CACHE="/tree-path";

    @Test
    public void testTreeCache() throws Exception {
        createIfNeed(TREE_CACHE);
        TreeCache treeCache = new TreeCache(curatorFramework, TREE_CACHE);
        treeCache.getListenable().addListener(new TreeCacheListener() {
            @Override
            public void childEvent(CuratorFramework client, TreeCacheEvent event) throws Exception {
                log.info(" tree cache: {}",event);
            }
        });
        treeCache.start();
    }
}
