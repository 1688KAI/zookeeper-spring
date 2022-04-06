package com.zookeeper.config;


import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

@Configuration
public class ZooKeeperConfig {

    @Value("${zookeeper.connectString}")
    private String zkQurom;

    private CountDownLatch connectedSignal = new CountDownLatch(1);

    private String root = "/locks";
    private final static byte[] data = new byte[0];

    @Bean("zooKeeper")
    public ZooKeeper zooKeeperClient() throws IOException {

        ZooKeeper zk = new ZooKeeper(zkQurom, 6000, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println("Receive event " + watchedEvent);
                if (Event.KeeperState.SyncConnected == watchedEvent.getState()) {
                    System.out.println("connection is established...");
                }
                if (watchedEvent.getState() == Event.KeeperState.SyncConnected) {
                    connectedSignal.countDown();
                }
            }
        });
        try {
            connectedSignal.await();
            Stat stat = zk.exists(root, false);
            if (null == stat) {
                // 创建根节点
                zk.create(root, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return zk;
    }
}
