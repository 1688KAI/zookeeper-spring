package com.zookeeper.controller;

import com.zookeeper.util.ZookeeperDistributedLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/testZK")
public class ZookeepLockController {

    private static final Logger log = LoggerFactory.getLogger(ZookeepLockController.class);

    @Autowired
    StringRedisTemplate stringRedisTemplate;

    public static final String STOCK_KEY = "stock";
    public static final String LOCK_KEY = "lock";

    @Autowired
    ZookeeperDistributedLock zookeeperDistributedLock;

    /**
     * 第一个版本 不控制并发情况
     * <p>
     * 多个线程获取同一个库存数量
     *
     * @return
     */
    @ResponseBody
    @GetMapping(value = "/lock/v1")
    public String v1() {
        try {
            zookeeperDistributedLock.lock(LOCK_KEY);
            String value = stringRedisTemplate.opsForValue().get(STOCK_KEY);
            Integer stock = Integer.valueOf(value);
            if (stock > 0) {
                stringRedisTemplate.opsForValue().set(STOCK_KEY, String.valueOf(--stock));
                System.out.println("threadName =" + Thread.currentThread().getName() + ", stock=" + stock);
            }
        } catch (Exception e) {
        } finally {
            zookeeperDistributedLock.unlock();
        }
        return "hello";
    }


}
