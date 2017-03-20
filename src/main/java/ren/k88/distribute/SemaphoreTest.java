package ren.k88.distribute;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by jasonzhu on 2017/3/17.
 */
public class SemaphoreTest {
    /**
     * 定时任务
     */
    public static ScheduledExecutorService scheduled = Executors.newSingleThreadScheduledExecutor();
    public static CountDownLatch countDownLatch;
    public static AtomicInteger atomicInteger = new AtomicInteger(0);

    public static void main(String[] args) throws Exception {
        //线程数
        int n = 100;
        //信号量
        int permits = 3;
        JedisPoolConfig config = new JedisPoolConfig();
        //最大连接数
        config.setMaxTotal(2000);
        //最小空闲连接数, 默认0
        config.setMinIdle(1);
        JedisPool jedisPool = new JedisPool(config, "127.0.0.1", 6379);
        System.out.println("redis连接成功");
        SemaphoreTest.countDownLatch = new CountDownLatch(n);
        //定时释放
//        scheduled.scheduleAtFixedRate(new ReleaseTask(jedisPool, permits), 3, 3, TimeUnit.SECONDS);
        long begin = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Thread t = new Thread(new DoTask(jedisPool, permits, "thread-" + i));
            t.start();
        }
        countDownLatch.await();
        System.out.println("新建【" + n + "】个线程 执行时间【" + (System.currentTimeMillis() - begin) + "】毫秒");

    }

}

/**
 * 执行获取信号量
 */
class DoTask implements Runnable {
    private String name;
    private JedisPool jedisPool;
    private int permits;

    public DoTask(JedisPool jedisPool, int permits, String name) {
        this.jedisPool = jedisPool;
        this.permits = permits;
        this.name = name;
    }

    @Override
    public void run() {
        DistributeSemaphore semaphore = new DistributeSemaphore(jedisPool, "cps", permits, 10);
        semaphore.acquire();
        int n = SemaphoreTest.atomicInteger.incrementAndGet();
        System.out.println("-- 线程【" + name + "】获得信号量 可用信号量【" + semaphore.getSemaphore() + "】累加结果【" + n + "】");
        //增加释放操作 ----
        semaphore.release();
        System.out.println("++ 线程【" + name + "】释放信号量 可用信号量【" + semaphore.getSemaphore() + "】累加结果【" + n + "】");
        // --------------
        SemaphoreTest.countDownLatch.countDown();
    }
}

/**
 * 释放线程
 */
class ReleaseTask implements Runnable {
    private JedisPool jedisPool;
    private int permits;

    public ReleaseTask(JedisPool jedisPool, int permits) {
        this.jedisPool = jedisPool;
        this.permits = permits;
    }

    @Override
    public void run() {
        DistributeSemaphore semaphore = new DistributeSemaphore(jedisPool, "cps", permits, 10);
        System.out.println("释放前 可用信号量【" + semaphore.getSemaphore() + "】");
        semaphore.releaseAll();
        SemaphoreTest.atomicInteger.set(0);
        System.out.println("释放后 可用信号量【" + semaphore.getSemaphore() + "】");
    }
}