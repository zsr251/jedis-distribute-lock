package ren.k88.distribute;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 简单分布式锁测试
 * Created by jasonzhu on 2017/3/7.
 */
public class LockTest {
    //结果
    public static int count = 0;
    public static CountDownLatch countDownLatch;

    public static AtomicInteger atomicInteger = new AtomicInteger(0);
    //线程睡眠时间
    public static long sleep = 200;

    public static void main(String[] args) throws Exception {
        //线程数
        int n = 5;
        long begin;

        count = 0;
        countDownLatch = new CountDownLatch(n);
        begin = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Thread t = new Thread(new AddUnsafeThread("unsafe-" + i));
            t.start();
        }
        countDownLatch.await();
        System.out.println("新建【" + n + "】个不安全线程，执行后结果为【" + count + "】【"+LockTest.atomicInteger.get()+"】 执行时间【" + (System.currentTimeMillis() - begin) + "】毫秒");

        JedisPoolConfig config = new JedisPoolConfig();
        //最大连接数
        config.setMaxTotal(2000);
        //最小空闲连接数, 默认0
        config.setMinIdle(1);
        JedisPool jedisPool = new JedisPool(config, "127.0.0.1", 6379);
        System.out.println("redis连接成功");
        count = 0;
        countDownLatch = new CountDownLatch(n);
        begin = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            Thread t = new Thread(new AddSafeThread("safe-" + i, jedisPool));
            t.start();
        }
        countDownLatch.await();
        System.out.println("新建【" + n + "】个安全线程，执行后结果为【" + count + "】执行时间【" + (System.currentTimeMillis() - begin) + "】毫秒");
    }

}

/**
 * 安全线程
 */
class AddSafeThread implements Runnable {
    private String name;
    private JedisPool jedisPool;

    AddSafeThread(String name, JedisPool jedisPool) {
        this.name = name;
        this.jedisPool = jedisPool;
    }
    @Override
    public void run() {
        DistributeLock lock = new DistributeLock(jedisPool, "add");
        String flag = lock.getThreadFlag();

        try {
            System.out.println("尝试获得锁，线程标识【"+flag+"】");
            lock.lock(10, 600,flag);
            System.out.println("尝试再次获得锁，线程标识【"+flag+"】");
            lock.lock(10, 600,flag);
        } catch (DistributeLockException e) {
            System.out.println(e.getMessage());
            LockTest.countDownLatch.countDown();
            return;
        }
        try {
            Thread.sleep(LockTest.sleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LockTest.count++;
        System.out.println("安全线程【" + name + "】 执行后结果【" + LockTest.count + "】线程标识【"+flag+"】");
        try {
            Thread.sleep(LockTest.sleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        flag = lock.getThreadFlag();
        if(lock.tryUnlock(flag)) {
            System.out.println("锁释放成功，线程标识【" + flag + "】");
        }else {
            System.out.println("锁释放失败，线程标识【" + flag + "】");
        }
        if(lock.tryUnlock(flag)) {
            System.out.println("锁释放成功，线程标识【" + flag + "】");
        }else {
            System.out.println("锁释放失败，线程标识【" + flag + "】");
        }
        LockTest.countDownLatch.countDown();
    }
}

/**
 * 不安全线程
 */
class AddUnsafeThread implements Runnable {
    private String name;

    public AddUnsafeThread(String name) {
        this.name = name;
    }
    @Override
    public void run() {
        try {
            Thread.sleep(LockTest.sleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LockTest.count++;
        LockTest.atomicInteger.incrementAndGet();
        System.out.println("不安全线程【" + name + "】 执行后结果【" + LockTest.count + "】");
        try {
            Thread.sleep(LockTest.sleep);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        LockTest.countDownLatch.countDown();
    }
}
