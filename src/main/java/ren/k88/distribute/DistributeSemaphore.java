package ren.k88.distribute;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * 简单分布式信号量
 * 原理：使用redis中的 incr 和 decr实现信号量的增加和释放 超时可实现定时释放全部信号量功能
 * 问题：某一个获得信号量的线程意外关闭时，会造成一个信号量无法释放
 * 解决方案：1.提供释放所有信号量方法
 * Created by jasonzhu on 2017/3/16.
 */
public class DistributeSemaphore {
    /**
     * 锁 redis key前缀
     */
    private static String REDIS_KEY = "semaphore:";
    /**
     * 等待锁默认超时时间
     */
    private static int WAIT_SECOND = 60;
    /**
     * 信号量个数
     */
    private int permits = 1;
    /**
     * 信号量超时时间
     */
    private static int expireSecond = -1;
    private JedisPool jedisPool;
    /**
     * redis中信号量key
     */
    private String redisSemaphoreKey;
    /**
     * redis释放信号量通知列表
     */
    private String redisListKey;
//    /**
//     * redis等待线程数
//     */
//    private String redisWaitKey;

    private DistributeLock lock;

    /**
     * 实例化简单分布式锁
     *
     * @param jedisPool redis连接池
     */
    public DistributeSemaphore(JedisPool jedisPool) {
        this(jedisPool, generateKey(), 1, -1);
    }

    /**
     * 实例化简单分布式锁
     *
     * @param jedisPool         redis连接池
     * @param redisSemaphoreKey 在redis中锁的key
     */
    public DistributeSemaphore(JedisPool jedisPool, String redisSemaphoreKey, int permits, int expireSecond) {
        this.jedisPool = jedisPool;
        this.redisSemaphoreKey = REDIS_KEY + "value:" + redisSemaphoreKey;
        this.redisListKey = REDIS_KEY + "list:" + redisSemaphoreKey;
//        this.redisWaitKey = REDIS_KEY + "wait:" + redisSemaphoreKey;
        this.permits = permits > 0 ? permits : 1;
        this.expireSecond = expireSecond;
        lock = new DistributeLock(jedisPool, REDIS_KEY + redisSemaphoreKey);
    }

    /**
     * 获得可用信号量 非原子操作 结果供参考
     */
    public int getSemaphore() {
        Jedis jedis = jedisPool.getResource();
        try {
            lock.lock();
            try {
                String countStr = jedis.get(redisSemaphoreKey);
                if (countStr == null || countStr.length() < 1) {
                    return permits;
                }
                return permits - Integer.parseInt(countStr);
            } finally {
                //释放锁
                lock.unlock();
            }
        } finally {
            jedis.close();
        }
    }

//    /**
//     * 获得等待线程数 非原子操作 结果供参考
//     */
//    public int getWaitCount() {
//        Jedis jedis = jedisPool.getResource();
//        try {
//            int n = getInt(jedis.get(redisWaitKey));
//            //如果依然后信号量 则删除
//            if (n < 0) {
//                jedis.del(redisWaitKey);
//                return 0;
//            }
//            //三重判断 清除 还有信号量但是有等待线程的个数
//            if (permits - getInt(jedis.get(redisSemaphoreKey)) > 0 && permits - getInt(jedis.get(redisSemaphoreKey)) > 0) {
//                try {
//                    Thread.sleep(10);
//                } catch (Exception e) {
//                }
//                if (permits - getInt(jedis.get(redisSemaphoreKey)) > 0) {
//                    jedis.del(redisWaitKey);
//                    return 0;
//                }
//            }
//            return n;
//        } finally {
//            jedis.close();
//        }
//    }

    /**
     * 获得单个信号量
     */
    public void acquire() {
        acquire(1, WAIT_SECOND);
    }

    /**
     * 获得信号量
     *
     * @param n          信号量个数
     * @param waitSecond 等待超时时间
     */
    public void acquire(int n, int waitSecond) {
        if (!tryAcquire(n, WAIT_SECOND)) {
            throw new DistributeSemaphoreException(DistributeSemaphoreException.WAIT_SEMAPHORE_TIMEOUT, "获取信号量超时");
        }
    }

    /**
     * 获得信号量
     *
     * @param n          信号量个数
     * @param waitSecond 等待超时时间
     * @return true 获得成功 false 超时
     */
    public boolean tryAcquire(int n, int waitSecond) {
        Jedis jedis = jedisPool.getResource();
        try {
            return tryAcquireInner(jedis, n, waitSecond);
        } finally {
            jedis.close();
        }
    }

    /**
     * 获得信号量
     *
     * @param jedis      redis连接
     * @param n          信号量个数
     * @param waitSecond 等待超时时间
     */
    public boolean tryAcquireInner(Jedis jedis, int n, int waitSecond) {
        //锁
        lock.lock();
        try {
            long cu = jedis.incrBy(redisSemaphoreKey, n);
            if (cu - n < 0) {
                //删除信号量
                jedis.del(redisSemaphoreKey);
                cu = jedis.incrBy(redisSemaphoreKey, n);
            }
            //获得信号量成功
            if (cu <= permits) {
                //设置超时时间
                if (expireSecond > 0 && cu - n == 0) {
                    jedis.expire(redisSemaphoreKey, expireSecond);
                }
                return true;
            } else if (cu - n > permits) {
                //如果当前信号量 大于最大信号量 更新为最大信号量
                jedis.set(redisSemaphoreKey, "" + permits);
            } else {
                //已经超过限制 减少最近获得的信号量
                jedis.decrBy(redisSemaphoreKey, n);
            }
        } finally {
            //释放锁
            lock.unlock();
        }
//        //增加等待线程数
//        jedis.incr(redisWaitKey);
        //阻塞等待释放信号量通知
        List<String> lp = jedis.blpop(waitSecond, redisListKey);
//        //减少等待线程数
//        jedis.decr(redisWaitKey);
        if (lp == null ||lp.size() < 1) {
            //如果超时则返回锁定失败
            return false;
        }
        return tryAcquireInner(jedis, n, waitSecond);
    }

    /**
     * 释放单个信号量
     */
    public void release() {
        release(1);
    }

    /**
     * 释放信号量
     *
     * @param n 信号量个数
     */
    public void release(int n) {
        Jedis jedis = jedisPool.getResource();
        try {
            lock.lock();
            try {
                long cur = jedis.decr(redisSemaphoreKey);
                if (cur < 0) {
                    jedis.del(redisSemaphoreKey);
                }
            } finally {
                //释放锁
                lock.unlock();
            }
            for (int i = 0; i < n; i++) {
                //通知等待的线程可以继续获得锁 非公平锁
                jedis.rpush(redisListKey, "ok");
            }
        } finally {
            jedis.close();
        }
    }

    /**
     * 释放所有信号量
     */
    public void releaseAll() {
        Jedis jedis = jedisPool.getResource();
        try {
            String countStr = jedis.get(redisSemaphoreKey);
            if (countStr == null || countStr.length() < 1) {
                return;
            }
            jedis.del(redisSemaphoreKey);
            for (int i = 0; i < Integer.parseInt(countStr); i++) {
                //通知等待的线程可以继续获得锁 非公平锁
                jedis.rpush(redisListKey, "ok");
            }
        } finally {
            jedis.close();
        }
    }


    /**
     * 生成唯一key
     */
    private static String generateKey() {
        return new StringBuilder(new SimpleDateFormat("yyMMddHHmmssSSS").format(new Date())).append(UUID.randomUUID().toString().substring(0, 3)).toString();
    }

    /**
     * 转换类型
     */
    private int getInt(String str) {
        if (str == null || str.length() < 1) {
            return 0;
        }
        return Integer.parseInt(str);
    }
}

/**
 * 自定义分布式信号量异常
 */
class DistributeSemaphoreException extends RuntimeException {
    private Integer code;
    /**
     * 其他
     */
    public static Integer OTHER = 0;
    /**
     * 信号量获取等待超时
     */
    public static Integer WAIT_SEMAPHORE_TIMEOUT = 1;

    public DistributeSemaphoreException(Integer code, String message) {
        super(message);
        this.code = code;
    }

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }
}
