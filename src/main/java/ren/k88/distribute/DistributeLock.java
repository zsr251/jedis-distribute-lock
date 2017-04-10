package ren.k88.distribute;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * 简单分布式锁
 * 原理：使用redis的SETNX实现，使用BLPOP实现释放锁时的通知，可防止等待线程自旋浪费cpu资源
 * 问题：1.可能会出现redis key超时时，锁通知队列中没有通知，造成假性死锁，需要等待下一个获得锁的线程进行通知
 * 2.锁释放通知列表键 在所有请求处理完成后 不会自动删除 但在实际场景中可以接受
 * 解决方案一：使用BLPOP设置超时时间，使锁定时间可控，同时控制线程饥饿时间
 * 解决方案二：另起一个线程检测redis中所有的锁释放通知队列的长度，如果对应的锁标识为未赋值则通知释放锁消息
 * Created by jasonzhu on 2017/3/7.
 */
public class DistributeLock {
    /**
     * 锁 redis key前缀
     */
    private static String REDIS_KEY = "lock:";
    /**
     * 锁默认超时时间
     */
    private static int LOCK_EXPIRE_SECOND = 60;
    /**
     * 等待锁默认超时时间
     */
    private static int WAIT_SECOND = 60;
    private JedisPool jedisPool;
    /**
     * redis中锁key
     */
    private String redisLockKey;
    /**
     * redis释放锁通知列表
     */
    private String redisListKey;
//    /**
//     * redis等待线程数
//     */
//    private String redisWaitKey;

    /**
     * 实例化简单分布式锁
     *
     * @param jedisPool redis连接池
     */
    public DistributeLock(JedisPool jedisPool) {
        this(jedisPool, generateKey());
    }

    /**
     * 实例化简单分布式锁
     *
     * @param jedisPool    redis连接池
     * @param redisLockKey 在redis中锁的key
     */
    public DistributeLock(JedisPool jedisPool, String redisLockKey) {
        this.jedisPool = jedisPool;
        this.redisLockKey = REDIS_KEY + "value:" + redisLockKey;
        this.redisListKey = REDIS_KEY + "list:" + redisLockKey;
//        this.redisWaitKey = REDIS_KEY + "wait:" + redisLockKey;
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
//            if (!jedis.exists(redisLockKey) && !jedis.exists(redisLockKey)) {
//                try {
//                    Thread.sleep(10);
//                } catch (Exception e) {
//                }
//                if (!jedis.exists(redisLockKey)) {
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
     * 获得锁
     */
    public void lock() {
        lock(LOCK_EXPIRE_SECOND, WAIT_SECOND);
    }

    /**
     * 获得锁
     */
    public void lock(int expireSecond, int waitSecond) {
        if (!tryLock(expireSecond, waitSecond)) {
            throw new DistributeLockException(DistributeLockException.WAIT_LOCK_TIMEOUT, "获取锁超时");
        }
    }

    /**
     * 释放锁
     */
    public void unlock() {
        tryUnlock();
    }

    /**
     * 获得锁
     *
     * @param expireSecond 持有锁超时秒数
     * @param waitSecond   等待锁超时秒数
     * @return
     */
    public boolean tryLock(int expireSecond, int waitSecond) {
        Jedis jedis = jedisPool.getResource();
        try {
            //2017-03-16 修复递归会造成的 资源无限获取且需递归释放的问题
            return tryLockInner(jedis, expireSecond, waitSecond);
        } finally {
            jedis.close();
        }
    }

    /**
     * 获得锁
     *
     * @param jedis        redis连接
     * @param expireSecond 持有锁超时秒数
     * @param waitSecond   等待锁超时秒数
     * @return
     */
    private boolean tryLockInner(Jedis jedis, int expireSecond, int waitSecond) {
        //尝试获得锁
        if (jedis.setnx(redisLockKey, "lock") > 0) {
            //成功获得锁 设置超时时间
            jedis.expire(redisLockKey, expireSecond);
            return true;
        }
//        //增加等待线程数
//        jedis.incr(redisWaitKey);
        //阻塞等待释放锁通知
        List<String> lp = jedis.blpop(waitSecond, redisListKey);
//        //减少等待线程数
//        jedis.decr(redisWaitKey);
        if (lp==null || lp.size() < 1) {
            //如果超时则返回锁定失败
            return false;
        }
        return tryLockInner(jedis, expireSecond, waitSecond);
    }

    /**
     * 释放锁
     *
     * @return
     */
    public boolean tryUnlock() {
        Jedis jedis = jedisPool.getResource();
        try {
            //删除锁定的key
            jedis.del(redisLockKey);
            //如果锁释放消息队列里没有值 则释放一个信号
            if (jedis.llen(redisListKey).intValue() == 0) {
                //通知等待的线程可以继续获得锁 非公平锁
                jedis.rpush(redisListKey, "ok");
            }
            return true;
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
 * 自定义分布式锁异常
 */
class DistributeLockException extends RuntimeException {
    private Integer code;
    /**
     * 其他
     */
    public static Integer OTHER = 0;
    /**
     * 锁获取等待超时
     */
    public static Integer WAIT_LOCK_TIMEOUT = 1;

    public DistributeLockException(Integer code, String message) {
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
