package ren.k88.distribute;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.UUID;

/**
 * 简单可重入分布式锁
 * 原理：使用redis的lua脚本实现 lua脚本可保证原子性，使用BLPOP实现释放锁时的通知，可防止等待线程自旋浪费cpu资源
 * 问题：1.可能会出现redis key超时时，锁通知队列中没有通知，造成假性死锁，需要等待下一个获得锁的线程进行通知
 * 2.锁释放通知列表键 在所有请求处理完成后 不会自动删除 但在实际场景中可以接受
 * 解决方案一：使用BLPOP设置超时时间，使锁定时间可控，同时控制线程饥饿时间
 * 解决方案二：另起一个线程检测redis中所有的锁释放通知队列的长度，如果对应的锁标识为未赋值则通知释放锁消息
 * Created by jasonzhu on 2017/3/7.
 */
public class DistributeLock {
    /**
     * 默认的线程标识
     */
    public static ThreadLocal<String> threadFlag = new ThreadLocal<>();
    /**
     * 线程标识
     */
    private static String THREAD_FLAG_NUM = "thread:flag:num";
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
    /**
     * 获得锁 lua脚本
     * 三个参数：key、线程标识、超时时间
     */
    public static String LOCK_SCRIPT = "local f = redis.call('HGET',KEYS[1],'flag');if type(f) == 'string' and f ~= KEYS[2] then return 0;end redis.call('HSET',KEYS[1],'flag',KEYS[2]);redis.call('EXPIRE',KEYS[1],KEYS[3]);local c = redis.call('HGET',KEYS[1],'count');if type(c) ~= 'string' or tonumber(c) < 0 then redis.call('HSET',KEYS[1],'count',1);else redis.call('HSET',KEYS[1],'count',c+1);end return 1";
    /**
     * 释放锁 lua脚本
     * 两个参数：key、线程标识
     */
    public static String UNLOCK_SCRIPT = "local f = redis.call('HGET',KEYS[1],'flag');if type(f) ~= 'string' or (type(f) == 'string' and f ~= KEYS[2]) then return 0;end local c = redis.call('HGET',KEYS[1],'count');if type(c) ~= 'string' or tonumber(c) < 2 then redis.call('DEL',KEYS[1]);return 1;else redis.call('HSET',KEYS[1],'count',c-1);return 2;end";

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
    }


    /**
     * 获得锁
     */
    public void lock() {
        lock(LOCK_EXPIRE_SECOND, WAIT_SECOND, getThreadFlag());
    }

    /**
     * 获得锁
     *
     * @param expireSecond 锁超时时间
     * @param waitSecond   锁等待时间
     * @param flag         线程标识
     */
    public void lock(int expireSecond, int waitSecond, String flag) {
        if (!tryLock(expireSecond, waitSecond, flag)) {
            throw new DistributeLockException(DistributeLockException.WAIT_LOCK_TIMEOUT, "获取锁超时");
        }
    }

    /**
     * 释放锁
     */
    public boolean unlock() {
        return unlock(getThreadFlag());
    }

    /**
     * 释放锁
     *
     * @param flag 线程标识
     * @return
     */
    public boolean unlock(String flag) {
        return tryUnlock(flag);
    }

    /**
     * 获得锁
     *
     * @param expireSecond 持有锁超时秒数
     * @param waitSecond   等待锁超时秒数
     * @param flag         线程标识
     * @return
     */
    public boolean tryLock(int expireSecond, int waitSecond, String flag) {
        Jedis jedis = jedisPool.getResource();
        try {
            //2017-03-16 修复递归会造成的 资源无限获取且需递归释放的问题
            return tryLockInner(jedis, expireSecond, waitSecond, flag);
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
     * @param flag         线程标识
     * @return
     */
    private boolean tryLockInner(Jedis jedis, int expireSecond, int waitSecond, String flag) {
        // 尝试获得锁 如果自身持有锁则可以再次获得
        if ((Long) jedis.eval(LOCK_SCRIPT, 3, redisLockKey, flag, "" + expireSecond) > 0) {
            return true;
        }
        //阻塞等待释放锁通知
        List<String> lp = jedis.blpop(waitSecond, redisListKey);
        if (lp == null || lp.size() < 1) {
            //如果超时则返回锁定失败
            return false;
        }
        return tryLockInner(jedis, expireSecond, waitSecond, flag);
    }

    /**
     * 释放锁
     *
     * @param flag 线程标识
     * @return
     */
    public boolean tryUnlock(String flag) {
        Jedis jedis = jedisPool.getResource();
        try {
            //删除锁定的key
            Long l = (Long) jedis.eval(UNLOCK_SCRIPT, 2, redisLockKey, flag);
            if (l < 1) {
                return false;
            }
            // 因为是可重入锁 所以释放成功不一定会释放锁
            if (l.intValue() == 2) {
                return true;
            }
            //如果锁释放消息队列里没有值 则释放一个信号
            if (l.intValue() == 1 && jedis.llen(redisListKey).intValue() == 0) {
                //通知等待的线程可以继续获得锁
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
     * 获取线程标识
     *
     * @return 标识符
     */
    public String getThreadFlag() {
        String flag = threadFlag.get();
        if (flag != null && flag.length() > 0) {
            return flag;
        }
        long num = 0;
        Jedis jedis = jedisPool.getResource();
        try {
            num = jedis.incr(THREAD_FLAG_NUM);
        } finally {
            jedis.close();
        }
        flag = "" + num;
        threadFlag.set(flag);
        return flag;
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
