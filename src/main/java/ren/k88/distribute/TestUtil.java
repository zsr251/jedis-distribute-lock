package ren.k88.distribute;

import redis.clients.jedis.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by jasonzhu on 2016/11/8.
 */
public class TestUtil {
    public static void testSimple(JedisPool jedisPool){
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.del("a");
            jedis.setnx("a","1");
            System.out.println(jedis.get("a"));
            jedis.set("a","12");
            System.out.println(jedis.get("a"));
            jedis.setnx("a","123");
            System.out.println(jedis.get("a"));
            jedis.expire("a",1);
            System.out.println(jedis.get("a"));
            System.out.println("暂停一秒 ttl:"+jedis.ttl("a"));
            Thread.sleep(1100);
            System.out.println(jedis.get("a"));
            jedis.setex("a",2,"1234");
            System.out.println(jedis.get("a"));
            System.out.println("暂停两秒 ttl:"+jedis.ttl("a"));
            Thread.sleep(2100);
            System.out.println(jedis.get("a"));

        }catch (Exception e){}
        finally {
            jedis.close();
        }
    }
    public static void testRank(JedisPool jedisPool){
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.del("z");
            jedis.zadd("z",49,"小明");
            jedis.zadd("z",30,"小红");
            jedis.zadd("z",98.5,"小暗");
            jedis.zadd("z",70.4,"小绿");
            Long count = jedis.zcard("z");
            System.out.println("排行榜总人数:"+count);
            System.out.println("倒数第一名:"+jedis.zrange("z",0,0).iterator().next());
            System.out.println("小明排行:"+(jedis.zrevrank("z","小明")+1)+" 分数："+jedis.zscore("z","小明"));
            System.out.println("成绩排序：");
            Set<String> ranks=jedis.zrevrange("z",0,-1);
            for (String rank : ranks) {
                System.out.println(rank);
            }
            System.out.println("小明加50.5分");
            jedis.zincrby("z",50.5,"小明");
            System.out.println("小明排行:"+(jedis.zrevrank("z","小明")+1));

        }finally {
            jedis.close();
        }
    }
    public static void testTransaction(JedisPool jedisPool ){
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.watch("a");
            Transaction tx = jedis.multi();
            tx.set("a", "12");
            tx.del("l");
            tx.lpush("l", "1", "2", "3");
            tx.exec();
            System.out.println(jedis.get("a"));
            List<String> list = jedis.lrange("l", 0, -1);
            for (String s : list) {
                System.out.println(s);
            }
        }finally {
            jedis.close();
        }
    }
    public static void testPipeline(JedisPool jedisPool ){
        Jedis jedis = jedisPool.getResource();
        try {
            Map<String, Response<String>> responseMap = new HashMap<String, Response<String>>();
            Pipeline p = jedis.pipelined();
            p.set("a", "1");
            p.set("b", "2");
            p.set("c", "3");
            p.set("d", "4");
            Response<String> a = p.get("a");
            Response<String> b = p.get("b");
            Response<String> c = p.get("c");
            Response<String> d = p.get("d");
//        System.out.println(a.get());
//        System.out.println(b.get());
//        System.out.println(c.get());
//        System.out.println(d.get());
            p.sync();
            System.out.println(a.get());
            System.out.println(b.get());
            System.out.println(c.get());
            System.out.println(d.get());
        }finally {
            jedis.close();
        }
    }
    public static void testSub(JedisPool jedisPool ){
        final Jedis jedis = jedisPool.getResource();
        try {
            JedisPubSub pubSub = new JedisPubSub() {
                @Override
                public void onMessage(String channel, String message) {
                    System.out.println("收到频道 : 【" + channel + " 】的消息 ：" + message);
                }
            };
            jedis.subscribe(pubSub, "chan");
        }finally {
            jedis.close();
        }
    }
    public static void main(String[] args) {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(5);
        JedisPool jedisPool = new JedisPool(config,"127.0.0.1",6379);
        //普通命令
        testSimple(jedisPool);
        //排行榜
        testRank(jedisPool);
        //事务
        testTransaction(jedisPool);
        //管道
        testPipeline(jedisPool);
        //订阅
        testSub(jedisPool);

    }
}
