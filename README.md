 **++当前代码只适用于非常简单的业务场景，稍微复杂的建议使用 [redisson](https://github.com/redisson/redisson)++**

# jedis-distribute-lock
使用redis实现分布式锁 分布式信号量 可解决等待线程自旋造成CPU资源浪费的问题

问题：锁等待或信号量等待时，redis连接需要保持 暂无方法解决  
参考文章：http://www.javasoso.com/articles/2017/12/11/1513007411963.html
## 简单分布式锁

### 原理

原理：使用redis的lua脚本实现 lua脚本可保证原子性，使用BLPOP实现释放锁时的通知，可防止等待线程自旋浪费cpu资源

### 问题

1. 可能会出现redis key超时时，锁通知队列中没有通知，造成假性死锁，需要等待下一个获得锁的线程进行通知
2. 锁释放通知列表键 在所有请求处理完成后 不会自动删除 但在实际场景中可以接受

### 解决方案

1. 使用BLPOP设置超时时间，使锁定时间可控，同时控制线程饥饿时间
2. 另起一个线程检测redis中所有的锁释放通知队列的长度，如果对应的锁标识为未赋值则通知释放锁消息

## 简单分布式信号量

### 原理
使用redis中的 incr 和 decr实现信号量的增加和释放 可实现定时释放全部信号量功能

### 问题
1. 某一个获得信号量的线程意外关闭时，会造成一个信号量无法释放
### 解决方案
1. 提供释放所有信号量方法
### 欢迎关注我的公众号和[JavaSoSo博客](http://www.javasoso.com)
![JavaSoSo公众号](https://i.loli.net/2017/11/24/5a177ebc75827.jpg) 

