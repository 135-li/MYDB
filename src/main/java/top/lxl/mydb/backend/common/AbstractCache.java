package top.lxl.mydb.backend.common;

import top.lxl.mydb.common.Error;

import java.util.HashMap;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lxl
 * @version 1.0
 */
public abstract class AbstractCache<T> {
    private HashMap<Long, T> cache;                // 实际缓存的数据
    private HashMap<Long, Integer> references;     // 资源引用计数
    private HashMap<Long, Boolean>  getting;       // 多个线程同时获取同一资源时，当前资源不在缓存中，都需要从硬盘中获取，记录当前资源是否正在从硬盘中刷到磁盘

    private int maxResource;                       // 缓存中可以存放的最大资源数
    private int count = 0;                         // 缓存中已经存放的资源数量
    private Lock lock;

    public AbstractCache(int maxResource) {
        this.maxResource = maxResource;
        cache = new HashMap<>();
        references = new HashMap<>();
        getting = new HashMap<>();
        lock = new ReentrantLock();
    }

    /**
     * 给上层模块的接口，用于获取资源，屏蔽缓存细节
     */
    protected T get(long key) throws Exception {
        while(true) {
          lock.lock();

          if(getting.containsKey(key)) {
              lock.unlock();
              try {
                  Thread.sleep(1);
              } catch (InterruptedException e) {
                  e.printStackTrace();
                  continue;
              }
          }

          if(cache.containsKey(key)) {
              T obj = cache.get(key);
              references.put(key, references.get(key) + 1);
              lock.unlock();
              return obj;
          }

          if(maxResource > 0 && count == maxResource) {
              lock.unlock();
              throw Error.CacheFullException;
          }
          count++;
          getting.put(key, true);
          lock.unlock();
          break;
        }

        // 从硬盘中获取资源，并缓存
        T obj = null;
        try {
            obj = getForCache(key);
        } catch (Exception e) { // 获取失败
            lock.lock();
            count--;
            getting.remove(key);
            lock.unlock();
            throw e;
        }

        lock.lock();
        getting.remove(key);
        cache.put(key, obj);
        references.put(key, 1);
        lock.unlock();

        return obj;
    }


    /**
     * 上层资源使用完后，对引用计数更新，如果引用计数变为0，刷回缓存
     */
    protected void release(long key) {
        lock.lock();
        try {
            int ref = references.get(key) - 1;
            if(ref == 0) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            } else {
                references.put(key, ref);
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 关闭缓存，写回所有资源
     */
    protected void close() {
        lock.lock();
        try {
            Set<Long> keys = cache.keySet();
            for(Long key : keys) {
                T obj = cache.get(key);
                releaseForCache(obj);
                references.remove(key);
                cache.remove(key);
                count--;
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 当资源不存在时，从磁盘中获取，缓存在内存中
     */
    protected abstract T getForCache(long key) throws Exception;

    /**
     * 根据引用计数原则，驱逐缓存，刷回硬盘
     */
    protected abstract void releaseForCache(T obj);
}
