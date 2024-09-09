package top.lxl.mydb.backend.dm.pageCache;

import top.lxl.mydb.backend.dm.page.MockPage;
import top.lxl.mydb.backend.dm.page.Page;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lxl
 * @version 1.0
 */
public class MockPageCache implements PageCache {
    private HashMap<Integer, MockPage> cache = new HashMap<>();
    private Lock lock = new ReentrantLock();
    private AtomicInteger pageNumbers = new AtomicInteger(0);

    @Override
    public int newPage(byte[] initData) {
        int pgNo = pageNumbers.incrementAndGet();
        MockPage pg = new MockPage(pgNo, initData);
        lock.lock();
        try {
            cache.put(pgNo, pg);
            return pgNo;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Page getPage(int pgNo) throws Exception {
        lock.lock();
        try {
            return cache.get(pgNo);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void release(Page page) {}

    @Override
    public void close() {}

    @Override
    public void truncateByPgNo(int maxPgNo) {}

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {}
}
