package top.lxl.mydb.backend.dm.pageIndex;

import top.lxl.mydb.backend.dm.pageCache.PageCache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lxl
 * @version 1.0
 */
public class PageIndex {
    private static final int INTERVALS_NO = 40;                                         // 分成40个区间
    private static final int  THRESHOLD = PageCache.PAGE_SIZE / INTERVALS_NO;           // 每个区间的大小

    private Lock lock;
    private List<PageInfo>[] lists;

    public PageIndex() {
        lock = new ReentrantLock();
        lists = new List[INTERVALS_NO + 1];
        for(int i = 0; i < INTERVALS_NO + 1; i++) {
            lists[i] = new ArrayList<>();
        }
    }

    // 添加页面的空闲信息
    public void add(int pgNo, int freeSpace) {
        int number = freeSpace / THRESHOLD;
        lock.lock();
        try {
            lists[number].add(new PageInfo(pgNo, freeSpace));
        } finally {
            lock.unlock();
        }
    }

    // 当取出了这个空闲页面，别的线程无法检索到该页面，一个页面只支持单个线程使用，不支持多个线程并发使用
    public PageInfo select(int spaceSize) {
        try {
            int number = (spaceSize + THRESHOLD - 1) / THRESHOLD;
            lock.lock();
            while(number <= INTERVALS_NO) {
                if(lists[number].size() == 0) {
                    number++;
                    continue;
                }
                return lists[number].remove(0);
            }
            return null;
        } finally {
            lock.unlock();
        }
    }


}
