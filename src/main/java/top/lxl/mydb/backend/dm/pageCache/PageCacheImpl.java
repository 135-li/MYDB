package top.lxl.mydb.backend.dm.pageCache;

import sun.security.util.Length;
import top.lxl.mydb.backend.common.AbstractCache;
import top.lxl.mydb.backend.dm.page.Page;
import top.lxl.mydb.backend.dm.page.PageImpl;
import top.lxl.mydb.backend.utils.Panic;
import top.lxl.mydb.common.Error;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lxl
 * @version 1.0
 */
public class PageCacheImpl extends AbstractCache<Page> implements PageCache {

    private static final int MEM_MIN_LIM = 10;
    public static final String DB_SUFFIX = ".db";

    private RandomAccessFile file;
    private FileChannel fc;
    private Lock fileLock;

    private AtomicInteger pageNumbers;


    PageCacheImpl(RandomAccessFile file, FileChannel fileChannel, int maxResource) {
        super(maxResource);
        if(maxResource < MEM_MIN_LIM) {
            Panic.panic(Error.MemTooSmallException);
        }

        long length = 0;
        try {
            length = file.length();
        } catch (IOException e) {
            Panic.panic(e);
        }
        this.file = file;
        this.fc = fileChannel;
        fileLock = new ReentrantLock();
        this.pageNumbers = new AtomicInteger((int)length / PageCache.PAGE_SIZE);
    }

    /**
     * 根据pageNumber从数据库文件中读取数据页数据，并包装成Page
     */
    @Override
    protected Page getForCache(long key) throws Exception {
        int pgNo = (int)key;
        long offset = pageOffset(pgNo);

        ByteBuffer buf = ByteBuffer.allocate(PAGE_SIZE);
        try {
            fc.position(offset);
            fc.read(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }
        return new PageImpl(pgNo, buf.array(), this);
    }

    @Override
    protected void releaseForCache(Page pg) {
        if(pg.isDirty()) {
            flush(pg);
            pg.setDirty(false);
        }
    }

    @Override
    public int newPage(byte[] initData) {
        int pgNo = pageNumbers.incrementAndGet();
        Page pg = new PageImpl(pgNo, initData, null);
        flush(pg);
        return pgNo;
    }

    @Override
    public Page getPage(int pgNo) throws Exception {
        return get(pgNo);
    }

    @Override
    public void release(Page page) {
        release(page.getPageNumber());
    }

    @Override
    public void truncateByPgNo(int maxPgNo) {
        long size = pageOffset(maxPgNo + 1);
        try {
            file.setLength(size);
        } catch (IOException e) {
            Panic.panic(e);
        }
        pageNumbers.set(maxPgNo);
    }

    @Override
    public int getPageNumber() {
        return pageNumbers.intValue();
    }

    @Override
    public void flushPage(Page pg) {
        flush(pg);
    }

    @Override
    public void close() {
        super.close();
        try {
            fc.close();
            file.close();
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

    private static long pageOffset(int pgNo) {
        return (long)(pgNo - 1) * PAGE_SIZE;
    }

    private void flush(Page pg) {
        int pgNo = pg.getPageNumber();
        long offset = pageOffset(pgNo);

        try {
            ByteBuffer buf = ByteBuffer.wrap(pg.getData());
            fc.position(offset);
            fc.write(buf);
            fc.force(false);
        } catch (IOException e) {
            Panic.panic(e);
        }
    }

}
