package top.lxl.mydb.backend.dm.page;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lxl
 * @version 1.0
 */
public class MockPage implements Page{
    private int pgNo;
    private byte[] data;
    private Lock lock;
    private boolean dirty;

    public MockPage(int pgNo, byte[] data) {
        this.pgNo = pgNo;
        this.data = data;
        lock = new ReentrantLock();
    }

    @Override
    public void lock() {
        lock.lock();
    }

    @Override
    public void unlock() {
        lock.unlock();
    }

    @Override
    public void release() {

    }

    @Override
    public void setDirty(boolean dirty) {
        this.dirty = dirty;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public int getPageNumber() {
        return pgNo;
    }

    @Override
    public byte[] getData() {
        return data;
    }
}
