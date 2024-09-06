package top.lxl.mydb.backend.dm.page;

/**
 * @author lxl
 * @version 1.0
 */
public interface Page {
    void lock();

    void unlock();

    void release();

    void setDirty(boolean dirty);

    boolean isDirty();

    int getPageNumber();

    byte[] getData();

}
