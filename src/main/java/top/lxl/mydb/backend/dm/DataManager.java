package top.lxl.mydb.backend.dm;

import sun.rmi.runtime.Log;
import top.lxl.mydb.backend.dm.dataItem.DataItem;
import top.lxl.mydb.backend.dm.logger.Logger;
import top.lxl.mydb.backend.dm.page.PageOne;
import top.lxl.mydb.backend.dm.pageCache.PageCache;
import top.lxl.mydb.backend.tm.TransactionManager;

/**
 * @author lxl
 * @version 1.0
 */
public interface DataManager {

    DataItem read(long uid) throws Exception;

    long insert(long xid, byte[] data) throws Exception;

    void close();


    static DataManager create(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.create(path, mem);
        Logger lg = Logger.create(path);

        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        dm.initPageOne();
        return dm;

    }

    static DataManager open(String path, long mem, TransactionManager tm) {
        PageCache pc = PageCache.open(path, mem);
        Logger lg = Logger.open(path);
        DataManagerImpl dm = new DataManagerImpl(pc, lg, tm);
        if(!dm.loadCheckPageOne()) {
            Recover.recover(tm, lg, pc);
        }
        dm.fillPageIndex();
        PageOne.setVcOpen(dm.pageOne);
        dm.pc.flushPage(dm.pageOne);

        return dm;
    }
}
