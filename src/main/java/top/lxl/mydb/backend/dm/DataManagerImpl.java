package top.lxl.mydb.backend.dm;

import top.lxl.mydb.backend.common.AbstractCache;
import top.lxl.mydb.backend.dm.dataItem.DataItem;
import top.lxl.mydb.backend.dm.dataItem.DataItemImpl;
import top.lxl.mydb.backend.dm.logger.Logger;
import top.lxl.mydb.backend.dm.page.Page;
import top.lxl.mydb.backend.dm.page.PageOne;
import top.lxl.mydb.backend.dm.page.PageX;
import top.lxl.mydb.backend.dm.pageCache.PageCache;
import top.lxl.mydb.backend.dm.pageIndex.PageIndex;
import top.lxl.mydb.backend.dm.pageIndex.PageInfo;
import top.lxl.mydb.backend.tm.TransactionManager;
import top.lxl.mydb.backend.tm.TransactionManagerImpl;
import top.lxl.mydb.backend.utils.Panic;
import top.lxl.mydb.backend.utils.Types;
import top.lxl.mydb.common.Error;

/**
 * @author lxl
 * @version 1.0
 */
public class DataManagerImpl extends AbstractCache<DataItem> implements DataManager{

    PageCache pc;
    Logger logger;
    TransactionManager tm;
    PageIndex pIndex;
    Page pageOne;

    public DataManagerImpl(PageCache pc, Logger logger, TransactionManager tm) {
        super(0);
        this.pc = pc;
        this.logger = logger;
        this.tm = tm;
        this.pIndex = new PageIndex();
    }

    @Override
    protected DataItem getForCache(long uid) throws Exception {
        short offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        int pgNo = (int)(uid & ((1L << 32) - 1));
        Page pg = pc.getPage(pgNo);
        return DataItem.parseDataItem(pg, offset, this);
    }

    @Override
    protected void releaseForCache(DataItem di) {
        di.page().release();
    }

    @Override
    public DataItem read(long uid) throws Exception {
        DataItemImpl di = (DataItemImpl)super.get(uid);
        if(!di.isValid()) {
            di.release();
            return null;
        }
        return di;
    }

    @Override
    public long insert(long xid, byte[] data) throws Exception {
        byte[] raw = DataItem.wrapDataItemRaw(data);
        if(raw.length > PageX.MAX_FREE_SPACE) {
            throw Error.DataTooLargeException;
        }

        // 获取空余页面
        PageInfo pi = null;
        for(int i = 0; i < 5; i++) {
            pi = pIndex.select(raw.length);
            if(pi != null) {
                break;
            } else {
                int newPgNo = pc.newPage(PageX.initRaw());
                pIndex.add(newPgNo, PageX.MAX_FREE_SPACE);
            }
        }
        if(pi == null) {
            throw Error.DatabaseBusyException;
        }

        Page pg = null;
        int freeSpace = 0;
        try {
            pg = pc.getPage(pi.pgNo);
            byte[] log = Recover.insertLog(xid, pg, raw);
            logger.log(log);
            short offset = PageX.insert(pg, raw);
            pg.release();
            return Types.addressToUID(pi.pgNo, offset);
        } finally {
            if(pg != null) {
                pIndex.add(pi.pgNo, PageX.getFreeSpace(pg));
            } else {
                pIndex.add(pi.pgNo, freeSpace);
            }
        }
    }

    @Override
    public void close() {
        logger.close();

        super.close();   // 将当前dataItem引用的page，全部释放
        PageOne.setVcClose(pageOne);
        pageOne.release();
        pc.close();
    }

    // 打开已有的数据库文件时，对数据文件进行校验
    boolean loadCheckPageOne() {
        try {
            pageOne = pc.getPage(1);
        } catch (Exception e) {
            Panic.panic(e);
        }
        return PageOne.checkVc(pageOne);
    }

    // 初始化第一页
    void initPageOne() {
        int pgNo = pc.newPage(PageOne.InitRaw());
        assert  pgNo == 1;
        try {
            pageOne = pc.getPage(pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        pc.flushPage(pageOne);
    }

    // 初始化页面索引
    void fillPageIndex() {
        int pageNumber = pc.getPageNumber();
        for(int i = 2; i <= pageNumber; i++) {
            Page pg = null;
            try {
                pg = pc.getPage(i);
            } catch (Exception e) {
                Panic.panic(e);
            }
            pIndex.add(pg.getPageNumber(), PageX.getFreeSpace(pg));
            pg.release();
        }
    }

    // 为xid生成update日志，并记录到日志
    public void logDataItem(long xid, DataItem di) {
        byte[] log = Recover.updateLog(xid, di);
        logger.log(log);
    }

    // 释放当前DataItem
    public void releaseDataItem(DataItem di) {
        super.release(di.getUid());
    }

}
