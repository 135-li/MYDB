package top.lxl.mydb.backend.dm;

import com.google.common.primitives.Bytes;
import top.lxl.mydb.backend.common.SubArray;
import top.lxl.mydb.backend.dm.dataItem.DataItem;
import top.lxl.mydb.backend.dm.logger.Logger;
import top.lxl.mydb.backend.dm.page.Page;
import top.lxl.mydb.backend.dm.page.PageX;
import top.lxl.mydb.backend.dm.pageCache.PageCache;
import top.lxl.mydb.backend.tm.TransactionManager;
import top.lxl.mydb.backend.utils.Panic;
import top.lxl.mydb.backend.utils.Parser;

import java.util.*;
import java.util.Map.Entry;

/**
 * @author lxl
 * @version 1.0
 */
public class Recover {
    private static final byte LOG_TYPE_INSERT = 0;
    private static final byte LOG_TYPE_UPDATE = 1;


    private static final int REDO = 0;
    private static final int UNDO = 1;

    static class UpdateLogInfo {
        long xid;
        int pgNo;
        short offset;
        byte[] oldRaw;
        byte[] newRaw;
    }

    static class InsertLogInfo {
        long xid;
        int pgNo;
        short offset;
        byte[] raw;
    }

    // TODO 这里有个疑问，为什么要将数据页面截断到最大的日志页面？
    public static void recover(TransactionManager tm, Logger lg, PageCache pc) {
        System.out.println("Recovering...");

        lg.rewind();
        int maxPgNo = 0;
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            int pgNo;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                pgNo = li.pgNo;
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                pgNo = xi.pgNo;
            }
            if(pgNo > maxPgNo) {
                maxPgNo = pgNo;
            }
        }
        if(maxPgNo == 0) {
            maxPgNo = 1;
        }
        pc.truncateByPgNo(maxPgNo);
        System.out.println("Truncate to " + maxPgNo + " pages.");

        redoTransactions(tm, lg, pc);
        System.out.println("Redo Transactions Over.");

        undoTransactions(tm, lg, pc);
        System.out.println("Undo Transactions Over.");

        System.out.println("Recovery over");
    }

    private static void redoTransactions(TransactionManager tm, Logger lg, PageCache pc) {
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
                InsertLogInfo li = parseInsertLog(log);
                long xid = li.xid;
                if(!tm.isActive(xid)) {
                    doInsertLog(pc, log, REDO);
                }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(!tm.isActive(xid)) {
                    doUpdateLog(pc, log, REDO);
                }
            }
        }
    }

    private static void undoTransactions(TransactionManager tm ,Logger lg, PageCache pc) {
        Map<Long, List<byte[]>> logCache = new HashMap<>();
        lg.rewind();
        while(true) {
            byte[] log = lg.next();
            if(log == null) break;
            if(isInsertLog(log)) {
               InsertLogInfo li = parseInsertLog(log);
               long xid = li.xid;
               if(tm.isActive(xid)) {
                   if(!logCache.containsKey(xid)) {
                       logCache.put(xid, new ArrayList<>());
                   }
                   logCache.get(xid).add(log);
               }
            } else {
                UpdateLogInfo xi = parseUpdateLog(log);
                long xid = xi.xid;
                if(tm.isActive(xid)) {
                    if(!logCache.containsKey(xid)) {
                        logCache.put(xid, new ArrayList<>());
                    }
                    logCache.get(xid).add(log);
                }
            }
        }

        for(Entry<Long, List<byte[]>> entry : logCache.entrySet()) {
            List<byte[]> logs = entry.getValue();
            for(int i = logs.size() - 1; i >= 0; i--) {
                byte[] log = logs.get(i);
                if(isInsertLog(log)) {
                    doInsertLog(pc, log, UNDO);
                } else {
                    doUpdateLog(pc, log, UNDO);
                }
            }
            tm.abort(entry.getKey());
        }
    }

    private static boolean isInsertLog(byte[] log) {
        return log[0] == LOG_TYPE_INSERT;
    }

    private static final int OF_TYPE = 0;
    private static final int OF_XID = OF_TYPE + 1;

    // [LogType][XID][UID][OldRaw][NewRaw]
    private static final int OF_UPDATE_UID = OF_XID + 8;
    private static final int OF_UPDATE_RAW = OF_UPDATE_UID + 8;

    // 生成一条updateLog
    public static byte[] updateLog(long xid, DataItem di) {
        byte[] logType = {LOG_TYPE_UPDATE};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] uidRaw = Parser.long2Byte(di.getUid());
        byte[] oldRaw = di.getOldRaw();
        SubArray raw = di.getRaw();
        byte[] newRaw = Arrays.copyOfRange(raw.raw, raw.start, raw.end);
        return Bytes.concat(logType, xidRaw, uidRaw, oldRaw, newRaw);
    }

    // 解析一条updateLog
    private static UpdateLogInfo parseUpdateLog(byte[] log) {
        UpdateLogInfo li = new UpdateLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_UPDATE_UID));
        long uid = Parser.parseLong(Arrays.copyOfRange(log, OF_UPDATE_UID, OF_UPDATE_RAW));
        li.offset = (short)(uid & ((1L << 16) - 1));
        uid >>>= 32;
        li.pgNo = (int)(uid & ((1L << 32) - 1));
        int length = (log.length - OF_UPDATE_RAW) / 2;
        li.oldRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW, OF_UPDATE_RAW + length);
        li.newRaw = Arrays.copyOfRange(log, OF_UPDATE_RAW + length, OF_UPDATE_RAW + length * 2);
        return li;
    }

    // 执行
    private static void doUpdateLog(PageCache pc, byte[] log, int flag) {
        int pgNo;
        short offset;
        byte[] raw;
        if(flag == REDO) {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgNo = xi.pgNo;
            offset = xi.offset;
            raw = xi.newRaw;
        } else {
            UpdateLogInfo xi = parseUpdateLog(log);
            pgNo = xi.pgNo;
            offset = xi.offset;
            raw = xi.oldRaw;
        }

        Page pg = null;
        try {
            pg = pc.getPage(pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            PageX.recoverUpdate(pg, raw, offset);
        } finally {
            pg.release();
        }
    }

    // [LogType][XID][PgNo][Offset][Raw] insert
    private static final int OF_INSERT_PGNO = OF_XID + 8;
    private static final int OF_INSERT_OFFSET = OF_INSERT_PGNO + 4;
    private static final int OF_INSERT_RAW = OF_INSERT_OFFSET + 2;

    // 生成一条insertLog
    public static byte[] insertLog(long xid, Page pg, byte[] raw) {
        byte[] logTypeRaw = {LOG_TYPE_INSERT};
        byte[] xidRaw = Parser.long2Byte(xid);
        byte[] pgNoRaw = Parser.int2Byte(pg.getPageNumber());
        byte[] offsetRaw = Parser.short2Byte(PageX.getFSO(pg));
        return Bytes.concat(logTypeRaw, xidRaw, pgNoRaw, offsetRaw, raw);
    }

    // 解析一条insertLog
    private static InsertLogInfo parseInsertLog(byte[] log) {
        InsertLogInfo li = new InsertLogInfo();
        li.xid = Parser.parseLong(Arrays.copyOfRange(log, OF_XID, OF_INSERT_PGNO));
        li.pgNo = Parser.parseInt(Arrays.copyOfRange(log, OF_INSERT_PGNO, OF_INSERT_OFFSET));
        li.offset = Parser.parseShort(Arrays.copyOfRange(log, OF_INSERT_OFFSET, OF_INSERT_RAW));
        li.raw = Arrays.copyOfRange(log, OF_INSERT_RAW, log.length);
        return li;
    }

    // 执行
    private static void doInsertLog(PageCache pc, byte[] log, int flag) {
        InsertLogInfo li = parseInsertLog(log);
        Page pg = null;
        try {
            pg = pc.getPage(li.pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        try {
            if(flag == UNDO) {
                DataItem.setDataItemRawInvalid(li.raw);
            }
            PageX.recoverInsert(pg, li.raw, li.offset);
        } finally {
            pg.release();
        }
    }
}
