package top.lxl.mydb.backend.dm.dataItem;

import com.google.common.primitives.Bytes;
import top.lxl.mydb.backend.common.SubArray;
import top.lxl.mydb.backend.dm.DataManagerImpl;
import top.lxl.mydb.backend.dm.page.Page;
import top.lxl.mydb.backend.utils.Parser;
import top.lxl.mydb.backend.utils.Types;

import java.util.Arrays;

/**
 * @author lxl
 * @version 1.0
 */
public interface DataItem {
    SubArray data();
    SubArray getRaw();

    void before();
    void unBefore();
    void after(long xid);
    void release();

    void lock();
    void unlock();
    void rLock();
    void rUnLock();

    Page page();
    long getUid();
    byte[] getOldRaw();

    static byte[] wrapDataItemRaw(byte[] raw) {
        byte[] valid = new byte[1];
        byte[] size = Parser.short2Byte((short)raw.length);
        return Bytes.concat(valid, size, raw);
    }

    static DataItem parseDataItem(Page pg, short offset, DataManagerImpl dm) {
        byte[] raw = pg.getData();
        short size = Parser.parseShort(Arrays.copyOfRange(raw, offset + DataItemImpl.OF_SIZE, offset + DataItemImpl.OF_DATA));
        short length = (short)(size + DataItemImpl.OF_DATA);
        long uid = Types.addressToUID(pg.getPageNumber(), offset);
        return new DataItemImpl(new SubArray(raw, offset, offset + length), new byte[length], pg, uid, dm);
    }

    static void setDataItemRawInvalid(byte[] raw) {
        raw[DataItemImpl.OF_VALID] = (byte)1;
    }
}
