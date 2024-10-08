package top.lxl.mydb.backend.tm;

import top.lxl.mydb.backend.utils.Panic;
import top.lxl.mydb.common.Error;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/**
 * @author lxl
 * @version 1.0
 */
public interface TransactionManager {
    long begin();                                                        // 开启一个新事务

    void commit(long xid);                                               // 提交一个新事务

    void abort(long xid);                                                // 取消一个新事务

    boolean isActive(long xid);                                          // 查询一个事务的状态是否正在进行

    boolean isCommitted(long xid);                                       // 查询一个事务的状态是否已提交

    boolean isAborted(long xid);                                          // 查询一个事务的状态是否已取消

    void close();                                                         // 关闭事务管理器

    public static TransactionManagerImpl crete(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        try {
            if(!f.createNewFile()) {
                Panic.panic(Error.FileExistsException);
            }
        } catch (IOException e) {
            Panic.panic(e);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        // 新文件，写空XID文件头
        ByteBuffer buf = ByteBuffer.wrap(new byte[TransactionManagerImpl.LEN_XID_HEADER_LENGTH]);
        try {
            fc.position(0);
            fc.write(buf);
        } catch (IOException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }         // 创建一个新的事务管理文件

    public static TransactionManagerImpl open(String path) {
        File f = new File(path + TransactionManagerImpl.XID_SUFFIX);
        if(!f.exists()) {
            Panic.panic(Error.FileNotExistsException);
        }
        if(!f.canRead() || !f.canWrite()) {
            Panic.panic(Error.FileCannotRWException);
        }

        FileChannel fc = null;
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(f, "rw");
            fc = raf.getChannel();
        } catch (FileNotFoundException e) {
            Panic.panic(e);
        }

        return new TransactionManagerImpl(raf, fc);
    }           // 打开一个事务管理文件
}
