package top.lxl.mydb.backend.tm;

import org.junit.Test;

import java.io.File;
import java.security.SecureRandom;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lxl
 * @version 1.0
 */
public class TransactionManagerTest {

    static Random random = new SecureRandom();
    private String path = "transManager_test";
    private int workerCnts = 50;
    private int works = 3000;
    private TransactionManager transManager;
    private Map<Long, Byte> transMap;
    private int transCnt = 0;
    private CountDownLatch cdl;
    private Lock lock = new ReentrantLock();


    @Test
    public void testMultiThread() {
        transManager = TransactionManager.crete(path);
        transMap = new HashMap<>();
        cdl = new CountDownLatch(workerCnts);
        for(int i = 0; i < workerCnts; i++) {
            Runnable r = this::worker;
            new Thread(r).start();
        }

        try {
            cdl.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        transManager.close();
        assert new File(path + TransactionManagerImpl.XID_SUFFIX).delete();
    }

    private void worker() {
        boolean inTrans = false;
        long transXID = 0;
        for(int i = 0; i < works; i++) {
            int op = Math.abs(random.nextInt(6));
            if(op <= 1) {
                if(inTrans == false) { // 创建新事务
                    lock.lock();
                    long xid = transManager.begin();
                    transMap.put(xid, (byte)0);
                    transCnt++;
                    lock.unlock();
                    transXID = xid;
                    inTrans = true;
                } else { // 更新事务状态
                    int status = Math.abs(random.nextInt(Integer.MAX_VALUE)) % 2 + 1;
                    lock.lock();
                    switch (status) {
                        case 1:
                            transManager.commit(transXID);
                            break;
                        case 2:
                            transManager.abort(transXID);
                            break;
                    }
                    transMap.put(transXID, (byte)status);
                    lock.unlock();
                    inTrans = false;
                }
            } else { // 检查事务状态是否正确
                if(transCnt > 0) {
                    lock.lock();
                    long xid = ((Math.abs(random.nextInt(Integer.MAX_VALUE)) % transCnt) + 1);
                    byte status = transMap.get(xid);
                    boolean ok = false;
                    switch (status) {
                        case 0:
                            ok = transManager.isActive(xid);
                            break;
                        case 1:
                            ok = transManager.isCommitted(xid);
                            break;
                        case 2:
                            ok = transManager.isAborted(xid);
                    }
                    lock.unlock();
                    assert ok;
                }
            }
        }
        cdl.countDown();;
    }
}
