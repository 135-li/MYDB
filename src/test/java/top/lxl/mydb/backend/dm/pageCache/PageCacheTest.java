package top.lxl.mydb.backend.dm.pageCache;

import org.junit.Test;
import top.lxl.mydb.backend.dm.page.Page;
import top.lxl.mydb.backend.utils.Panic;
import top.lxl.mydb.backend.utils.RandomUtil;

import java.io.File;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author lxl
 * @version 1.0
 */
public class PageCacheTest {
    private static final String path = "pageCache_test";
    private static Random random = new SecureRandom();

    @Test
    public void testPageCacheSingleThread() throws Exception {
        PageCache pc = PageCache.create(path, PageCache.PAGE_SIZE * 50);
        for(int i = 0; i < 100; i++) {
            byte[] tmp = new byte[PageCache.PAGE_SIZE];
            tmp[0] = (byte)i;
            int pgNo = pc.newPage(tmp);
            Page pg = pc.getPage(pgNo);
            pg.setDirty(true);
            pg.release();
        }
        assert pc.getPageNumber() == 100;
        pc.close();

        pc = PageCache.open(path, PageCache.PAGE_SIZE * 50);
        assert pc.getPageNumber() == 100;
        for(int i = 1; i <= 100; i++) {
            Page pg = pc.getPage(i);
            assert pg.getData()[0] == (byte)(i - 1);
            pg.release();
        }

        pc.close();

        assert new File(path + PageCacheImpl.DB_SUFFIX).delete();
    }


    private CountDownLatch cdl1;
    private PageCache pc1;
    @Test
    public void testPageCacheMultiThreadNewPage() throws Exception {
        cdl1 = new CountDownLatch(20);
        pc1 = PageCache.create(path, PageCache.PAGE_SIZE * 200);
        for(int i = 1; i <= 20; i++) {
            Runnable r = this::worker1;
            new Thread(r).start();
        }
        cdl1.await();
        pc1.close();
        pc1 = PageCache.open(path, PageCache.PAGE_SIZE * 200);
        assert pc1.getPageNumber() == 120;
        for(int i = 1; i <= 120; i++) {
            Page pg = pc1.getPage(i);
            assert pg.getData()[0] == (byte)i;
            int op = Math.abs(random.nextInt()) % 20;
            if(op <= 5) {
                pg.release();
            }
        }

        pc1.close();

        assert new File(path + PageCacheImpl.DB_SUFFIX).delete();
    }
    private void worker1()  {
        for(int i = 1; i <= 6; i++) {
            byte[] tmp = new byte[PageCache.PAGE_SIZE];
            int pgNo = pc1.newPage(tmp);
            Page pg = null;
            try {
                pg = pc1.getPage(pgNo);
            } catch (Exception e) {
                Panic.panic(e);
            }
            assert pg != null;
            tmp = pg.getData();
            tmp[0] = (byte)pgNo;
            pg.setDirty(true);
            int op = Math.abs(random.nextInt()) % 20;
            if(op <= 5) {
                pg.release();
            }
        }
        cdl1.countDown();;
    }


    private CountDownLatch cdl2;
    private PageCache pc2, mpc2;
    private Lock lock2;
    private AtomicInteger noPages2;
    @Test
    public void testPageCacheMultiThreadNewAndUpdate() throws Exception {
        cdl2 = new CountDownLatch(30);
        pc2 = PageCache.create(path, PageCache.PAGE_SIZE * 3000);
        mpc2 = new MockPageCache();
        lock2 = new ReentrantLock();
        noPages2 = new AtomicInteger(0);
        for(int i = 1; i <= 30; i++) {
            Runnable r = this::worker2;
            new Thread(r).start();
        }
        cdl2.await();
        pc2.close();
        assert new File(path + PageCacheImpl.DB_SUFFIX).delete();
    }
    private void worker2() {
        for(int i = 0; i < 1000; i++) {
            int op = Math.abs(random.nextInt()) % 20;
            if(op <= 1) { // new Page
                byte[] data = RandomUtil.randomBytes(PageCache.PAGE_SIZE);
                lock2.lock();
                int pgNo = pc2.newPage(data);
                int mpgNo = mpc2.newPage(data);
                assert pgNo == mpgNo;
                lock2.unlock();
                noPages2.incrementAndGet();
            } else if(op < 11) { // check
                int mod = noPages2.intValue();
                if(mod == 0) continue;
                int pgNo = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null, mpg = null;
                try {
                    pg = pc2.getPage(pgNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mpg = mpc2.getPage(pgNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                pg.lock();
                assert Arrays.equals(pg.getData(), mpg.getData());
                pg.unlock();
                pg.release();
            } else { // update
                int mod = noPages2.intValue();
                if(mod == 0) continue;
                int pgNo = Math.abs(random.nextInt()) % mod + 1;
                Page pg = null, mpg = null;
                try {
                    pg = pc2.getPage(pgNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                try {
                    mpg = mpc2.getPage(pgNo);
                } catch (Exception e) {
                    Panic.panic(e);
                }
                byte[] newData = RandomUtil.randomBytes(PageCache.PAGE_SIZE);

                pg.lock();
                pg.setDirty(true);
                for(int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    pg.getData()[j] = newData[j];
                }

                mpg.setDirty(true);
                for(int j = 0; j < PageCache.PAGE_SIZE; j++) {
                    mpg.getData()[j] = newData[j];
                }
                pg.unlock();
                pg.release();
            }
        }
        cdl2.countDown();
    }


}
