package top.lxl.mydb.backend.dm.page;

import org.junit.Test;
import top.lxl.mydb.backend.dm.pageCache.PageCache;
import top.lxl.mydb.backend.dm.pageCache.PageCacheImpl;
import top.lxl.mydb.backend.utils.Panic;

import java.io.File;
import java.security.SecureRandom;
import java.util.Random;

/**
 * @author lxl
 * @version 1.0
 */
public class PageXTest {
    private String path = "pageX_test";
    private PageCache pc;
    private Random random;

    @Test
    public void testInsert() {
        pc = PageCache.create(path, PageCache.PAGE_SIZE * 300);
        random = new SecureRandom();
        int pgNo = pc.newPage(PageX.initRaw());
        Page pg = null;
        try {
            pg = pc.getPage(pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert pg != null;
        int fso = 2;
        int freeSpace = PageX.MAX_FREE_SPACE;
        for(int i = 1; i <= 100; i++) {
            int len = random.nextInt(82);
            PageX.insert(pg, new byte[len]);
            fso += len;
            freeSpace -= len;
            assert fso == PageX.getFSO(pg);
            assert freeSpace == PageX.getFreeSpace(pg);
        }

        pc.close();
        assert new File(path + PageCacheImpl.DB_SUFFIX).delete();
    }

    @Test
    public void testRecoverInsert() {
        pc = PageCache.create(path, PageCache.PAGE_SIZE * 300);
        random = new SecureRandom();
        int pgNo = pc.newPage(PageX.initRaw());
        Page pg = null;
        try {
            pg = pc.getPage(pgNo);
        } catch (Exception e) {
            Panic.panic(e);
        }
        assert pg != null;

        int start = 100;
        int oldFso = 2;
        for(int i = 1; i <= 1000000; i++) {
            int len = random.nextInt(8000);
            PageX.recoverInsert(pg, new byte[len], (byte)start);
            if(start + len > oldFso) {
                oldFso = start + len;
                assert oldFso == PageX.getFSO(pg);
            }
        }

        pc.close();
        assert new File(path + PageCacheImpl.DB_SUFFIX).delete();
    }
}
