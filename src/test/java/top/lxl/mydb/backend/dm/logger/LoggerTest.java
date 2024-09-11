package top.lxl.mydb.backend.dm.logger;

import org.junit.Test;
import top.lxl.mydb.backend.utils.RandomUtil;

import java.io.File;
import java.security.SecureRandom;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

/**
 * @author lxl
 * @version 1.0
 */
public class LoggerTest {
    private static final String path = "logger_test";
    private Random random = new SecureRandom();

    @Test
    public void testLogger() {
        Logger lg = Logger.create(path);
        lg.log("aaa".getBytes());
        lg.log("bbb".getBytes());
        lg.log("ccc".getBytes());
        lg.log("ddd".getBytes());
        lg.log("eee".getBytes());
        lg.close();

        lg = Logger.open(path);
        lg.rewind();

        byte[] log = lg.next();
        assert log != null;
        assert "aaa".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "bbb".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ccc".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "ddd".equals(new String(log));

        log = lg.next();
        assert log != null;
        assert "eee".equals(new String(log));

        log = lg.next();
        assert log == null;

        lg.close();

        assert new File(path + LoggerImpl.LOG_SUFFIX).delete();
    }

    private Logger lg;
    private CountDownLatch cdl1;

    @Test
    public void testMultiThreadLogger() throws InterruptedException {
        lg = Logger.create(path);
        cdl1 = new CountDownLatch(50);
        for(int i = 1; i <= 50; i++) {
            Runnable r = this::worker1;
            new Thread(r).start();
        }
        cdl1.await();

        lg.close();
        lg = Logger.open(path);
        lg.close();
        assert new File(path + LoggerImpl.LOG_SUFFIX).delete();
    }

    public void worker1() {
        for(int i = 1; i <= 1000; i++) {
            lg.log(RandomUtil.randomBytes(Math.abs(random.nextInt(50))));
        }
        cdl1.countDown();
    }
}
