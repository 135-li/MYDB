package top.lxl.mydb.backend.utils;

import java.util.Random;

/**
 * @author lxl
 * @version 1.0
 */
public class RandomUtil {
    public static byte[] randomBytes(int length) {
        Random r = new Random();
        byte[] buf = new byte[length];
        r.nextBytes(buf);
        return buf;
    }
}
