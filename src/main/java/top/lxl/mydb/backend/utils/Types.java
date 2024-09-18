package top.lxl.mydb.backend.utils;

/**
 * @author lxl
 * @version 1.0
 */
public class Types {
    public static long addressToUID(int pgNo, short offset) {
        long u0 = pgNo;
        long u1 = offset;
        return u0 << 32 | u1;
    }
}
