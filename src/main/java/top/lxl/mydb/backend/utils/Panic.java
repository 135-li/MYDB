package top.lxl.mydb.backend.utils;

/**
 * @author lxl
 * @version 1.0
 */
public class Panic {
    public static void panic(Exception err) {
        err.printStackTrace();
        System.exit(1);
    }
}
