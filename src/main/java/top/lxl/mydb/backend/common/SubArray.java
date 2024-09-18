package top.lxl.mydb.backend.common;

/**
 * @author lxl
 * @version 1.0
 * 默认，左开右闭
 *
 */
public class SubArray {
    public byte[] raw;
    public int start;
    public int end;

    public SubArray(byte[] raw, int start, int end) {
        this.raw = raw;
        this.start = start;
        this.end = end;
    }
}
