package top.lxl.mydb.backend.dm.pageIndex;

/**
 * @author lxl
 * @version 1.0
 */
public class  PageInfo {
    public int pgNo;
    public int freeSpace;


    public PageInfo(int pgNo, int freeSpace) {
        this.pgNo = pgNo;
        this.freeSpace = freeSpace;
    }
}
