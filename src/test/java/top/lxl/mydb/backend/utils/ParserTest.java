package top.lxl.mydb.backend.utils;

import org.junit.Test;

/**
 * @author lxl
 * @version 1.0
 */
public class ParserTest {

    @Test
    public void testParseLong() {
        long start = (long)Integer.MAX_VALUE * 2;
        long end = (long)Integer.MAX_VALUE * 2 + 1_000_000_0;
        for(long i = start; i <= end; i++) {
            byte[] bytes = Parser.long2Byte(i);
            long tmp = Parser.parseLong(bytes);
            assert  tmp == i;
        }
    }
}
