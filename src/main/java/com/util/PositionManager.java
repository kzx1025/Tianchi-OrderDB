package com.util;

/**
 * Created by iceke on 16/7/22.
 */
public class PositionManager {

    public static long makeRecord(long offset, int len) {
        return (offset & 0x000000FFFFFFFFFFL) | ((long)(len & 0x00FFFFFF) << 40);
    }
    public static long getOffset(long record) {
        return (record & 0x000000FFFFFFFFFFL);
    }
    public static int getLength(long record) {
        return (int)((record & 0xFFFFFF0000000000L) >>> 40);
    }

    public static void main(String []args){

        long offset = 123456;
        int len = 333;
        long record = makeRecord(offset, len);

        System.out.println(offset);
        System.out.println(len);
        System.out.println(record);
        System.out.println(getOffset(record));
        System.out.println(getLength(record));

    }

}
