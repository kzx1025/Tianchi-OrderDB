package com.util;

/**
 * Created by iceke on 16/7/29.
 */
public class LongBytes {
    public  static long bytes2Long(byte[] bb){
        return ((((long) bb[ 0] & 0xff) << 56)
                | (((long) bb[ 1] & 0xff) << 48)
                | (((long) bb[ 2] & 0xff) << 40)
                | (((long) bb[ 3] & 0xff) << 32)
                | (((long) bb[ 4] & 0xff) << 24)
                | (((long) bb[ 5] & 0xff) << 16)
                | (((long) bb[ 6] & 0xff) << 8)
                | (((long) bb[ 7] & 0xff) << 0));
    }


    public static byte[] long2Bytes(long num){
        byte[] bytes=new byte[8];
        for (int ix=0;ix<8;++ix){
            int offset=64-(ix+1)*8;
            bytes[ix]=(byte)((num>>offset)& 0xff);
        }
        return bytes;
    }
}