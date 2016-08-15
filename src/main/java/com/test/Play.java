package com.test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by iceke on 16/7/26.
 */
public class Play {
    public static void main(String args[]){

        //System.out.println(Integer.MAX_VALUE+3);
        Map<Long,Long> test = new HashMap<>(10000000);

        long start = System.currentTimeMillis();
        for(long i = 0;i<10000000;i++){
            test.put(i,i);
            test.get(i-1);
        }
        long end = System.currentTimeMillis();

        System.out.println("time is "+(end-start)/1000);
    }
}
