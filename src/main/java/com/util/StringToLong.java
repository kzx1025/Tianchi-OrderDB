package com.util;

import java.util.HashSet;
import java.util.Set;

/**
 * Created by iceke on 16/7/20.
 */
public class StringToLong {
    public static long hash(String s){
        long seed = 131; // 31 131 1313 13131 131313 etc..  BKDRHash
        long hash=0;
        for (int i = 0; i< s.length(); i++){
            hash = (hash * seed) + s.charAt(i);
        }
        return hash;
    }

    public static void main(String args[]) {
        long start = System.currentTimeMillis();
        StringToLong.hash("wx-aaab-9a3ef6b228e1");
        long end = System.currentTimeMillis();
        System.out.println(end-start);
        Set<String> keySet = new HashSet<>();
        keySet.add("das");
        keySet.add("das");
        keySet.add("dd");

    }
}
