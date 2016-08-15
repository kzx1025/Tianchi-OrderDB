package com.alibaba.middleware.race;

import java.util.Comparator;

/**
 * 从小到大排序
 * Created by iceke on 16/7/19.
 */
public class KeyComparatorTwo implements Comparator<String> {
    private String key;

    public KeyComparatorTwo(String key) {
        this.key = key;


    }

    @Override
    public int compare(String o1, String o2) {
        int a1=o1.indexOf(key);
        int b1=o1.indexOf("\t",a1);
        int a2=o2.indexOf(key);
        int b2=o2.indexOf("\t",a2);
        if(Long.parseLong(o1.substring(a1+key.length()+1,(b1!=-1)?b1:o1.length()))>Long.parseLong(
                o2.substring(a2+key.length()+1,(b2!=-1)?b2:o2.length())))
            return 1;
        return -1;

        /**String[] pairs1 = o1.split("\t");
        String[] pairs2 = o2.split("\t");
        long value1 = 0L;
        long value2 = 0L;

        for(String pair1:pairs1){
            if(pair1.split(":")[0].equals(key)){
                value1 = Long.parseLong(pair1.split(":")[1]);
                break;
            }
        }

        for(String pair2:pairs2){
            if(pair2.split(":")[0].equals(key)){
                value2 = Long.parseLong(pair2.split(":")[1]);
                break;
            }
        }

        return value1>value2?1:-1;**/

    }
}
