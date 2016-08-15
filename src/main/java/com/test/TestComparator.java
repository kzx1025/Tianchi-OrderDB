package com.test;

import com.alibaba.middleware.race.KeyComparatorTwo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by iceke on 16/7/28.
 */
public class TestComparator {
    public static void main(String args[]){
         List<String> orderList = new ArrayList<>();

        orderList.add("done:true\ta_o_29364:1\ta_o_29449:true\tgoodid:al-99b1-0595828f1d47\tamount:7\tbuyerid:ap-9b55-bcfbfb36551c\tcreatetime:4914556775\ta_o_31258:1468385536414325\ta_o_23354:1468387030028431\torderid:21152509063");
        orderList.add("a_o_31720:-32762\ta_o_29340:0.13\ta_o_31258:1468387098662135\tgoodid:al-99b1-0595828f1d47\tcreatetime:3199812615\torderid:10796860913\tamount:5\tdone:false\tbuyerid:tp-adb7-aa4f594da27e\ta_o_8667:1468384920345229");
        orderList.add("orderid:3387433703\tcreatetime:1963039375\tbuyerid:wx-bee7-a368dfb873ce\tgoodid:al-99b1-0595828f1d47\tremark:围栏手术台马克雷训令，时域耳目一新管仲科蒂。\tamount:2\tdone:false\ta_o_639:1468386322651033\ta_o_29340:0.6105");

        for(String o :orderList){
            System.out.println(o);
        }

        System.out.println();
        Collections.sort(orderList,new KeyComparatorTwo("orderid"));
        for(String order:orderList){
            System.out.println(order);
        }
    }
}
