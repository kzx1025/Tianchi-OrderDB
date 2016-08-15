package com.alibaba.middleware.race;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * Created by iceke on 16/7/20.
 */
public class TestThread {

    private OrderSystem os = null;
    public TestThread(OrderSystem os){
        this.os =os;
    }

    public static void main(String args[]){
        List<String> orderFiles = new ArrayList<String>();
        List<String> buyerFiles = new ArrayList<String>();
        List<String> goodFiles = new ArrayList<String>();
        List<String> storeFolders = new ArrayList<String>();

        orderFiles.add("data/order.0.0");
        orderFiles.add("data/order.1.1");
        orderFiles.add("data/order.2.2");
        orderFiles.add("data/order.0.3");
        buyerFiles.add("data/buyer.0.0");
        buyerFiles.add("data/buyer.1.1");
        goodFiles.add("data/good.0.0");
        goodFiles.add("data/good.1.1");
        goodFiles.add("data/good.2.2");
        storeFolders.add("data/split1/");
        storeFolders.add("data/split2/");
        storeFolders.add("data/split3/");

        OrderSystem os = new OrderSystemImpl();
        try {
            os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);
        }catch (Exception e){
            e.printStackTrace();
        }

        TestThread testThread = new TestThread(os);


        long start = System.currentTimeMillis();

        for(int i =0;i<1000;i++) {
          //  testThread.speed();
        }

        testThread.come();

        long end = System.currentTimeMillis();

        System.out.println("time is "+(end-start));


    }
    public void speed(){
        long orderid = 627919211;
        // System.out.println("\n查询订单号为" + orderid + "的订单");
        os.queryOrder(orderid, null);

        // System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
        os.queryOrder(orderid, new ArrayList<String>());

        // System.out.println("\n查询订单号为" + orderid
        //        + "的订单的contactphone, buyerid, foo, done, price字段");
        List<String> queryingKeys = new ArrayList<String>();
        queryingKeys.add("contactphone");
        queryingKeys.add("buyerid");
        queryingKeys.add("foo");
        queryingKeys.add("done");
        queryingKeys.add("price");
        OrderSystem.Result result = os.queryOrder(orderid, queryingKeys);
        System.out.println(result);
        //  System.out.println("\n查询订单号不存在的订单");
        result = os.queryOrder(61236191, queryingKeys);
        if (result == null) {
            System.out.println(61236191 + " order not exist");
        }

        String buyerid = "wx-bb14-3901c80bf330";
        long startTime = 1469364306;
        long endTime = 1477483362;
        //  System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
        Iterator<OrderSystem.Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
        while (it.hasNext()) {
            it.next();
        }

        String goodid = "dd-ad2b-a46acaa527b1";
        String salerid = "ay-9239-9849ac6989df";
        // System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
        it = os.queryOrdersBySaler(salerid, goodid, new ArrayList<String>());
        while (it.hasNext()) {
            it.next();
        }

        goodid = "aye-82b6-dcc7479a8bf7";
        String attr = "a_o_4699";
        // System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        System.out.println(os.sumOrdersByGood(goodid, attr));

        attr = "a_o_28730";
        // System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        OrderSystem.KeyValue sum = os.sumOrdersByGood(goodid, attr);
        if (sum == null) {
            System.out.println("由于该字段是布尔类型，返回值是null");
        }

        attr = "a_o_3070";
        // System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        sum = os.sumOrdersByGood(goodid, attr);
        if (sum == null) {
            System.out.println("由于该字段不存在，返回值是null");
        }


    }

    public void come(){
        RequestThread thread = new RequestThread(os);
        RequestThread2  thread2 = new RequestThread2(os);
        List<Thread> threads = new ArrayList<>();

            Thread threada = new Thread(thread);
            Thread threadb = new Thread(thread2);
            threada.start();
            threadb.start();
            threads.add(threada);
            threads.add(threadb);

        try {

            for (Thread t : threads) {
                t.join();
            }
        }catch(Exception e){
            e.printStackTrace();
        }


    }



    private class RequestThread implements Runnable{
        private OrderSystem os = null;

        public RequestThread(OrderSystem orderSystem){
            this.os = orderSystem;
        }

        @Override
        public void run() {
            for(int i = 0;i<5000;i++) {

                long orderid = 627919211;
                // System.out.println("\n查询订单号为" + orderid + "的订单");
                os.queryOrder(orderid, null);

                // System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
                os.queryOrder(orderid, new ArrayList<String>());

                // System.out.println("\n查询订单号为" + orderid
                //        + "的订单的contactphone, buyerid, foo, done, price字段");
                List<String> queryingKeys = new ArrayList<String>();
                queryingKeys.add("contactphone");
                queryingKeys.add("buyerid");
                queryingKeys.add("foo");
                queryingKeys.add("done");
                queryingKeys.add("price");
                OrderSystem.Result result = os.queryOrder(orderid, queryingKeys);
                System.out.println(result);
                //  System.out.println("\n查询订单号不存在的订单");
                result = os.queryOrder(61236191, queryingKeys);
                if (result == null) {
                    System.out.println(61236191 + " order not exist");
                }

                String buyerid = "wx-bb14-3901c80bf330";
                long startTime = 1469364306;
                long endTime = 1477483362;
                //  System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
                Iterator<OrderSystem.Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
                while (it.hasNext()) {
                    it.next();
                }

                String goodid = "dd-ad2b-a46acaa527b1";
                String salerid = "ay-9239-9849ac6989df";
                // System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
                it = os.queryOrdersBySaler(salerid, goodid, new ArrayList<String>());
                while (it.hasNext()) {
                    it.next();
                }

                goodid = "al-ac5b-f20bd9da1ef3";
                String attr = "a_b_11255";
                // System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
                System.out.println(os.sumOrdersByGood(goodid, attr));

                attr = "a_o_28730";
                // System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
                OrderSystem.KeyValue sum = os.sumOrdersByGood(goodid, attr);
                if (sum == null) {
                    System.out.println("由于该字段是布尔类型，返回值是null");
                }

                attr = "a_o_3070";
                // System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
                sum = os.sumOrdersByGood(goodid, attr);
                if (sum == null) {
                    System.out.println("由于该字段不存在，返回值是null");
                }
            }

        }
    }



    private class RequestThread2 implements Runnable{
        private OrderSystem os = null;

        public RequestThread2(OrderSystem orderSystem){
            this.os = orderSystem;
        }

        @Override
        public void run() {
            for (int i = 0; i < 5000; i++) {

                long orderid = 627919211;
                // System.out.println("\n查询订单号为" + orderid + "的订单");
                //System.out.println(os.queryOrder(orderid, null));

                //System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
                //System.out.println(os.queryOrder(orderid, new ArrayList<String>()));

                // System.out.println("\n查询订单号为" + orderid
                //       + "的订单的contactphone, buyerid, foo, done, price字段");
                List<String> queryingKeys = new ArrayList<String>();
                queryingKeys.add("contactphone");
                queryingKeys.add("buyerid");
                queryingKeys.add("foo");
                queryingKeys.add("done");
                queryingKeys.add("price");
                OrderSystem.Result result = os.queryOrder(orderid, queryingKeys);
                System.out.println(result);
                //  System.out.println("\n查询订单号不存在的订单");
                result = os.queryOrder(61236192, queryingKeys);
                if (result == null) {
                    System.out.println(61236192 + " order not exist");
                }

                String buyerid = "wx-bb14-3901c80bf330";
                long startTime = 1469364306;
                long endTime = 1477483362;
                //    System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
                Iterator<OrderSystem.Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
                while (it.hasNext()) {
                    it.next();
                }

                String goodid = "dd-ad2b-a46acaa527b1";
                String salerid = "ay-9239-9849ac6989df";
                //    System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
                it = os.queryOrdersBySaler(salerid, goodid, new ArrayList<String>());
                while (it.hasNext()) {
                    it.next();
                }

                goodid = "gd-b3f0-da9c17eb74c2";
                String attr = "a_b_10930";
                //   System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
                System.out.println(os.sumOrdersByGood(goodid, attr));

                attr = "a_o_28730";
                //    System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
                OrderSystem.KeyValue sum = os.sumOrdersByGood(goodid, attr);
                if (sum == null) {
                    System.out.println("由于该字段是布尔类型，返回值是null");
                }

                attr = "a_o_3070";
                //  System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
                sum = os.sumOrdersByGood(goodid, attr);
                if (sum == null) {
                    System.out.println("由于该字段不存在，返回值是null");
                }

            }
        }
    }
}
