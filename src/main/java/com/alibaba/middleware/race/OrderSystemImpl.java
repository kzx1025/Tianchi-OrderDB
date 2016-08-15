package com.alibaba.middleware.race;

import com.db.table.*;
import com.util.CommonValue;
import com.util.FileSplit;
import com.util.LRULinkedHashMap;
import com.util.StringToLong;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * 订单系统的demo实现，订单数据全部存放在内存中，用简单的方式实现数据存储和查询功能
 *
 * @author wangxiang@alibaba-inc.com
 */
public class OrderSystemImpl implements OrderSystem {

    static private String booleanTrueValue = "true";
    static private String booleanFalseValue = "false";

    private BlockingQueue queue = new ArrayBlockingQueue(18);
    private static boolean constructFinished = false;

   // private LRULinkedHashMap<Long,Row> lruBuyerMap = null;
   // private LRULinkedHashMap<Long,Row> lruGoodMap = null;


    //声明所有db对象
    OrderDB orderDB = null;
    BuyerOrderDB buyerOrderDB = null;
    GoodOrderDB goodOrderDB = null;
    BuyerDB buyerDB = null;
    GoodDB goodDB = null;

    static public class KV implements Comparable<KV>, KeyValue {
        String key;
        String rawValue;

        boolean isComparableLong = false;
        long longValue;

        private KV(String key, String rawValue) {
            this.key = key;
            this.rawValue = rawValue;
            if (key.equals("createtime") || key.equals("orderid")) {
                isComparableLong = true;
                longValue = Long.parseLong(rawValue);
            }
        }

        public String key() {
            return key;
        }

        public String valueAsString() {
            return rawValue;
        }

        public long valueAsLong() throws TypeException {
            try {
                return Long.parseLong(rawValue);
            } catch (NumberFormatException e) {
                throw new TypeException();
            }
        }

        public double valueAsDouble() throws TypeException {
            try {
                return Double.parseDouble(rawValue);
            } catch (NumberFormatException e) {
                throw new TypeException();
            }
        }

        public boolean valueAsBoolean() throws TypeException {
            if (this.rawValue.equals(booleanTrueValue)) {
                return true;
            }
            if (this.rawValue.equals(booleanFalseValue)) {
                return false;
            }
            throw new TypeException();
        }

        public int compareTo(KV o) {
            if (!this.key().equals(o.key())) {
                throw new RuntimeException("Cannot compare from different key");
            }
            if (isComparableLong) {
                return Long.compare(this.longValue, o.longValue);
            }
            return this.rawValue.compareTo(o.rawValue);
        }

        @Override
        public String toString() {
            return "[" + this.key + "]:" + this.rawValue;
        }
    }

    @SuppressWarnings("serial")
    static public class Row extends HashMap<String, KV> {
        Row() {
            super();
        }

        Row(KV kv) {
            super();
            this.put(kv.key(), kv);
        }

        KV getKV(String key) {
            KV kv = this.get(key);
            if (kv == null) {
                throw new RuntimeException(key + " is not exist");
            }
            return kv;
        }

        Row putKV(String key, String value) {
            KV kv = new KV(key, value);
            this.put(kv.key(), kv);
            return this;
        }

        Row putKV(String key, long value) {
            KV kv = new KV(key, Long.toString(value));
            this.put(kv.key(), kv);
            return this;
        }
    }

    private static class ResultImpl implements Result {
        private long orderid;
        private Row kvMap;

        private ResultImpl(long orderid, Row kv) {
            this.orderid = orderid;
            this.kvMap = kv;
        }

        static private ResultImpl createResultRow(Row orderData, Row buyerData,
                                                  Row goodData, Set<String> queryingKeys) {
            /** if (orderData == null || buyerData == null || goodData == null) {
             throw new RuntimeException("Bad data!");
             }**/
            Row allkv = new Row();
            long orderid;
            try {
                orderid = orderData.get("orderid").valueAsLong();
            } catch (TypeException e) {
                throw new RuntimeException("Bad data!");
            }

            for (KV kv : orderData.values()) {
                if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                    allkv.put(kv.key(), kv);
                }
            }
            if(buyerData != null) {
                for (KV kv : buyerData.values()) {
                    if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                        allkv.put(kv.key(), kv);
                    }
                }
            }
            if(goodData != null) {
                for (KV kv : goodData.values()) {
                    if (queryingKeys == null || queryingKeys.contains(kv.key)) {
                        allkv.put(kv.key(), kv);
                    }
                }
            }
            return new ResultImpl(orderid, allkv);
        }

        public KeyValue get(String key) {
            return this.kvMap.get(key);
        }

        public KeyValue[] getAll() {
            return kvMap.values().toArray(new KeyValue[0]);
        }

        public long orderId() {
            return orderid;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("orderid: " + orderid + " {");
            if (kvMap != null && !kvMap.isEmpty()) {
                for (KV kv : kvMap.values()) {
                    sb.append(kv.toString());
                    sb.append(",\n");
                }
            }
            sb.append('}');
            return sb.toString();
        }
    }


    public OrderSystemImpl() {


    }

    private class ConstructThread implements Runnable {

        @Override
        public void run() {
            try {
                long orderIndexStart = System.currentTimeMillis();
                orderDB.createIndex();
                long orderIndexEnd = System.currentTimeMillis();
                System.out.println("order create index time:" + ((orderIndexEnd - orderIndexStart) / 1000 / 60));
                long buyerIndexStart = System.currentTimeMillis();
                buyerDB.createIndex();
                long buyerIndexEnd = System.currentTimeMillis();
                System.out.println("buyer create index time:" + ((buyerIndexEnd - buyerIndexStart) / 1000 / 60));
                goodDB.createIndex();
                Thread.sleep(10);
                System.out.println("construct complete!!!");
                for (int i = 0; i < 18; i++) {
                    //放18个数据
                    queue.put(i);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public class MergeThread implements Runnable{
        private String finalMergeName = null;
        private List<String> srcFileNames = null;

        public MergeThread(String finalMergeName,List<String> srcFileNames){
            this.finalMergeName = finalMergeName;
            this.srcFileNames = srcFileNames;
        }

        @Override
        public void run() {
            FileSplit.mergerFiles(srcFileNames.toArray(new String[srcFileNames.size()]),finalMergeName);
        }
    }


    public void mergeOrderFiles(List<String> orderSplitFiles,List<String> orderMergeFiles)throws Exception{
        if(orderSplitFiles.size()<40){
            throw  new Exception("orderSplitFiles size less than 40!!");
        }

        //分配8个线程去合并
        List<Thread> threads = new ArrayList<>();
        MergeThread task1 = new MergeThread(orderMergeFiles.get(0),orderSplitFiles.subList(0,6));
        MergeThread task2 = new MergeThread(orderMergeFiles.get(1),orderSplitFiles.subList(6,11));
        MergeThread task3 = new MergeThread(orderMergeFiles.get(2),orderSplitFiles.subList(11,17));
        MergeThread task4 = new MergeThread(orderMergeFiles.get(3),orderSplitFiles.subList(17,22));
        MergeThread task5 = new MergeThread(orderMergeFiles.get(4),orderSplitFiles.subList(22,27));
        MergeThread task6 = new MergeThread(orderMergeFiles.get(5),orderSplitFiles.subList(27,32));
        MergeThread task7 = new MergeThread(orderMergeFiles.get(6),orderSplitFiles.subList(32,38));
        MergeThread task8 = new MergeThread(orderMergeFiles.get(7),orderSplitFiles.subList(38,orderSplitFiles.size()));
        Thread thread1 = new Thread(task1);
        thread1.start();
        threads.add(thread1);

        Thread thread2 = new Thread(task2);
        thread2.start();
        threads.add(thread2);

        Thread thread3 = new Thread(task3);
        thread3.start();
        threads.add(thread3);

        Thread thread4 = new Thread(task4);
        thread4.start();
        threads.add(thread4);


        Thread thread5 = new Thread(task5);
        thread5.start();
        threads.add(thread5);

        Thread thread6 = new Thread(task6);
        thread6.start();
        threads.add(thread6);

        Thread thread7 = new Thread(task7);
        thread7.start();
        threads.add(thread7);

        Thread thread8 = new Thread(task8);
        thread8.start();
        threads.add(thread8);


        //等待合并完成
        for(Thread tempThread:threads){
            tempThread.join();
        }

    }



    public void construct(Collection<String> orderFiles,
                          Collection<String> buyerFiles, Collection<String> goodFiles,
                          Collection<String> storeFolders) throws IOException, InterruptedException {

        //暂用一个输出文件夹
        String[] storeFolder = new String[3];

        if (storeFolders.size() < 3) {
            System.out.println("storeFolders size is less than 3!!!!");
            return;
        }
        int i = 0;
        for (String folder : storeFolders) {
            storeFolder[i] = folder;
            i += 1;
        }

        List<String> orderSplitFiles = new ArrayList<String>();
        List<String> buyerSplitFiles = new ArrayList<String>();
        List<String> goodSplitFiles = new ArrayList<String>();

        System.out.println("orderFiles length is " + orderFiles.size());
        System.out.println("buyerFiles length is " + buyerFiles.size());
        System.out.println("goodFiles length is " + goodFiles.size());


        orderSplitFiles.addAll(orderFiles);

        //合并buyer good文件

        long splitStart = System.currentTimeMillis();
        List<String> mergeBuyerFiles = new ArrayList<>();
        FileSplit.mergerFiles(buyerFiles.toArray(new String[buyerFiles.size()]), storeFolder[1] + "buyer_merge");
        mergeBuyerFiles.add(storeFolder[1] + "buyer_merge");
        List<String> mergeGoodFiles = new ArrayList<>();
        FileSplit.mergerFiles(goodFiles.toArray(new String[goodFiles.size()]), storeFolder[2] + "good_merge");
        mergeGoodFiles.add(storeFolder[2] + "good_merge");
        long splitEnd = System.currentTimeMillis();
        System.out.println("split time is " + (splitEnd - splitStart) / 1000);


        if(orderSplitFiles.size()>40&&CommonValue.MERGE_ORDER_FILE) {

            //合并文件
            List<String> orderMergerFiles = new ArrayList<>();
            orderMergerFiles.add(storeFolder[0] + "order_merge1");
            orderMergerFiles.add(storeFolder[0] + "order_merge2");
            orderMergerFiles.add(storeFolder[0] + "order_merge3");
            orderMergerFiles.add(storeFolder[1] + "order_merge4");
            orderMergerFiles.add(storeFolder[1] + "order_merge5");
            orderMergerFiles.add(storeFolder[1] + "order_merge6");
            orderMergerFiles.add(storeFolder[2] + "order_merge7");
            orderMergerFiles.add(storeFolder[2] + "order_merge8");

            long mergeStart = System.currentTimeMillis();
            try {
                //合并成8个文件
                mergeOrderFiles(orderSplitFiles, orderMergerFiles);
            } catch (Exception e) {
                e.printStackTrace();
                return;
            }
            long mergeEnd = System.currentTimeMillis();
            System.out.println("merge order file time is "+(mergeEnd-mergeStart)/1000/60);


            System.out.println("buyerMergeFiles length is " + mergeBuyerFiles.size());
            System.out.println("goodMergeFiles length is " + mergeGoodFiles.size());
            System.out.println("orderMergeFiles length is " + orderMergerFiles.size());


            orderDB = new OrderDB(orderMergerFiles, storeFolder);
            buyerOrderDB = new BuyerOrderDB(orderMergerFiles, storeFolder);
            goodOrderDB = new GoodOrderDB(orderMergerFiles, storeFolder);


            buyerDB = new BuyerDB(mergeBuyerFiles, storeFolder[0]);
            goodDB = new GoodDB(mergeGoodFiles, storeFolder[0]);
        }else{

            System.out.println("buyerMergeFiles length is " + mergeBuyerFiles.size());
            System.out.println("goodMergeFiles length is " + mergeGoodFiles.size());


            orderDB = new OrderDB(orderSplitFiles, storeFolder);
            buyerOrderDB = new BuyerOrderDB(orderSplitFiles, storeFolder);
            goodOrderDB = new GoodOrderDB(orderSplitFiles, storeFolder);

            buyerDB = new BuyerDB(mergeBuyerFiles, storeFolder[0]);
            goodDB = new GoodDB(mergeGoodFiles, storeFolder[0]);

        }

        //create index
        try {

            long buyerOrderIndexStart = System.currentTimeMillis();
            buyerOrderDB.createIndex();
            long buyerOrderIndexEnd = System.currentTimeMillis();
            System.out.println("buyer order create index time:" + ((buyerOrderIndexEnd - buyerOrderIndexStart) / 1000 / 60));

            long goodOrderIndexStart = System.currentTimeMillis();
            goodOrderDB.createIndex();
            long goodOrderIndexEnd = System.currentTimeMillis();
            System.out.println("goodOrder create index time:" + ((goodOrderIndexEnd - goodOrderIndexStart) / 1000 / 60));


           long orderIndexStart = System.currentTimeMillis();
            orderDB.createIndex();
            long orderIndexEnd = System.currentTimeMillis();
            System.out.println("order create index time:" + ((orderIndexEnd - orderIndexStart) / 1000 / 60));

            long buyerIndexStart = System.currentTimeMillis();
            buyerDB.createIndex();
            long buyerIndexEnd = System.currentTimeMillis();
            System.out.println("buyer create index time:" + ((buyerIndexEnd - buyerIndexStart) / 1000 / 60));
            goodDB.createIndex();

            //继续构造
            //ConstructThread constructThread = new ConstructThread();
            //new Thread(constructThread).start();

           // lruBuyerMap = new LRULinkedHashMap<>(1000000);
           // lruGoodMap = new LRULinkedHashMap<>(500000);

            Runtime run = Runtime.getRuntime();
            long max = run.maxMemory();
            long total = run.totalMemory();
            long free = run.freeMemory();
            long usable = max - total + free;
            System.out.println("最大内存 = " + max/1024/1024);
            System.out.println("已分配内存 = " + total/1024/1024);
            System.out.println("已分配内存中的剩余空间 = " + free/1024/1024);
            System.out.println("最大可用内存 = " + usable/1024/1024);
        } catch (Exception e) {
            System.out.println("build  index failed");
            e.printStackTrace();
        }

        System.out.println("build index succeed!!!!");
        long directMemory = Runtime.getRuntime().maxMemory();
        System.out.println("!!!!!!directMemory is " + directMemory);

    }

    public static Row String2Row(String line) {
        if(line == null){
            return null;
        }
        //
        String[] kvs = line.split("\t");
        Row kvMap = new Row();
        for (String rawkv : kvs) {
            int p = rawkv.indexOf(':');
            String key = "";
            String value = "";
            key = rawkv.substring(0, p);
            value = rawkv.substring(p + 1);

            if (key.length() == 0 || value.length() == 0) {
                throw new RuntimeException("Bad data:" + line);
            }
            KV kv = new KV(key, value);
            kvMap.put(kv.key(), kv);
        }
        return kvMap;
    }

    private ResultImpl createResultFromOrderData(Row orderData,
                                                 Collection<String> keys) {


        String buyerId = orderData.get("buyerid").rawValue;
        String goodId = orderData.get("goodid").rawValue;

        Long buyerHash = StringToLong.hash(buyerId);
        Long goodHash = StringToLong.hash(goodId);

        String buyerLine = null;
        String goodLine = null;
        boolean needBuyer = false;
        boolean needGood = false;
        Row buyerRow = null;
        Row goodRow = null;
        //System.out.println(BuyerTable.keySet);
        if(keys != null) {
            try {
                for (String key : keys) {
                    if (BuyerTable.keySet.contains(key)) {
                        needBuyer = true;
                        break;
                    }
                }
                if (needBuyer) {
                    Row lruBuyerRow = BuyerTable.lruBuyerMap.get(buyerHash);
                    if(lruBuyerRow != null){
                        buyerRow = lruBuyerRow;
                    }else {
                        //内存中不存在
                        buyerLine = buyerDB.queryBuyer(buyerId);
                        buyerRow = String2Row(buyerLine);
                        //缓存buyer
                        BuyerTable.lruBuyerMap.put(buyerHash,buyerRow);
                    }
                }
            } catch (Exception e) {
                System.out.println("OrderSystemImpl==>createResultFromData: queryBuyer failed");
                e.printStackTrace();
            }

            try {

                for (String key : keys) {
                    if (GoodTable.keySet.contains(key)) {
                        needGood = true;
                        break;
                    }
                }
                if (needGood) {
                    Row lruGoodRow = GoodTable.lruGoodMap.get(goodHash);
                    if(lruGoodRow != null){
                        goodRow = lruGoodRow;
                    }else {
                        //内存中不存在
                        goodLine = goodDB.queryGood(goodId);
                        goodRow = String2Row(goodLine);
                        //缓存good
                        GoodTable.lruGoodMap.put(goodHash,goodRow);
                    }
                }
            } catch (Exception e) {
                System.out.println("OrderSystemImpl==>createResultFromData: queryGood failed");
                e.printStackTrace();
            }
        }else{
            //buyer 和good都要查询
            try{
                Row lruBuyerRow = BuyerTable.lruBuyerMap.get(buyerHash);
                if(lruBuyerRow!= null){
                    buyerRow = lruBuyerRow;
                }else {
                    buyerLine = buyerDB.queryBuyer(buyerId);
                    buyerRow = String2Row(buyerLine);
                    BuyerTable.lruBuyerMap.put(buyerHash,buyerRow);
                }

                Row lruGoodRow = GoodTable.lruGoodMap.get(goodHash);
                if(lruGoodRow !=null){
                    goodRow = lruGoodRow;
                }else{
                    goodLine = goodDB.queryGood(goodId);
                    goodRow = String2Row(goodLine);
                    GoodTable.lruGoodMap.put(goodHash,goodRow);
                }

                if(buyerRow == null || goodRow == null){
                    return null;
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }

        return ResultImpl.createResultRow(orderData, buyerRow, goodRow,
                createQueryKeys(keys));

    }


    private HashSet<String> createQueryKeys(Collection<String> keys) {
        if (keys == null) {
            return null;
        }
        return new HashSet<String>(keys);
    }


    public Result queryOrder(long orderId, Collection<String> keys) {

        /** if (!constructFinished) {
         try {
         queue.take();
         constructFinished = true;
         } catch (Exception e) {
         e.printStackTrace();
         }
         }**/

        String line = null;

        try {
            line = orderDB.queryOrder(orderId);
        } catch (Exception e) {
            System.out.println("OrderSystemImpl==> queryOrder failed!!!");
            e.printStackTrace();

        }

        if (line == null) {
            //System.out.println("OrderSystemImpl==> queryOrder: not found!");
            return null;
        }

        //  System.out.println("OrderSystemImpl==> queryOrder:" + line);

        return createResultFromOrderData(String2Row(line), createQueryKeys(keys));

    }


    public Iterator<Result> queryOrdersByBuyer(long startTime, long endTime,
                                               String buyerid) {
        /** if (!constructFinished) {
         try {
         queue.take();
         constructFinished = true;
         } catch (Exception e) {
         e.printStackTrace();
         }
         }**/

        List<String> orderLines = new ArrayList<String>();

        try {
            orderLines = buyerOrderDB.queryOrder(startTime, endTime, buyerid);
        } catch (Exception e) {
            System.out.println("OrderSystemImpl=====> queryOrdersByBuyer query orders failed!!");
            e.printStackTrace();
        }


        final List<String> orderList = orderLines;
        Collections.sort(orderList, new KeyComparator("createtime"));

        //  System.out.println("OrderSystemImpl=====> queryOrdersByBuyer order succeed ");

        return new Iterator<Result>() {

            List<String> list = orderList;

            @Override
            public boolean hasNext() {
                return list != null && list.size() > 0;
            }

            @Override
            public Result next() {
                if (!hasNext()) {
                    return null;
                }
                String orderRecord = list.get(0);
                list.remove(orderRecord);
                return createResultFromOrderData(String2Row(orderRecord), null);
            }

            @Override
            public void remove() {
            }
        };


    }

    public Iterator<Result> queryOrdersBySaler(String salerid, String goodid,
                                               Collection<String> keys) {
        /** if (!constructFinished) {
         try {
         queue.take();
         constructFinished = true;
         } catch (Exception e) {
         e.printStackTrace();
         }
         }**/

        List<String> orderLines = new ArrayList<String>();
        final Collection<String> key = keys;


        try {
            orderLines = goodOrderDB.queryOrder(goodid);
        } catch (Exception e) {
            System.out.println("OrderSysteImpl=====> queryOrdersBySaler query orders failed!!");
            e.printStackTrace();
        }


        final List<String> orderList = orderLines;
        //打印出来

        // System.out.println("saler's order size is " + orderList.size());

        Collections.sort(orderList, new KeyComparatorTwo("orderid"));

        return new Iterator<Result>() {

            List<String> list = orderList;

            @Override
            public boolean hasNext() {
                return list != null && list.size() > 0;
            }

            @Override
            public Result next() {
                if (!hasNext()) {
                    return null;
                }
                String orderRecord = list.get(0);
                list.remove(orderRecord);
                return createResultFromOrderData(String2Row(orderRecord), key);
            }

            @Override
            public void remove() {

            }
        };


    }

    public KeyValue sumOrdersByGood(String goodid, String key) {
       /** if (!constructFinished) {
         try {
         queue.take();
         constructFinished = true;
         } catch (Exception e) {
         e.printStackTrace();
         }
         }**/
        List<String> orderLines = new ArrayList<String>();
        try {
            orderLines = goodOrderDB.queryOrder(goodid);
        } catch (Exception e) {
            System.out.println("OrderSystemImpl sumOrdersByGood query Orders failed!!!");
            e.printStackTrace();
        }
        final HashSet<String> queryKeySet = new HashSet<String>();
        queryKeySet.add(key);

        if (orderLines.size() == 0) {
            return null;
        }

        final List<String> resultList = orderLines;

        ResultImpl result;
        //Double sum = 0d;
        //if long
        try {
            boolean hasValidData = false;
            long sum = 0;
            for (String order : resultList) {
                result = createResultFromOrderData(String2Row(order), queryKeySet);
                if (result == null) {
                    return null;
                } else {
                    KeyValue value = result.get(key);
                    if (value != null) {
                        sum += value.valueAsLong();
                        hasValidData = true;
                    }
                }

            }
            if (hasValidData) {
                return new KV(key, Long.toString(sum));
            }

        } catch (Exception e) {
            System.err.println("type is not long");
        }

        try {
            boolean hasValidData = false;
            double sum = 0.0;
            for (String order : resultList) {
                result = createResultFromOrderData(String2Row(order), queryKeySet);
                if (result == null) {
                    return null;
                } else {
                    KeyValue value = result.get(key);
                    if (value != null) {
                        sum += value.valueAsDouble();
                        hasValidData = true;
                    }
                }

            }
            if (hasValidData) {
                return new KV(key, Double.toString(sum));
            }

        } catch (Exception e) {
            System.err.println("type is not double");
        }

        return null;
    }


    public static void main(String[] args) throws IOException,
            InterruptedException {

        // init order system
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
        os.construct(orderFiles, buyerFiles, goodFiles, storeFolders);

        // 用例
        long start = System.currentTimeMillis();
        long orderid = 627207926;
        List<String> keys = new ArrayList<String>();
        keys.add("a_g_17779");
        System.out.println("\n查询订单号为" + orderid + "的订单");
        System.out.println(os.queryOrder(orderid, keys));

        System.out.println("\n查询订单号为" + orderid + "的订单，查询的keys为空，返回订单，但没有kv数据");
        System.out.println(os.queryOrder(orderid, null));

        System.out.println("\n查询订单号为" + orderid
                + "的订单的contactphone, buyerid, foo, done, price字段");
        List<String> queryingKeys = new ArrayList<String>();
        queryingKeys.add("contactphone");
        queryingKeys.add("buyerid");
        queryingKeys.add("foo");
        queryingKeys.add("done");
        queryingKeys.add("price");
        Result result = os.queryOrder(orderid, queryingKeys);
        System.out.println(result);
        System.out.println("\n查询订单号不存在的订单");
        result = os.queryOrder(61236191, queryingKeys);
        if (result == null) {
            System.out.println(61236191 + " order not exist");
        }

        String buyerid = "ap-bcd4-9ed250d15f4f";
        long startTime = 1474483106;
        long endTime = 1480592490;
        System.out.println("\n查询买家ID为" + buyerid + "的一定时间范围内的订单");
        Iterator<Result> it = os.queryOrdersByBuyer(startTime, endTime, buyerid);
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        String goodid = "gd-a833-2f48381c6738";
        String salerid = "ay-b4d2-2530df4acd6b";
        System.out.println("\n查询商品id为" + goodid + "，商家id为" + salerid + "的订单");
        List<String> needKeys = new ArrayList<>();
        needKeys.add("good_name");
        needKeys.add("a_o_12490");
        needKeys.add("a_o_4082");
        needKeys.add("buyerid");
        needKeys.add("a_o_9238");
        it = os.queryOrdersBySaler(salerid, goodid, needKeys);
        while (it.hasNext()) {
            System.out.println(it.next());
        }

        goodid = "al-ac5b-f20bd9da1ef3";
        String attr = "a_b_11255";
        System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        System.out.println(os.sumOrdersByGood(goodid, attr));

        attr = "a_o_28730";
        System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        KeyValue sum = os.sumOrdersByGood(goodid, attr);
        if (sum == null) {
            System.out.println("由于该字段是布尔类型，返回值是null");
        }

        attr = "a_o_3070";
        System.out.println("\n对商品id为" + goodid + "的 " + attr + "字段求和");
        sum = os.sumOrdersByGood(goodid, attr);
        if (sum == null) {
            System.out.println("由于该字段不存在，返回值是null");
        }

        long end = System.currentTimeMillis();
        System.out.println("=========>time:" + (end - start));
    }
}