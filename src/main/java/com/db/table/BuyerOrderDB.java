package com.db.table;

import com.util.CommonValue;
import com.util.FileSplit;
import com.util.LRULinkedHashMap;
import com.util.StringToLong;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 可能包含多张子表  索引为buyerId
 * Created by iceke on 16/7/16.
 */
public class BuyerOrderDB {
    private List<BuyerOrderMap> buyerOrderMaps = null;
    private static final int LINES_NUM = CommonValue.ORDER_FILE_SPLIT_LINES;
    private List<String> splitFiles = null;
    private String[] folders = null;
    //private LRULinkedHashMap<Long,List<String>> lruMap = new LRULinkedHashMap<>(300000);

    /**
     * 有多个订单文件
     *
     * @param
     */
    public BuyerOrderDB(List<String> splitOrderFiles,String[] folders) {
        this.splitFiles = splitOrderFiles;
        this.buyerOrderMaps = new ArrayList<BuyerOrderMap>();
        this.folders = folders;
    }



    public void createIndex() throws Exception{

        List<List<String>> threadSplitFiles = new ArrayList<List<String>>();
        int totalSize = splitFiles.size();
        int cores = 6;
        int sizeOfThread = totalSize/cores;

        if(totalSize!=8 && totalSize!=43){
            threadSplitFiles.add(splitFiles);
        }else {
            System.out.println("orderBuyerFiles to 8 threads!!!!");
            if(totalSize == 43) {
                threadSplitFiles.add(splitFiles.subList(0, 6));
                threadSplitFiles.add(splitFiles.subList(6, 11));
                threadSplitFiles.add(splitFiles.subList(11, 17));
                threadSplitFiles.add(splitFiles.subList(17, 22));
                threadSplitFiles.add(splitFiles.subList(22, 27));
                threadSplitFiles.add(splitFiles.subList(27, 32));
                threadSplitFiles.add(splitFiles.subList(32, 37));
                threadSplitFiles.add(splitFiles.subList(37, totalSize));
            }else {
                threadSplitFiles.add(splitFiles.subList(0, 1));
                threadSplitFiles.add(splitFiles.subList(1, 2));
                threadSplitFiles.add(splitFiles.subList(2, 3));
                threadSplitFiles.add(splitFiles.subList(3, 4));
                threadSplitFiles.add(splitFiles.subList(4, 5));
                threadSplitFiles.add(splitFiles.subList(5, 6));
                threadSplitFiles.add(splitFiles.subList(6, 7));
                threadSplitFiles.add(splitFiles.subList(7, totalSize));
            }

        }

        //将索引文件分配到多个线程去build
        List<Thread> threads = new ArrayList<Thread>();
        int j = 1;
        String tempFolder = null;
        for(List<String> threadFiles: threadSplitFiles){
            if(j ==1 ||j ==2||j==3){
                tempFolder = folders[0];
            }else if(j ==4 ||j ==5||j==6){
                tempFolder = folders[1];
            }else{
                tempFolder = folders[2];
            }
            Thread tempThread =  new Thread(new LoadThread(threadFiles,tempFolder));
            tempThread.start();
            threads.add(tempThread);
            j++;
        }

        //等待所有线程执行完
        for(Thread thread:threads){
            thread.join();
        }

        System.out.println("total BuyerOrderMaps size is "+buyerOrderMaps.size());

    }


    private class LoadThread implements Runnable{
        private List<String> splitThreadFiles = null;
        private String threadFolder = null;
        public LoadThread(List<String> splitTreadFiles,String folder){
            this.threadFolder = folder;
            this.splitThreadFiles = splitTreadFiles;
        }

        @Override
        public void run() {

            try {
                int i = 1;
                for (String splitThreadFile : splitThreadFiles) {
                    BuyerOrderMap buyerOrderMap = new BuyerOrderMap(threadFolder+CommonValue.BUYER_ORDER_INDEX + "_" + i + "_" + Thread.currentThread().getId());
                    File buyerOrderFile = new File(splitThreadFile);
                    long start = System.currentTimeMillis();
                    buyerOrderMap.load(buyerOrderFile);
                    long end = System.currentTimeMillis();
                    System.out.println("load time is "+(end-start)/1000);
                    buyerOrderMap.flush();
                    buyerOrderMaps.add(buyerOrderMap);
                    System.gc();
                    i++;
                }
            }catch (Exception e){
                e.printStackTrace();
            }


        }
    }


    /**
     * 根据buyerid查询订单,可返回多个记录
     * @param buyerId
     * @return
     */
    public List<String> queryOrder(String buyerId) throws Exception{
        List<String> buyerOrderValues = null;
        //查缓存
        /**long buyerHash = StringToLong.hash(buyerId);
         buyerOrderValues = lruMap.get(buyerHash);
         if(buyerOrderValues != null && buyerOrderValues.size()!=0){
         return buyerOrderValues;
         }**/
        buyerOrderValues = new ArrayList<String>();
        for (BuyerOrderMap buyerOrderMap : buyerOrderMaps) {
            List<String> tempList = buyerOrderMap.findOrder(buyerId);
            if (tempList.size() != 0) {
                buyerOrderValues.addAll(tempList);
            }
        }
        // lruMap.put(buyerHash,buyerOrderValues);
        return buyerOrderValues;
    }

    public List<String> queryOrder(long startTime, long endTime,String buyerId) throws Exception{
        List<String> orders = queryOrder(buyerId);
        List<String> finalOrders = new ArrayList<String>();
        String key = "createtime";
        for(String order:orders){
            //有错 这样不一定能获取到时间

            int a1=order.indexOf(key);
            int b1=order.indexOf("\t",a1);
            long createTime = Long.parseLong(order.substring(a1+key.length()+1,(b1!=-1)?b1:order.length()));
            if(createTime>=startTime && createTime <= endTime){
                finalOrders.add(order);
            }
        }

        return finalOrders;

    }

    public static void main(String args[]) {
        try {
            List<String> orderFiles = new ArrayList<String>();
            List<String> orderSplitFiles = new ArrayList<>();
            /**orderFiles.add("data/order.0.0");
             orderFiles.add("data/order.1.1");
             orderFiles.add("data/order.2.2");
             orderFiles.add("data/order.0.3");**/
            orderFiles.add("data/order_record_4g.txt");
            String[] folders = {"data/split1/","data/split2/","data/split3/"};
            for(String orderFile:orderFiles){
                List<String> tempList = FileSplit.splitToDir(orderFile,"data/split/", CommonValue.ORDER_FILE_SPLIT_LINES);
                orderSplitFiles.addAll(tempList);
            }
            BuyerOrderDB buyerOrderDB = new BuyerOrderDB(orderSplitFiles,folders);
            long start = System.currentTimeMillis();
            buyerOrderDB.createIndex();
            long end = System.currentTimeMillis();
            System.out.println("time is "+(end-start)/1000);

            System.out.println(buyerOrderDB.queryOrder("ap_2f35a41d-1f0e-4c18-a270-b8b23ff64aee"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}