package com.db.table;

import com.util.CommonValue;
import com.util.FileSplit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * 可能包含多张子表  索引为orderId
 * Created by iceke on 16/7/15.
 */
public class OrderDB {

    private List<OrderTable> orderTables = null;
    private static final int LINES_NUM = CommonValue.ORDER_FILE_SPLIT_LINES;
    private List<String> splitFiles = null;
    private String[] folders = null;

    public OrderDB(List<String> splitOrderFiles,String[] folders) {
        this.orderTables = new ArrayList<OrderTable>();
        this.splitFiles = splitOrderFiles;
        this.folders = folders;
    }



    /**
     * 6核 多线程处理索引
     * @throws Exception
     */
    public void createIndex() throws Exception{
        //cores几个 便将splitFiles分成几组
        //splitSize为50
        //所以固定的最优分配方式为8,7,8,7,8,7,5
        int cores = 8;
        List<List<String>> threadSplitFiles = new ArrayList<List<String>>();
        //list总大小
        int totalSize = splitFiles.size();
        //每组大小
        int sizeOfThread = totalSize/cores;

        if(totalSize <40){

            //for test
            System.out.println("enter test");
            threadSplitFiles.add(splitFiles);
            //threadSplitFiles.add(splitFiles.subList(2, 3));

        }else {
            //8个线程
            System.out.println("orderFiles to 8 threads!!!!");
            threadSplitFiles.add(splitFiles.subList(0, 5));
            threadSplitFiles.add(splitFiles.subList(5, 10));
            threadSplitFiles.add(splitFiles.subList(10, 15));
            threadSplitFiles.add(splitFiles.subList(15, 20));
            threadSplitFiles.add(splitFiles.subList(20, 25));
            threadSplitFiles.add(splitFiles.subList(25, 30));
            threadSplitFiles.add(splitFiles.subList(30, 37));
            threadSplitFiles.add(splitFiles.subList(37, totalSize));
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

        System.out.println("total OrderTables size is "+orderTables.size());


    }


    /**
     * 根据orderid查询order记录,查到一个便返回
     *
     * @param orderId
     * @return
     */
    public String queryOrder(long orderId) throws Exception {
        List<String> orderValues = new ArrayList<String>();
        for (OrderTable orderTable : orderTables) {
            List<String> tempList = orderTable.findOrderId(orderId);
            if (tempList.size() != 0) {
                orderValues.addAll(tempList);
                break;
            }
        }
        if(orderValues.size() == 0){
            return null;
        }else {
            return orderValues.get(0);
        }
    }

    private class LoadThread implements Runnable{
        private String threadFolder = null;
        private List<String> splitThreadFiles = null;
        public LoadThread(List<String> splitTreadFiles,String folder){
            this.splitThreadFiles = splitTreadFiles;
            this.threadFolder = folder;

        }

        @Override
        public void run() {
            try {
                int i = 1;
                for (String splitThreadFile : splitThreadFiles) {
                    OrderTable orderTable = new OrderTable(threadFolder+CommonValue.ORDER_INDEX + "_" + i + "_" + Thread.currentThread().getId());
                    File orderFile = new File(splitThreadFile);
                    System.out.println("orderFile length is "+orderFile.length());
                    orderTable.load(orderFile);
                    orderTable.flush();
                    //索引建立完成重新包装索引
                    OrderTable newTable = new OrderTable(orderTable.getIndexFile());
                    newTable.setRandomAccessFile(orderTable.getRandomAccessFile());
                    newTable.setFileChannel(orderTable.getFileChannel());
                    newTable.setMappedByteBuffer(orderTable.getMappedByteBuffer());
                    orderTables.add(newTable);
                    orderTable = null;
                    System.gc();
                    i++;
                }
            }catch (Exception e){
                e.printStackTrace();
            }


        }
    }


    public static void main(String args[]) {
        try {
            List<String> orderFiles = new ArrayList<String>();
            List<String> orderSplitFiles = new ArrayList<String>();
            //orderFiles.add("data/order.0.0");
            //orderFiles.add("data/order.1.1");
            //orderFiles.add("data/order.2.2");
            //orderFiles.add("data/order.0.3");
            orderFiles.add("data/order_record_4g.txt");
            String[] folders = {"data/split1/","data/split2/","data/split3/"};

            //拆分文件
            for(String orderFile:orderFiles){
                List<String> tempList = FileSplit.splitToDir(orderFile,"data/split/", CommonValue.ORDER_FILE_SPLIT_LINES);
                orderSplitFiles.addAll(tempList);
            }
            OrderDB orderDB = new OrderDB(orderSplitFiles,folders);
            long start = System.currentTimeMillis();
            orderDB.createIndex();
            long end = System.currentTimeMillis();
            System.out.println("time:"+(end - start));

            String result = orderDB.queryOrder(612361917);
            String result2 = orderDB.queryOrder(3150337);
            String result3 = orderDB.queryOrder(588886277);

            //List<Integer> result = orderDB.queryOrderPosition(2982386);

            System.out.println(result2);
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

}