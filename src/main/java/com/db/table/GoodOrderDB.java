package com.db.table;

import com.util.CommonValue;
import com.util.FileSplit;
import com.util.LRULinkedHashMap;
import com.util.StringToLong;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by iceke on 16/7/18.
 */
public class GoodOrderDB {
    private List<GoodOrderMap> goodOrderMaps = null;
    private List<String> splitFiles = null;
    private String[] folders = null;
    //  private LRULinkedHashMap<Long,List<String>> lruMap = new LRULinkedHashMap<>(200000);


    public GoodOrderDB(List<String> splitOrderFiles,String[] folders) {
        this.splitFiles = splitOrderFiles;
        this.goodOrderMaps = new ArrayList<GoodOrderMap>();
        this.folders = folders;
    }



    public void createIndex() throws Exception {
        List<List<String>> threadSplitFiles = new ArrayList<List<String>>();
        int totalSize = splitFiles.size();
        int cores = 6;
        if(totalSize!=8 && totalSize!=43){
            threadSplitFiles.add(splitFiles);
        }else {
            System.out.println("orderGoodFiles to 8 threads!!!!");
            if(totalSize == 43) {
                //没有合并文件
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
            Thread tempThread =  new Thread(new LoadThread(threadFiles, tempFolder));
            tempThread.start();
            threads.add(tempThread);
            j++;
        }

        //等待所有线程执行完
        for(Thread thread:threads){
            thread.join();
        }

        System.out.println("total GoodOrderMaps size is "+goodOrderMaps.size());


    }

    private class LoadThread implements Runnable{
        private List<String> splitThreadFiles = null;
        private String threadFolder = null;
        public LoadThread(List<String> splitTreadFiles, String folder){
            this.splitThreadFiles = splitTreadFiles;
            this.threadFolder = folder;
        }

        @Override
        public void run() {
            try {
                int i = 1;
                for (String splitThreadFile :splitThreadFiles) {
                    GoodOrderMap goodOrderMap = new GoodOrderMap(threadFolder+CommonValue.GOOD_ORDER_INDEX + "_" + i + "_" + Thread.currentThread().getId());
                    File goodOrderFile = new File(splitThreadFile);
                    goodOrderMap.load(goodOrderFile);
                    goodOrderMap.flush();
                    goodOrderMaps.add(goodOrderMap);
                    System.gc();
                    i++;
                }
            }catch (Exception e){
                e.printStackTrace();
            }


        }
    }


    /**
     * 根据orderid返回多个order记录
     * @param goodId
     * @return
     * @throws Exception
     */
    public List<String> queryOrder(String goodId) throws Exception{

        List<String> goodOrderValues = null;
        //查缓存
        /**long goodHash = StringToLong.hash(goodId);
         goodOrderValues = lruMap.get(goodHash);
         if(goodOrderValues != null && goodOrderValues.size()!=0){
         return goodOrderValues;
         }**/

        //差索引
        goodOrderValues = new ArrayList<String>();
        for (GoodOrderMap goodOrderMap : goodOrderMaps) {
            List<String> tempList = goodOrderMap.findOrder(goodId);
            if (tempList.size() != 0) {
                goodOrderValues.addAll(tempList);
            }
        }

        //放入缓存
        // lruMap.put(goodHash,goodOrderValues);
        return goodOrderValues;
    }


    public static void main(String args[]){
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
            GoodOrderDB goodOrderDB = new GoodOrderDB(orderSplitFiles,folders);
            long start = System.currentTimeMillis();
            goodOrderDB.createIndex();
            long end = System.currentTimeMillis();
            System.out.println("time is "+(end-start)/1000);


            System.out.println(goodOrderDB.queryOrder("good_017cd907-1630-499b-9324-4eccad0ccc6f"));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}