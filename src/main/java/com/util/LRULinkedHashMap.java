package com.util;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.db.BplusTreeLongToLong;

import java.io.File;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Created by iceke on 16/7/31.
 */
public class LRULinkedHashMap<K,V> extends LinkedHashMap<K,V> {
    //定义缓存的容量
    private int capacity;
    private static final long serialVersionUID = 1L;
    //带参数的构造器
    public LRULinkedHashMap(int capacity){
        //调用LinkedHashMap的构造器，传入以下参数
        super(16,0.75f,true);
        //传入指定的缓存最大容量
        this.capacity=capacity;
    }
    //实现LRU的关键方法，如果map里面的元素个数大于了缓存最大容量，则删除链表的顶端元素
    @Override
    public boolean removeEldestEntry(Map.Entry<K, V> eldest){
        //System.out.println(eldest.getKey() + "=" + eldest.getValue());
        return size()>capacity;
    }

    public static void main(String args[]){
        Map<Long,OrderSystemImpl.Row> map=new LRULinkedHashMap<>(4000000);

        File file  = new File("data/order_record_4g.txt");
        try {
            BufferedRandomAccessFile randomAccessFile = new BufferedRandomAccessFile(file, "r");
            int i = 0;
            String line = null;
            String[] pair = null;
            int lines = 0;
            while (true) {
                long orderPosition = randomAccessFile.getFilePointer();
                line = randomAccessFile.readLine();
                if (line == null) {
                    break;
                }
                pair = line.split("\t");
                long orderId = 0L;
                for (String item : pair) {
                    if (item.split(":")[0].equals("orderid")) {
                        orderId = Long.parseLong(item.split(":")[1]);
                        break;
                    }
                }
                int length = line.length();
                if(map.size()<2000000) {

                 //   map.put(orderId, OrderSystemImpl.String2Row(line));
                }
                //进行合成



                // System.out.println("i:" + i + " position:" + orderPosition);
                i++;
                lines++;


            }
        }catch (Exception e){
            e.printStackTrace();
        }
        Runtime run = Runtime.getRuntime();
        long max = run.maxMemory();
        long total = run.totalMemory();
        long free = run.freeMemory();
        long usable = max - total + free;
        System.out.println("最大内存 = " + max/1024/1024);
        System.out.println("已分配内存 = " + total/1024/1024);
        System.out.println("已分配内存中的剩余空间 = " + free/1024/1024);
        System.out.println("最大可用内存 = " + usable/1024/1024);

        long start = System.currentTimeMillis();
        OrderSystemImpl.Row value = OrderSystemImpl.String2Row("key:value");
        for(long i = 0;i<40000000;i++){
            map.put(i,value);

        }
        long end = System.currentTimeMillis();
        System.out.println((end-start)/1000);
    }
}