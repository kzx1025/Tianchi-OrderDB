package com.test;

import com.alibaba.middleware.race.KeyComparator;
import com.alibaba.middleware.race.OrderSystem;
import com.db.Bytes;
import com.db.table.OrderDB;
import com.util.BufferedRandomAccessFile;
import com.util.CommonValue;
import com.util.FileIndex;
import com.util.FileSort;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by iceke on 16/7/8.
 */
public class TestMain {

    public static Iterator<String> getIterator(){
        List<String>  orderLines = new ArrayList<>();
        orderLines.add("dadsd");
        orderLines.add("safgds");
        orderLines.add("dafdsgfg'");
        final SortedSet<String> sortedSet=new TreeSet<String>();
        for(String orderLine:orderLines){
            sortedSet.add(orderLine);
        }
        System.out.println("OrderSystemImpl=====> queryOrdersByBuyer order:"+sortedSet);

        return new Iterator<String>() {

            SortedSet<String> set=sortedSet;
            @Override
            public boolean hasNext() {
                return set!= null && set.size()>0;
            }

            @Override
            public String next() {
                if(!hasNext()){
                    return null;
                }
                String orderRecord=set.last();
                set.remove(orderRecord);
                return orderRecord;
            }

            @Override
            public void remove() {
            }
        };
    }



    public static void main(String args[]){
        try {

            BufferedRandomAccessFile randomAccessFile = new BufferedRandomAccessFile("data/position.index", "rw");
            FileChannel channel = randomAccessFile.getChannel();

            int b = (int) randomAccessFile.getFilePointer();
            randomAccessFile.writeBytes("123&0000hahahhahahahagffffffffffsdfbsbfbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
            int c = (int) randomAccessFile.getFilePointer();
            randomAccessFile.writeBytes("34543#3534654#35465#445");
            int d = (int) randomAccessFile.getFilePointer();
            randomAccessFile.writeBytes("2345345#45435#1454");

            ByteBuffer buffer = ByteBuffer.allocate(8);
            channel.read(buffer,b);
            buffer.flip();
            byte[] m = new byte[8];
            buffer.get(m);
            String strRead = new String(m);
            System.out.println(strRead);
            ByteBuffer finalBuffer = ByteBuffer.allocate(50);
            channel.read(finalBuffer,b+8);
            finalBuffer.flip();
            byte[] result = new byte[50];
            finalBuffer.get(result);
            System.out.println(new String(result));



           /** randomAccessFile.seek(a);
            String rawValue = randomAccessFile.readUTF();
            System.out.println(rawValue);
            randomAccessFile.seek(b);
            System.out.println(randomAccessFile.readUTF());
            randomAccessFile.seek(c);
            System.out.println(randomAccessFile.readUTF());**/
            String q = "sfdf#dsda";

           System.out.println(q.split("#").length);


        }catch (Exception e){
            e.printStackTrace();
        }


    }



}
