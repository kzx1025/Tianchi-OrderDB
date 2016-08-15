package com.test;

import com.db.BplusTreeLongToLong;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by iceke on 16/7/24.
 */
public class TestBplus {

    private BplusTreeLongToLong records = null;
    public TestBplus(BplusTreeLongToLong records){
        this.records = records;
    }


    public static void main(String args[]){
        try {
            BplusTreeLongToLong records = new BplusTreeLongToLong("data/test.index", 1024);
            //TestBplus testBplus = new TestBplus(records);
            //testBplus.construct();

            records.put(12L,34L);
            records.put(24L,56L);
            records.flush();
            BplusTreeLongToLong newRecord = new BplusTreeLongToLong("data/test.index", 1024);
            newRecord.put(78L,45L);
            newRecord.put(1111L,55L);
            newRecord.flush();

            BplusTreeLongToLong record3 = new BplusTreeLongToLong("data/test.index", 1024);
            record3.put(78L,45L);
            record3.put(11L,666L);
            for(long j =100;j<2000000;j++){
                record3.put(j,j-1);
            }
            record3.flush();


            BplusTreeLongToLong result = new BplusTreeLongToLong("data/test.index", 1024);
            System.out.println(result.find(12L));
            System.out.println(result.find(24L));
            System.out.println(result.find(78L));
            System.out.println(result.find(11L));

            for(long m =0;m<2000000;m++){
                System.out.println(result.find(m));
                if(m==200){
                    break;
                }
            }

        }catch (Exception e){
            e.printStackTrace();
        }
    }

    public void construct(){
        PutThread putThread = new PutThread(records);
        FlushThread flushThread = new FlushThread(records);
        List<Thread> threads = new ArrayList<>();

        for(int i = 0;i<5;i++) {
            Thread a = new Thread(putThread);
            a.start();
            threads.add(a);
        }
        try {

            for (Thread thread : threads) {
                thread.join();
            }

            records.flush();
        }catch (Exception e){
            e.printStackTrace();
        }




    }


    private class PutThread implements Runnable{
        private BplusTreeLongToLong tree;

        public PutThread(BplusTreeLongToLong tree){
            this.tree = tree;

        }

        @Override
        public void run() {

            for(int i = 0 ;i<100;i++){
                tree.put((long)(Math.random()*300),(long)(Math.random()*20));
            }

        }
    }

    private class FlushThread implements Runnable{
        private BplusTreeLongToLong tree;

        public FlushThread(BplusTreeLongToLong tree){
            this.tree = tree;

        }

        @Override
        public void run() {
            while(true) {
                try {

                    tree.flush();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }

        }
    }


}
