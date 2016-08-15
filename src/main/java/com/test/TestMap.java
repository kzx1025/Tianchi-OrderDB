package com.test;

import com.db.table.BuyerOrderDB;
import com.util.CommonValue;
import com.util.FileSplit;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by iceke on 16/7/27.
 */
public class TestMap {
    private BuyerOrderDB buyerOrderDB = null;
    public TestMap(){
        List<String> orderFiles = new ArrayList<String>();
        List<String> orderSplitFiles = new ArrayList<>();
        /**orderFiles.add("data/order.0.0");
         orderFiles.add("data/order.1.1");
         orderFiles.add("data/order.2.2");
         orderFiles.add("data/order.0.3");**/
        orderFiles.add("data/order_record_4g.txt");
        try {
            for (String orderFile : orderFiles) {
                List<String> tempList = FileSplit.splitToDir(orderFile, "data/split/", CommonValue.ORDER_FILE_SPLIT_LINES);
                orderSplitFiles.addAll(tempList);
                String[] folders = {"data/split1/","data/split2/","data/split3/"};
                buyerOrderDB = new BuyerOrderDB(orderSplitFiles,folders);
                buyerOrderDB.createIndex();
            }
        }catch (Exception e){

        }

    }
    public static void main(String args[]){
        TestMap map = new TestMap();
        map.come();

    }

    public void come(){

        for(int i =0;i<10;i++) {
            RequestThread thread = new RequestThread(buyerOrderDB);
            new Thread(thread).start();
        }

    }

    private class RequestThread implements Runnable{
        private BuyerOrderDB buyerOrderDB = null;

        public RequestThread(BuyerOrderDB buyerOrderDB){
            this.buyerOrderDB = buyerOrderDB;
        }

        @Override
        public void run() {
            try {
                for(int i =0;i<20;i++){
                System.out.println(buyerOrderDB.queryOrder("ap_2f35a41d-1f0e-4c18-a270-b8b23ff64aee"));
                }

            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }
}
