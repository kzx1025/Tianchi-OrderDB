package com.db.table;

import com.util.CommonValue;
import com.util.FileSplit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by iceke on 16/7/18.
 */
public class BuyerDB {
    private static final int LINES_NUM = CommonValue.ORDER_FILE_SPLIT_LINES;
    private List<String> splitFiles = null;
    private List<BuyerTable> buyerTables = null;
    private String folder = null;

    //一个关于买家的文件
    public BuyerDB(List<String> splitBuyerFiles,String folder) {
        this.buyerTables = new ArrayList<BuyerTable>();
        this.splitFiles = splitBuyerFiles;
        this.folder = folder;
    }


    public void buildIndex() throws Exception{
        BuyerTable buyerTable = new BuyerTable(folder+CommonValue.BUYER_INDEX);
    }


    public void createIndex() throws Exception{

        int i = 1;
        for (String splitFile : splitFiles) {
            BuyerTable buyerTable = new BuyerTable(folder+CommonValue.BUYER_INDEX + i);
            File buyerFile = new File(splitFile);
            buyerTable.load(buyerFile);
            buyerTable.flush();
            //重新包装索引
           /** BuyerTable newTable = new BuyerTable(buyerTable.getIndexFile());
            newTable.setRandomAccessFile(buyerTable.getRandomAccessFile());
            newTable.setFileChannel(buyerTable.getFileChannel());
            newTable.setMappedByteBuffer(buyerTable.getMappedByteBuffer());**/
            buyerTables.add(buyerTable);
            i++;
        }

    }


    public String queryBuyer(String buyerId) throws Exception {
        List<String> buyerValues = new ArrayList<String>();
        for (BuyerTable buyerTable : buyerTables) {
            List<String> tempList = buyerTable.findBuyer(buyerId);
            if (tempList.size() != 0) {
                //一条记录,找到便返回
                buyerValues.addAll(tempList);
                break;
            }
        }
        if(buyerValues.size() == 0){
            return  null;
        }else {
            return buyerValues.get(0);
        }
    }


    public static void main(String args[]){
        try {
            List<String> buyerFiles = new ArrayList<String>();
            buyerFiles.add("data/buyer.0.0");
            buyerFiles.add("data/buyer.1.1");

            FileSplit.mergerFiles(buyerFiles.toArray(new String[2]),"data/buyer_merge");
            List<String> rsultFiles = new ArrayList<>();
            rsultFiles.add("data/buyer_merge");

            BuyerDB buyerDB = new BuyerDB(rsultFiles,"data/index/");
            buyerDB.createIndex();
            long start = System.currentTimeMillis();
            String result = buyerDB.queryBuyer("ap-9523-f8d17a93670c");
            String result2 = buyerDB.queryBuyer("tp-a471-423534e4bccb");
            System.out.println(BuyerTable.keySet);

            //List<Integer> result = orderDB.queryOrderPosition(2982386);
            long end = System.currentTimeMillis();
            System.out.println((end - start));
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}
