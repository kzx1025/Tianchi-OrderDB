package com.db.table;

import com.util.CommonValue;
import com.util.FileSplit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by iceke on 16/7/18.
 */
public class GoodDB {
    private static final int LINES_NUM = CommonValue.ORDER_FILE_SPLIT_LINES;
    private List<String> splitFiles = null;
    private List<GoodTable> goodTables = null;
    private String folder = null;



    //一个商品文件
    public GoodDB(List<String> splitGoodFiles,String folder) {
        this.goodTables = new ArrayList<GoodTable>();
        this.splitFiles =splitGoodFiles;
        this.folder = folder;
    }


    /**
     * 创建索引
     */
    public void createIndex() throws Exception{
        int i = 1;
        for (String splitFile : splitFiles) {
            GoodTable goodTable = new GoodTable(folder+CommonValue.GOOD_INDEX + i);
            File goodFile = new File(splitFile);
            goodTable.load(goodFile);
            goodTable.flush();
            /**GoodTable newTable = new GoodTable(goodTable.getIndexFile());
            newTable.setRandomAccessFile(goodTable.getRandomAccessFile());
            newTable.setFileChannel(goodTable.getFileChannel());
            newTable.setMappedByteBuffer(goodTable.getMappedByteBuffer());**/
            goodTables.add(goodTable);
            i++;
        }

    }

    public String queryGood(String goodId) throws Exception{
        List<String> goodValues = new ArrayList<String>();
        for (GoodTable buyerTable : goodTables) {
            List<String> tempList = buyerTable.findGood(goodId);
            if (tempList.size() != 0) {
                //找到一个便返回
                goodValues.addAll(tempList);
                break;
            }
        }
        if(goodValues.size() == 0){
            return null;
        }else {
            return goodValues.get(0);
        }
    }


    public static void main(String args[]){
        try {
            List<String> goodFiles = new ArrayList<String>();
            goodFiles.add("data/good.0.0");
            goodFiles.add("data/good.1.1");
            goodFiles.add("data/good.2.2");

            GoodDB goodDB = new GoodDB(goodFiles,"data/index/");
            goodDB.createIndex();
            long start = System.currentTimeMillis();
            String result = goodDB.queryGood("dd-a6c4-465e4d7b42b5");
            System.out.println(GoodTable.keySet);

            //List<Integer> result = orderDB.queryOrderPosition(2982386);
            long end = System.currentTimeMillis();
            System.out.println((end - start));
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
