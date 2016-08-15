package com.db.table;

import com.db.*;
import com.db.bplustree.BPlusTreeFile;
import com.util.BufferedRandomAccessFile;
import com.util.CommonValue;
import com.util.PositionManager;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by iceke on 16/7/15.
 */
public class OrderTable {

    private BplusTreeLongToLong orderRecords;

    public static final String ORDER_INDEX = CommonValue.ORDER_INDEX;
    private String indexFile;

    private BufferedRandomAccessFile randomAccessFile = null;
    private static int BLOCK_SIZE = 1024;
    private FileChannel fileChannel = null;
    private boolean isMaped = false;
    private List<MappedByteBuffer> mappedByteBuffers = null;
    private int mappedByteBufferSize = Integer.MAX_VALUE - 200000;
   // private int mappedByteBufferSize = 20000;



    public OrderTable(String indexFile) {
        this.indexFile = indexFile;
        orderRecords = new BplusTreeLongToLong(this.indexFile, BLOCK_SIZE);

    }


    public BufferedRandomAccessFile getRandomAccessFile() {
        return this.randomAccessFile;

    }

    public void setRandomAccessFile(BufferedRandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    public FileChannel getFileChannel(){
        return this.fileChannel;
    }

    public void setFileChannel(FileChannel fileChannel){
        this.fileChannel = fileChannel;
    }

    public List<MappedByteBuffer> getMappedByteBuffer(){
        return this.mappedByteBuffers;
    }
    public void setMappedByteBuffer(List<MappedByteBuffer> mappedByteBuffers){
        this.mappedByteBuffers = mappedByteBuffers;
    }

    public void setIndexFile(String indexFile) {
        this.indexFile = indexFile;
    }

    public String getIndexFile() {
        return this.indexFile;
    }


    public void load(File file) throws Exception {
        if (!file.exists()) {
            System.out.println(file + "not found!");
            return;
        }

        System.out.println("Loading....");


        BufferedRandomAccessFile randomAccessFile = new BufferedRandomAccessFile(file, "r");
        this.randomAccessFile = randomAccessFile;



        if (new File(indexFile).length() > BLOCK_SIZE) {
            System.out.println("Tree found on disk. No need to reconstruct");
            return;
        }

        int i = 0;
        String line = null;
        String[] pair = null;
        int lines = 0;
        while (true) {
            long orderPosition =  randomAccessFile.getFilePointer();
            line = randomAccessFile.readLine();
            if (line == null) {
                break;
            }
            pair = line.split("\t");
            long orderId = 0L;
            for(String item:pair){
                if(item.split(":")[0].equals("orderid")){
                    orderId = Long.parseLong(item.split(":")[1]);
                    break;
                }
            }
            int length = line.length();
            //进行合成
            long orderRecord = PositionManager.makeRecord(orderPosition,length);

            orderRecords.put(orderId, orderRecord);
            if(lines == 3000000){
                orderRecords.flush();
                //旧对象释放 新对象生成
                orderRecords = new BplusTreeLongToLong(this.indexFile, BLOCK_SIZE);
                lines = 0;
            }
            // System.out.println("i:" + i + " position:" + orderPosition);
            i++;
            lines++;


        }
        System.out.println("总行数:" + i);

        System.out.println("Loading Done");
    }

    public void flush() throws IOException {
        orderRecords.flush();

        //映射buffer
        fileChannel = randomAccessFile.getChannel();
        mappedByteBuffers = new ArrayList<>();
        for(long i =0;i<randomAccessFile.length();i+=mappedByteBufferSize){
            MappedByteBuffer mappedByteBuffer = null;
            if(i+mappedByteBufferSize>randomAccessFile.length()){
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, i, randomAccessFile.length()-i);
            }else {
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, i,  mappedByteBufferSize);
            }
            mappedByteBuffers.add(mappedByteBuffer);
        }

        isMaped = true;

    }

    public List<String> findOrderId(long orderId) throws Exception {

        List<Long> orderPositions = orderRecords.find(orderId);
        List<String> orderValues = new ArrayList<String>();
        if (orderPositions == null || orderPositions.size() == 0) {
            return orderValues;
        }
        for (long orderPosition : orderPositions) {
            String orderValue = null;
            int length = PositionManager.getLength(orderPosition);
            long offset = PositionManager.getOffset(orderPosition);

            byte[] record = new byte[length];
            int index = (int)(offset/(long)mappedByteBufferSize);
            MappedByteBuffer nowMappedByteBuffer = mappedByteBuffers.get(index);


            int realOffset = (int)(offset - index*mappedByteBufferSize);

            //一条记录跨段
            if(realOffset+length>mappedByteBufferSize){
                MappedByteBuffer nextMappedByteBuffer = mappedByteBuffers.get(index+1);
                //截取到最后一段
                int j = 0;
                for(int i = realOffset;i<mappedByteBufferSize;i++){
                    record[j] = nowMappedByteBuffer.get(i);
                    j++;
                }

                for(int i = 0;j<length;j++,i++){
                    record[j] = nextMappedByteBuffer.get(i);
                }

            }else {//不跨段
                for (int i = realOffset, j = 0; i < realOffset + length; i++) {
                    record[j] = nowMappedByteBuffer.get(i);
                    j++;
                }
            }
            orderValue = new String(record);
            orderValues.add(orderValue);
        }

        return orderValues;

    }



}
