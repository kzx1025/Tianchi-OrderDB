package com.db.table;

import com.alibaba.middleware.race.OrderSystemImpl;
import com.db.BPlusTreeString60toInt;
import com.db.BplusTreeLongToInt;
import com.db.BplusTreeLongToLong;
import com.db.BplusTreeStringToString20;
import com.db.bplustree.BPlusTreeFile;
import com.util.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by iceke on 16/7/17.
 */
public class BuyerTable {
   // private BplusTreeLongToLong buyerRecords;

    private String indexFile;

    private BufferedRandomAccessFile randomAccessFile = null;
    private FileChannel fileChannel = null;
    private static int BLOCK_SIZE = 1024;

    private boolean isMaped = false;
    private List<MappedByteBuffer> mappedByteBuffers = null;
    private int mappedByteBufferSize = Integer.MAX_VALUE - 200000;
    private Map<Long,Long> indexMap = null;
    public static Set<String> keySet = new HashSet<>();

    public static LRULinkedHashMap<Long,OrderSystemImpl.Row> lruBuyerMap = new LRULinkedHashMap<>(900000);



    public BuyerTable(String indexFile) {
        this.indexFile = indexFile;
      //  buyerRecords = new BplusTreeLongToLong(this.indexFile, BLOCK_SIZE);
        indexMap = new HashMap<>();
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

        System.out.println("Loading File....");

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
            long buyerPosition =  randomAccessFile.getFilePointer();
            line = randomAccessFile.readLine();
            if (line == null) {
                break;
            }
            pair = line.split("\t");
            String buyerId = null;
            int length = line.length();
            for(String item:pair){
                String key = item.split(":")[0];
                if(key.equals("buyerid")){
                    buyerId = item.split(":")[1];
                    //break;
                }else{
                    if(!keySet.contains(key)) {
                        keySet.add(key);
                    }
                }
            }

            long buyerRecord = PositionManager.makeRecord(buyerPosition,length);
            indexMap.put(StringToLong.hash(buyerId), buyerRecord);

            if(lruBuyerMap.size()<500000){
                lruBuyerMap.put(StringToLong.hash(buyerId),OrderSystemImpl.String2Row(new String(line.getBytes("ISO-8859-1"), "utf-8")));
            }

            //可能存在hash碰撞问题
           /** buyerRecords.put(StringToLong.hash(buyerId), buyerRecord);
            if(lines == 2000000){
                buyerRecords.flush();
                //旧对象释放 新对象生成
                buyerRecords = new BplusTreeLongToLong(this.indexFile, BLOCK_SIZE);
                lines = 0;
            }**/
            lines++;
            i++;
        }
        System.out.println("总行数:" + i);
        System.out.println("Loading Done");

    }


    public void flush() throws IOException {
       // buyerRecords.flush();

        //映射buffer
        this.fileChannel = randomAccessFile.getChannel();
        mappedByteBuffers = new ArrayList<>();
        for(long i =0;i<randomAccessFile.length();i+=mappedByteBufferSize){
            MappedByteBuffer mappedByteBuffer ;
            if(i+mappedByteBufferSize>randomAccessFile.length()){
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, i, randomAccessFile.length()-i);
            }else {
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, i,  mappedByteBufferSize);
            }
            mappedByteBuffers.add(mappedByteBuffer);
        }

    }

    /**
     * 返回的list size 为1 或0
     *
     * @param buyerId
     * @return
     * @throws Exception
     */
    public List<String> findBuyer(String buyerId) throws Exception {
        Long position = indexMap.get(StringToLong.hash(buyerId));
        List<String> buyerValues = new ArrayList<String>();
        if(position == null){
            return buyerValues;
        }
        List<Long> buyerPositions = new ArrayList<>();
        buyerPositions.add(position);

        if (buyerPositions == null || buyerPositions.size() == 0) {
            return buyerValues;
        }
        for (long buyerPosition : buyerPositions) {
            String buyerValue = null;
            int length = PositionManager.getLength(buyerPosition);
            long offset = PositionManager.getOffset(buyerPosition);
            byte[] record = new byte[length];
            int index = (int)(offset/(long)mappedByteBufferSize);
            MappedByteBuffer nowMappedByteBuffer = mappedByteBuffers.get(index);


            int realOffset = (int)(offset - index*mappedByteBufferSize);

            //一条记录跨段
            if(realOffset+length>mappedByteBufferSize){
                MappedByteBuffer nextMappedByteBuffer = mappedByteBuffers.get(index+1);
                int j = 0;
                int m = 0;
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
            buyerValue = new String(record);

            if (buyerValue != null) {
                //就一条记录,找到便返回
                buyerValues.add(buyerValue);
            }
        }

        return buyerValues;


    }
}
