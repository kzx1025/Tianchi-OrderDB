package com.db.table;

import com.db.*;
import com.util.BufferedRandomAccessFile;
import com.util.PositionManager;
import com.util.StringToLong;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by iceke on 16/7/16.
 */
public class BuyerOrderTable {
    private BplusTreeLongToLong buyerOrderRecords;

    private String indexFile;

    private BufferedRandomAccessFile randomAccessFile = null;
    private BufferedRandomAccessFile positionFile = null;
    private FileChannel randomAccessFileChannel = null;
    private FileChannel positionFileChannel = null;

    private boolean isMaped = false;
    private boolean isPositionMaped = false;
    private List<MappedByteBuffer> mappedByteBuffers = null;
    private MappedByteBuffer positionMappedByteBuffer = null;
    private int mappedByteBufferSize = Integer.MAX_VALUE - 200000;


    private Map<Long, List<Long>> indexMap = null;

    private static int BLOCK_SIZE = 5240;


    public BuyerOrderTable(String indexFile) {
        this.indexFile = indexFile;
        buyerOrderRecords = new BplusTreeLongToLong(this.indexFile, BLOCK_SIZE);
        indexMap = new HashMap<>();
    }

    public BufferedRandomAccessFile getRandomAccessFile() {
        return this.randomAccessFile;

    }

    public void setRandomAccessFile(BufferedRandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }


    public BufferedRandomAccessFile getPositionFile(){
        return this.positionFile;
    }

    public void setPositionFile(BufferedRandomAccessFile positionFile){
        this.positionFile = positionFile;
    }

    public FileChannel getRandomAccessFileChannel(){
        return this.randomAccessFileChannel;
    }

    public void setRandomAccessFileChannel(FileChannel randomAccessFileChannel){
        this.randomAccessFileChannel = randomAccessFileChannel;
    }

    public FileChannel getPositionFileChannel(){
        return this.positionFileChannel;
    }

    public void setPositionFileChannel(FileChannel positionFileChannel){
        this.positionFileChannel = positionFileChannel;
    }

    public List<MappedByteBuffer> getMappedByteBuffer(){
        return this.mappedByteBuffers;
    }
    public MappedByteBuffer getPositionMappedByteBuffer(){
        return this.positionMappedByteBuffer;
    }

    public void setMappedByteBuffer(List<MappedByteBuffer> mappedByteBuffers){
        this.mappedByteBuffers = mappedByteBuffers;
    }

    public void setPositionMappedByteBuffer(MappedByteBuffer positionMappedByteBuffer){
        this.positionMappedByteBuffer = positionMappedByteBuffer;
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
        this.positionFile = new BufferedRandomAccessFile(indexFile + ".position", "rw");



        if (new File(indexFile).length() > BLOCK_SIZE) {
            System.out.println("Tree found on disk. No need to reconstruct");
            return;
        }

        int i = 0;
        String line = null;
        String[] pair = null;
        int j = 0;
        while (true) {
            long buyerPosition =  randomAccessFile.getFilePointer();
            line = randomAccessFile.readLine();
            if (line == null) {
                break;
            }
            pair = line.split("\t");
            //获取buyerid
            String buyerId = null;

            for(String item:pair){
                if(item.split(":")[0].equals("buyerid")){
                    buyerId = item.split(":")[1];
                    break;
                }
            }

            long buyerHash = StringToLong.hash(buyerId);
            int length = line.length();
            //组合成关于偏移的信息
            long positionRecord = PositionManager.makeRecord(buyerPosition,length);
            //该行长度与偏移量用&符号相隔
            if (indexMap.containsKey(buyerHash)) {
                List<Long> oldValue = indexMap.get(buyerHash);
                oldValue.add(positionRecord);
                indexMap.put(buyerHash, oldValue);
            } else {
                List<Long> positions = new ArrayList<>();
                positions.add(positionRecord);
                indexMap.put(buyerHash, positions);
            }
            i++;


        }
        System.out.println("总行数:" + i);
        //System.out.println("put buyers:" + j);
        System.gc();

        System.out.println("Loading Done");
    }


    public void flush() throws IOException {
        //写入B+树 同时将位置写入文件
        long position = 0;
        StringBuilder sb = new StringBuilder();
        //一般buyer 200w 到 400w之间
        int lines = 0;
        System.out.println("BuyerOrderTable flush indexMap size is "+indexMap.size());
        for (Map.Entry item : indexMap.entrySet()) {
            List<Long> positions = (List<Long>)item.getValue();
            for(long tempPosition:positions){
                sb.append(tempPosition).append("#");
            }
            String value = sb.toString();
            int length = value.length();
            if(length > 8000000){
                throw new IOException("flush: value's length bigger than 30000");
            }
            //record整合了偏移量和该行的长度 但长度有限制,可以进行改动
            long record = PositionManager.makeRecord(position,length);
            positionFile.writeBytes(value);
            position+=length;
            //positionFile.writeUTF((String) item.getValue());
            buyerOrderRecords.put((long) item.getKey(), record);
            sb.setLength(0);
            if(lines ==400000){
                buyerOrderRecords.flush();
                buyerOrderRecords = new BplusTreeLongToLong(this.indexFile, BLOCK_SIZE);
                lines = 0;
                System.gc();
            }
            lines++;
        }

        indexMap.clear();
        buyerOrderRecords.flush();


        //定义文件通道
        this.randomAccessFileChannel = this.randomAccessFile.getChannel();
        this.positionFileChannel = this.positionFile.getChannel();

        //生成bytebuffer
        if(positionFile.length()>Integer.MAX_VALUE - 200000){
            throw new IOException("positionFile is bigger than buffersize");
        }
        //可能长度超过int值
        positionMappedByteBuffer = positionFileChannel.map(FileChannel.MapMode.READ_ONLY,0,positionFile.length());
        isPositionMaped = true;

        //映射buffer
        mappedByteBuffers = new ArrayList<>();
        for(long i =0;i<randomAccessFile.length();i+=mappedByteBufferSize){
            MappedByteBuffer mappedByteBuffer ;
            if(i+mappedByteBufferSize>randomAccessFile.length()){
                mappedByteBuffer = randomAccessFileChannel.map(FileChannel.MapMode.READ_ONLY, i, randomAccessFile.length()-i);
            }else {
                mappedByteBuffer = randomAccessFileChannel.map(FileChannel.MapMode.READ_ONLY, i,  mappedByteBufferSize);
            }
            mappedByteBuffers.add(mappedByteBuffer);
        }
    }

    /**
     * 可以查到很多个order记录
     * 不得不说这里面的操作很繁杂,代码很冗长,等待重构
     *
     * @param buyerId
     * @return
     * @throws Exception
     */
    public List<String> findOrder(String buyerId) throws Exception {

        List<Long> positions = buyerOrderRecords.find(StringToLong.hash(buyerId));
        List<String> buyerValues = new ArrayList<>();
        //肯定是唯一的
        if (positions == null || positions.size() == 0) {
            return buyerValues;
        }
        long record = positions.get(0);
        long offset = PositionManager.getOffset(record);
        int length = PositionManager.getLength(record);
        String buyerPositions = null;
       // ByteBuffer buffer = ByteBuffer.allocate(length);
       // positionFileChannel.read(buffer,offset);
        //buffer.flip();
        byte[] positionByte = new byte[length];
        //buffer.get(positionByte);
        for(int i = (int)offset,j=0;i<(int)offset+length;i++) {
            positionByte[j] = positionMappedByteBuffer.get(i);
            j++;
        }
        buyerPositions = new String(positionByte);


        for (String buyerPosition : buyerPositions.split("#")) {
            long positionRecord = Long.parseLong(buyerPosition);
            int orderLength = PositionManager.getLength(positionRecord);
            long orderOffset = PositionManager.getOffset(positionRecord);
            String buyerValue;
            byte[] orderByte = new byte[orderLength];
            int index = (int)(orderOffset/(long)mappedByteBufferSize);
            MappedByteBuffer nowMappedByteBuffer = mappedByteBuffers.get(index);


            int realOffset = (int)(orderOffset - index*mappedByteBufferSize);

            //一条记录跨段
            if(realOffset+orderLength>mappedByteBufferSize){
                MappedByteBuffer nextMappedByteBuffer = mappedByteBuffers.get(index+1);
                int j = 0;
                int m = 0;
                for(int i = realOffset;i<mappedByteBufferSize;i++){
                    orderByte[j] = nowMappedByteBuffer.get(i);
                    j++;
                }

                for(int i = 0;j<orderLength;j++,i++){
                    orderByte[j] = nextMappedByteBuffer.get(i);
                }

            }else {//不跨段
                for (int i = realOffset, j = 0; i < realOffset + orderLength; i++) {
                    orderByte[j] = nowMappedByteBuffer.get(i);
                    j++;
                }
            }

            //可能需要编码
            buyerValue = new String(orderByte);

            buyerValues.add(buyerValue);
        }

        return buyerValues;

    }


}
