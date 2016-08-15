package com.db.table;

import Frohmd.FrohmdMapBuilder;
import Frohmd.FrohmdMapMultV;
import com.db.BplusTreeLongToLong;
import com.util.BufferedRandomAccessFile;
import com.util.LRULinkedHashMap;
import com.util.PositionManager;
import com.util.StringToLong;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by iceke on 16/7/27.
 */
public class BuyerOrderMap {
    private FrohmdMapBuilder mapBuilder= null;
    private FrohmdMapMultV queryMap = null;
    private String indexFile;
    private BufferedRandomAccessFile randomAccessFile = null;
    private FileChannel randomAccessFileChannel = null;
    private boolean isMaped = false;
    private List<MappedByteBuffer> mappedByteBuffers = null;
    private int mappedByteBufferSize = Integer.MAX_VALUE - 200000;

   // private LRULinkedHashMap<Long,List<String>> lruMap = new LRULinkedHashMap<>(4000);

    public BuyerOrderMap(String indexFile){
        this.indexFile = indexFile;
        this.mapBuilder=new FrohmdMapBuilder(indexFile);
    }

    public BufferedRandomAccessFile getRandomAccessFile() {
        return this.randomAccessFile;

    }

    public void setRandomAccessFile(BufferedRandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }


    public FileChannel getRandomAccessFileChannel(){
        return this.randomAccessFileChannel;
    }

    public void setRandomAccessFileChannel(FileChannel randomAccessFileChannel){
        this.randomAccessFileChannel = randomAccessFileChannel;
    }

    public void setMappedByteBuffer(List<MappedByteBuffer> mappedByteBuffers){
        this.mappedByteBuffers = mappedByteBuffers;
    }

    public List<MappedByteBuffer> getMappedByteBuffer(){
        return this.mappedByteBuffers;
    }

    public void setIndexFile(String indexFile) {
        this.indexFile = indexFile;
    }

    public String getIndexFile() {
        return this.indexFile;
    }


    public void load(File file) throws Exception {
        if (!file.exists()) {
            throw new IOException(file + "not found!");
        }

        System.out.println("Loading File....");

        BufferedRandomAccessFile randomAccessFile = new BufferedRandomAccessFile(file, "r");
        this.randomAccessFile = randomAccessFile;

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

            mapBuilder.putLong(buyerHash,positionRecord);

            i++;


        }
        System.out.println("总行数:" + i);
        //System.out.println("put buyers:" + j);
        System.gc();

        System.out.println("Loading Done");
    }

    public void flush()throws IOException{
        mapBuilder.close();
        mapBuilder = null;
        this.queryMap = new FrohmdMapMultV(this.indexFile);

        //定义文件通道
        this.randomAccessFileChannel = this.randomAccessFile.getChannel();
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

    public List<String> findOrder(String buyerId) throws Exception {
        long buyerHash = StringToLong.hash(buyerId);
        List<String> orders = null;
        //先从缓存取数据
        /**orders = lruMap.get(buyerHash);
        if(orders != null && orders.size()!=0){
            //缓存命中
            return orders;
        }**/


        orders = new ArrayList<>();
        List<Long>  buyerPostions = queryMap.getLong(buyerHash);

        if(buyerPostions == null || buyerPostions.size() == 0){
            return orders;
        }

        for(long buyerPosition:buyerPostions){
            long positionRecord = buyerPosition;
            int orderLength = PositionManager.getLength(positionRecord);
            long orderOffset = PositionManager.getOffset(positionRecord);
            String orderValue;
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
            orderValue = new String(orderByte);
            orders.add(orderValue);

        }

        //加入缓存中
       // lruMap.put(buyerHash,orders);

        return  orders;

    }

    public static void main(String args[]){
        try {
            BuyerOrderMap buyerOrderMap = new BuyerOrderMap("data/split/buyerOrder.index");
            File file = new File("data/order_record_4g.txt");
            buyerOrderMap.load(file);
            buyerOrderMap.flush();
            List<String> result = buyerOrderMap.findOrder("ap_2f35a41d-1f0e-4c18-a270-b8b23ff64aee");
            System.out.println(result);
        }catch (Exception e){
            e.printStackTrace();
        }

    }

}
