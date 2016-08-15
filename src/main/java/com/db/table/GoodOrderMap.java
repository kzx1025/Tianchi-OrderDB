package com.db.table;

import Frohmd.FrohmdMapBuilder;
import Frohmd.FrohmdMapMultV;
import com.util.BufferedRandomAccessFile;
import com.util.LRULinkedHashMap;
import com.util.PositionManager;
import com.util.StringToLong;

import java.io.File;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by iceke on 16/7/27.
 */
public class GoodOrderMap {
    private FrohmdMapBuilder mapBuilder= null;
    private FrohmdMapMultV queryMap = null;
    private String indexFile;
    private BufferedRandomAccessFile randomAccessFile = null;
    private FileChannel randomAccessFileChannel = null;
    private boolean isMaped = false;
    private List<MappedByteBuffer> mappedByteBuffers = null;
    private int mappedByteBufferSize = Integer.MAX_VALUE - 200000;

   // private LRULinkedHashMap<Long,List<String>> lruMap = new LRULinkedHashMap<>(4000);

    //for test
   // private Map<Long,List<Long>> storeMap = new HashMap<Long,List<Long>>();

    public GoodOrderMap(String indexFile){
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
            long goodPosition =  randomAccessFile.getFilePointer();
            line = randomAccessFile.readLine();
            if (line == null) {
                break;
            }
            pair = line.split("\t");
            //获取goodid
            String goodId = null;

            for(String item:pair){
                if(item.split(":")[0].equals("goodid")){
                    goodId = item.split(":")[1];
                    break;
                }
            }

          /**  if(goodId.equals("good_017cd907-1630-499b-9324-4eccad0ccc6f")){
                j++;
            }**/

            long goodHash = StringToLong.hash(goodId);
            int length = line.length();
            //组合成关于偏移的信息
            long positionRecord = PositionManager.makeRecord(goodPosition,length);

            mapBuilder.putLong(goodHash,positionRecord);


         /**   if(storeMap.containsKey(goodHash)){
                List<Long> temp = storeMap.get(goodHash);
                temp.add(positionRecord);
                storeMap.put(goodHash,temp);
            }else {
                List<Long> initTemp = new ArrayList<>();
                initTemp.add(positionRecord);
                storeMap.put(goodHash, initTemp);
            }**/

            i++;

        }
        System.out.println("总行数:" + i);
        System.out.println("put buyers:" + j);
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

    public List<String> findOrder(String goodId) throws Exception {
        long goodHash = StringToLong.hash(goodId);
        List<String> orders = null;
        //先从缓存取数据
       /** orders = lruMap.get(goodHash);
        if(orders != null && orders.size()!=0){
            //缓存命中
            return orders;
        }**/

        orders = new ArrayList<>();
        List<Long> goodPostions = queryMap.getLong(goodHash);

        if(goodPostions == null || goodPostions.size() == 0){
            return orders;
        }
        //System.out.println("saler's order position size is "+goodPostions.size());
        for(long goodPosition:goodPostions){
            long positionRecord = goodPosition;
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
       // lruMap.put(goodHash,orders);
        return  orders;

    }


  /**  public boolean verified(){
        System.out.println("store map size is "+storeMap.size());
        for(Map.Entry<Long,List<Long>> item:storeMap.entrySet()){
            String buyerHash = Long.toString(item.getKey());
            List<Long> value = item.getValue();
            List<String> tempList = queryMap.getString(buyerHash);
            if(tempList.size() != value.size()){
                return false;
            }else{
                for(int j = 0;j<value.size();j++){
                    if(value.get(j) != Long.parseLong(tempList.get(j))){
                        return false;
                    }
                }
            }
        }
        return true;
    }**/


    public static void main(String args[]){
        try {
            GoodOrderMap goodOrderMap = new GoodOrderMap("data/split/goodOrder.index");
            File file = new File("data/order_record_4g.txt");
            goodOrderMap.load(file);
            goodOrderMap.flush();
            long queryStart = System.currentTimeMillis();
            List<String> result = goodOrderMap.findOrder("good_017cd907-1630-499b-9324-4eccad0ccc6f");
            long queryEnd = System.currentTimeMillis();
            System.out.println(result.size()+","+result);
            System.out.println("time is "+(queryEnd-queryStart));
            /**if(goodOrderMap.verified()){
                System.out.println("succeed");
            }else{
                System.out.println("failed");

            }**/
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
