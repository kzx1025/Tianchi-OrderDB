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
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.*;

/**
 * Created by iceke on 16/7/18.
 */
public class GoodTable {
   // private BplusTreeLongToLong goodRecords;

    private String indexFile;

    private BufferedRandomAccessFile randomAccessFile = null;
    private FileChannel fileChannel = null;

    private static int BLOCK_SIZE = 1024;

    private boolean isMaped = false;
    private List<MappedByteBuffer> mappedByteBuffers = null;
    private Map<Long,Long> indexMap = null;
    public static Set<String> keySet = new HashSet<>();
    public static LRULinkedHashMap<Long,OrderSystemImpl.Row> lruGoodMap = new LRULinkedHashMap<>(700000);

    private int mappedByteBufferSize = Integer.MAX_VALUE - 200000;


    public GoodTable(String indexFile) {
        this.indexFile = indexFile;
        indexMap = new HashMap<>();
       // goodRecords = new BplusTreeLongToLong(this.indexFile, BLOCK_SIZE);
    }


    public BufferedRandomAccessFile getRandomAccessFile() {
        return this.randomAccessFile;

    }

    public void setRandomAccessFile(BufferedRandomAccessFile randomAccessFile) {
        this.randomAccessFile = randomAccessFile;
    }

    public FileChannel getFileChannel() {
        return this.fileChannel;
    }

    public void setFileChannel(FileChannel fileChannel) {
        this.fileChannel = fileChannel;
    }

    public List<MappedByteBuffer> getMappedByteBuffer() {
        return this.mappedByteBuffers;
    }

    public void setMappedByteBuffer(List<MappedByteBuffer> mappedByteBuffers) {
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
        String line;
        String[] pair;
        int lines = 0;
        while (true) {
            long goodPosition = randomAccessFile.getFilePointer();
            line = randomAccessFile.readLine();
            if (line == null) {
                break;
            }
            pair = line.split("\t");
            String goodId = null;
            for(String item:pair){
                String key = item.split(":")[0];
                if(key.equals("goodid")){
                    goodId = item.split(":")[1];
                    //break;
                }else{
                    if(!keySet.contains(key)) {
                        keySet.add(key);
                    }
                }
            }
            int length = line.length();

            //进行合成
            long goodRecord = PositionManager.makeRecord(goodPosition, length);
            indexMap.put(StringToLong.hash(goodId), goodRecord);

            if(lruGoodMap.size()<400000){
                lruGoodMap.put(StringToLong.hash(goodId),OrderSystemImpl.String2Row(new String(line.getBytes("ISO-8859-1"), "utf-8")));
            }

            //注意这里可能存在hash碰撞问题
           /** goodRecords.put(StringToLong.hash(goodId), goodRecord);
            if(lines == 2000000){
                goodRecords.flush();
                //旧对象释放 新对象生成
                goodRecords = new BplusTreeLongToLong(this.indexFile, BLOCK_SIZE);
                lines = 0;
            }**/
            lines++;
            i = i + 1;
        }
        System.out.println("总行数:" + i);

        System.out.println("Loading Done");

    }


    public void flush() throws IOException {
        //goodRecords.flush();

        //映射buffer
        //加载通道
        this.fileChannel = randomAccessFile.getChannel();
        mappedByteBuffers = new ArrayList<>();
        for (long i = 0; i < randomAccessFile.length(); i += mappedByteBufferSize) {
            MappedByteBuffer mappedByteBuffer;
            if (i + mappedByteBufferSize > randomAccessFile.length()) {
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, i, randomAccessFile.length() - i);
            } else {
                mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_ONLY, i, mappedByteBufferSize);
            }
            mappedByteBuffers.add(mappedByteBuffer);
        }

        isMaped = true;

    }

    public List<String> findGood(String goodId) throws Exception {

        Long position = indexMap.get(StringToLong.hash(goodId));
        List<String> goodValues = new ArrayList<String>();
        if(position == null){
            return goodValues;
        }
        List<Long> goodPositions = new ArrayList<>();
        goodPositions.add(position);
        if (goodPositions == null || goodPositions.size() == 0) {
            return goodValues;
        }
        for (long goodPosition : goodPositions) {
            String goodValue;
            int length = PositionManager.getLength(goodPosition);
            long offset = PositionManager.getOffset(goodPosition);

            byte[] record = new byte[length];
            int index = (int) (offset / (long) mappedByteBufferSize);
            MappedByteBuffer nowMappedByteBuffer = mappedByteBuffers.get(index);


            int realOffset = (int) (offset - index * mappedByteBufferSize);

            //一条记录跨段
            if (realOffset + length > mappedByteBufferSize) {
                MappedByteBuffer nextMappedByteBuffer = mappedByteBuffers.get(index + 1);
                //截取到最后一段
                int j = 0;
                for (int i = realOffset; i < mappedByteBufferSize; i++) {
                    record[j] = nowMappedByteBuffer.get(i);
                    j++;
                }

                for (int i = 0; j < length; j++, i++) {
                    record[j] = nextMappedByteBuffer.get(i);
                }


            } else {//不跨段
                for (int i = realOffset, j = 0; i < realOffset + length; i++) {
                    record[j] = nowMappedByteBuffer.get(i);
                    j++;
                }
            }
            goodValue = new String(record);

            goodValues.add(goodValue);

        }

        return goodValues;


    }
}
