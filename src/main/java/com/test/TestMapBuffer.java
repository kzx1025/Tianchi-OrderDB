package com.test;

import com.util.BufferedRandomAccessFile;
import com.util.PositionManager;

import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by iceke on 16/7/29.
 */
public class TestMapBuffer {
    public static void main(String args[]){
        try {
            BufferedRandomAccessFile randomAccessFile = new BufferedRandomAccessFile("data/mapbuffer.txt", "r");
             FileChannel randomAccessFileChannel = randomAccessFile.getChannel();
            List<MappedByteBuffer> buffers = new ArrayList<>();
            int mappedByteBufferSize = 10;
            for(long i =0;i<randomAccessFile.length();i+=mappedByteBufferSize){
                MappedByteBuffer mappedByteBuffer ;
                if(i+mappedByteBufferSize>randomAccessFile.length()){
                    mappedByteBuffer = randomAccessFileChannel.map(FileChannel.MapMode.READ_ONLY, i, randomAccessFile.length()-i);
                }else {
                    mappedByteBuffer = randomAccessFileChannel.map(FileChannel.MapMode.READ_ONLY, i,  mappedByteBufferSize);
                }
                buffers.add(mappedByteBuffer);
            }

            int orderLength = 8;
            long orderOffset = 24;


            String orderValue;
            byte[] orderByte = new byte[orderLength];
            int index = (int)(orderOffset/(long)mappedByteBufferSize);
            MappedByteBuffer nowMappedByteBuffer =buffers.get(index);


            int realOffset = (int)(orderOffset - index*mappedByteBufferSize);

            //一条记录跨段
            if(realOffset+orderLength>mappedByteBufferSize){
                MappedByteBuffer nextMappedByteBuffer = buffers.get(index+1);
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
            System.out.println(orderValue);



        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
