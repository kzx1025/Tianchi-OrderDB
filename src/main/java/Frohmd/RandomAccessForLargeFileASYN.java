package Frohmd;

import java.io.Closeable;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import java.util.ArrayList;
import java.util.List;


/**
 * Allows to do binary search in a possibly very big file
 */
public class RandomAccessForLargeFileASYN implements Closeable {

    private static final long PAGE_SIZE = Integer.MAX_VALUE;
    private List<MappedByteBuffer> buffers = new ArrayList<MappedByteBuffer>();

    private MappedByteBuffer mappedByteBuffer = null;

    File file;
    FileInputStream fs;
    long fileLength = 0;
    FileChannel channel;

    public static void main(String[] args) throws IOException {

    }

    public RandomAccessForLargeFileASYN(File file, int lengthPrefixB64Index) throws IOException {
        this.file = file;
        fs = new FileInputStream(file);
        channel = (fs).getChannel();
        fileLength = file.length();
        mappedByteBuffer = channel.map(MapMode.READ_ONLY, 0, fileLength);
        if(fileLength>Integer.MAX_VALUE){
            throw new IOException("Map fileLength is more than int's max value!!");
        }
    }


    public RandomAccessForLargeFileASYN(File file) throws IOException {
        this(file, 0);
    }

    public byte getByte(long bytePosition) {
        int page = (int) (bytePosition / PAGE_SIZE);
        int index = (int) (bytePosition % PAGE_SIZE);
        return buffers.get(page).get(index);
    }

    public byte[] getBytes(long position, int length) throws ArrayIndexOutOfBoundsException {
        if ((position + length) > fileLength || position < 0) {
            System.out.println(position + length);
            System.out.println(fileLength);
            throw new ArrayIndexOutOfBoundsException("Access out of bounds");
        }
        byte[] answer = new byte[length];

        if (position + length > Integer.MAX_VALUE) {
            throw new ArrayIndexOutOfBoundsException("offset is more than Integer's max value!!!!");
        }
        for (int i = (int) position, j = 0; i < position + length; i++) {
            //只能是int值,会不会超过int最大值
            answer[j] = mappedByteBuffer.get(i);
            j++;
        }

        return answer;
    }


    @Override
    public void close() throws IOException {
        fs.close();
    }


}