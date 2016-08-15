package com.db;

/* Code for COMP261 Assignment
 * Name:
 * Usercode:
 * ID:
 */

import com.util.BufferedRandomAccessFile;
import sun.misc.Cleaner;
import sun.nio.ch.DirectBuffer;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * BlockFile Manages a random access file that can only be read and written to
 * as fixed size blocks of bytes. Java hides the details of reading and writing
 * blocks from files, giving the illusion that a file is just a sequence of
 * bytes. This is generally good because it makes Java more platform and OS
 * independent. For efficiency that takes advantage of platform details, it
 * would be more appropriate to use a language like C. This class provides a
 * file that wraps a standard RandomAccessFile in a way that requires the bytes
 * to be read and written in fixed block sizes. simulating what might be
 * provided by a low-level language.
 */

public class BlockFile {

	//bufferrandomaccessfile
	private RandomAccessFile file;
	final int blockSize; // size of blocks
	protected int size = 1; // current number of blocks in file
	private FileChannel fc;
	private boolean isMaped = false;
	private MappedByteBuffer mappedByteBuffer = null;

	/**
	 * Construct a new BlockFile with the given name and blocksize. If the file
	 * does not exist, it will be created, otherwise, it will be opened. A
	 * BlockFile is random access, with both read and write access, but data can
	 * only be read or a written in complete blocks. A block must be an array of
	 * bytes of the block size specified when the file was constructed. A
	 * BlockFile will keep track of the size of the file (in blocks) and can -
	 * read any existing block in the file - write (ie, replace) any existing
	 * block in the file - write a new block to the end of the file
	 */
	public BlockFile(String name, int blockSize) throws IOException {
		this.blockSize = blockSize;
		File f = new File(name);
		file = new RandomAccessFile(f, "rw");
		fc = file.getChannel();
		if (f.exists()) {
			size = (int) (f.length() / blockSize);
			if (f.length() % blockSize != 0) {
				file.setLength(size * blockSize);
			}
		} 
		else {
			size = 1;
		}
	}

	/**
	 * Reads the index'th block from the file.
	 * */
	public byte[] read(int index) throws IOException {
		if (index < 0 || index >= size)
			throw new ArrayIndexOutOfBoundsException(index+size);
		if(!isMaped){
			//System.out.println("blockfile map mappedByteBuffer!!"+file.length());
			mappedByteBuffer = fc.map(FileChannel.MapMode.READ_WRITE, 0, file.length());
			isMaped = true;
		}
		//add by kzx

		byte[] block = new byte[blockSize];
		if(index*blockSize+blockSize>Integer.MAX_VALUE){
			throw new IOException("offset is more than Integer's max value!!!!");
		}
		for(int i = (int)(index*blockSize),j=0;i<(int)(index*blockSize+blockSize);i++) {
			//只能是int值,会不会超过int最大值
			block[j] = mappedByteBuffer.get(i);
			j++;
		}

		return block;
	}

	public static void clean(ByteBuffer bb) {
		if(bb == null) return;
		Cleaner cleaner = ((DirectBuffer) bb).cleaner();
		if(cleaner != null) cleaner.clean();
	}

	/**
	 * Write a block at the end of the file. The block must be of the correct
	 * size
	 */
	public int write(byte[] block) throws IOException {
		if (block.length != blockSize)
			throw new IllegalArgumentException();
		file.seek(size * blockSize);
		file.write(block);
		return size++;
	}

	public void write(byte[] block, int index) throws IOException {
		if (block.length != blockSize)
			throw new IllegalArgumentException();
		if (index < 0 || index > size)
			throw new ArrayIndexOutOfBoundsException(index);
		file.seek(index * blockSize);
		file.write(block);
	}

	public int getSize() {
		return size;
	}

	public void close() throws IOException {
		file.close();
	}

}
