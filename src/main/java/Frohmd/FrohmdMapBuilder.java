package Frohmd;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.Closeable;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.util.LongBytes;

public class FrohmdMapBuilder implements Closeable{
	int logNbBuckets = 5; // meaning the number of buckets is 2^5=32
	int nbBuckets = (int) Math.pow(2, logNbBuckets);
	HashFunction hashFunc=Hashing.murmur3_128();
	OutputStream os_datastore;
	OutputStream[] os_index;
	long currentPositionInDatastore=0L;
	long nbKeys=0L;
	public static final Charset charset=Charset.forName("UTF-8");
	String path;
	
	
	public FrohmdMapBuilder(String path) {
		this.path=path;
		try{
			os_datastore=new BufferedOutputStream(new FileOutputStream(path+".data"));
			os_index=new OutputStream[nbBuckets];
			for (int i=0; i<nbBuckets; i++)
				os_index[i]=new BufferedOutputStream(new FileOutputStream(path+".index_tmp"+i));
		}catch(IOException ioe){
			ioe.printStackTrace();
		}
	}
	
	public void put(String key, String data) throws IOException{
		byte[] b_key=key.getBytes(charset);
		byte[] b_data=data.getBytes(charset);
		put(b_key,b_data);
	}

	public void putLong(long key,long data) throws IOException{

		put(LongBytes.long2Bytes(key),LongBytes.long2Bytes(data));
	}
	
	public synchronized void put(byte[] key, byte[] data) throws IOException{
		long hash=hashFunc.hashBytes(key).asLong();
		int bucketId= getBucketorSlotId(hash, logNbBuckets, nbBuckets);
		int lengthData=data.length;
		os_datastore.write(data);
		
		os_index[bucketId].write(IndexLine.toLine(hash, currentPositionInDatastore, lengthData));
		currentPositionInDatastore+=lengthData;
		nbKeys++;
	}
	
	
	private void closeOS(){
		try {
			os_datastore.close();
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		for (int i=0; i<nbBuckets; i++){
			try {
				os_index[i].close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	
	
	private void sortIndex(){
		int logNbSlots=Math.max(logNbBuckets,(int) (Math.log(Math.max(1,1.0*nbKeys/30))/Math.log(2)));
		int nbSlots=(int) Math.pow(2, logNbSlots);
		OutputStream io_index=null, io_headIndex=null;
		try {
			io_index=new BufferedOutputStream(new FileOutputStream(path+".indexBody"));
			io_headIndex=new BufferedOutputStream(new FileOutputStream(path+".indexHead"));
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		}
		
		long currentSlotId=0L;
		long positionSlotInIndexBody=0L;
		long currentPositionInIndexBody=0L;
		int nbbytesInCurrentSlot=0;
		for (int i=0; i<nbBuckets; i++){
			File f=new File(path+".index_tmp"+i);
			try {
				InputStream is=new BufferedInputStream(new FileInputStream(f));
				List<IndexLine> allLines=new ArrayList<IndexLine>();
				byte[] buffer=new byte[20];
				int nbByteRead=is.read(buffer);
				while(nbByteRead!=-1){
					allLines.add(new IndexLine(buffer));
					nbByteRead=is.read(buffer);
				}
				is.close();
				Collections.sort(allLines, new hashCompare());
						
				for (IndexLine line : allLines){
					int slotId=getBucketorSlotId(line.hash, logNbSlots, nbSlots);
					while(currentSlotId<slotId){
						io_headIndex.write(IndexLine.toLine(currentSlotId, positionSlotInIndexBody, nbbytesInCurrentSlot));
						currentSlotId++;
						nbbytesInCurrentSlot=0;
						positionSlotInIndexBody=currentPositionInIndexBody;
					}
					io_index.write(IndexLine.toLine(line.hash, line.position, line.length));
					nbbytesInCurrentSlot+=20;
					currentPositionInIndexBody+=20;
				}
				f.delete();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		while(currentSlotId<nbSlots){
			try {
				io_headIndex.write(IndexLine.toLine(currentSlotId, positionSlotInIndexBody, nbbytesInCurrentSlot));
			} catch (IOException e) {
				e.printStackTrace();
			}
			currentSlotId++;
			nbbytesInCurrentSlot=0;
			positionSlotInIndexBody=currentPositionInIndexBody;
		}
		try {
			io_headIndex.close();
			io_index.close();
			DataOutputStream dos=new DataOutputStream(new FileOutputStream(path+".mapProperties"));
			dos.writeInt(logNbSlots);
			dos.writeLong(nbSlots);
			dos.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static int getBucketorSlotId(long hash, int logNb, long nb){
		int bucketId= (int) (hash >> (64-logNb));
		return (int) (bucketId+(nb/2));
	}


	@Override
	public void close() throws IOException {
		System.out.println("closing the map, preparing the index.");
		closeOS();
		System.out.println("starting to sort the index");
		sortIndex();
		System.out.println("done.");
		
	}
	
	public static void printIndexFile(String path){
		try{
			InputStream is=new BufferedInputStream(new FileInputStream(path));
			byte[] buffer=new byte[20];
			int nbByteRead=is.read(buffer);
			while(nbByteRead!=-1){
				IndexLine il=new IndexLine(buffer);
				System.out.println(il.hash+","+il.position+","+il.length);
				nbByteRead=is.read(buffer);
			}
			is.close();
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	public static void main(String[] args) throws IOException{
		FrohmdMapBuilder fmb=new FrohmdMapBuilder("testIndex");
		for (int i=0; i<200_000_000; i++){
			fmb.put("key"+i, "This is the value (and it is quite a very very very long value) for the key. "+i);
		}
		fmb.close();
	}


}
class  hashCompare implements Comparator<IndexLine>{
	@Override
	public int compare(IndexLine o1, IndexLine o2) {
		return Long.compare(o1.hash,o2.hash);
	}
}