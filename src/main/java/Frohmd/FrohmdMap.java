package Frohmd;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Random;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

/**
 * fast read only disk map
 */


public class FrohmdMap implements Closeable{
	
	private static final Charset charset=Charset.forName("UTF-8");
	private  RandomAccessForLargeFileASYN accessIndexHead;
	private RandomAccessForLargeFileASYN accessIndexBody;
	private RandomAccessForLargeFileASYN accessData;
	private long nbSlots;
	private int logNbSlots;
	private HashFunction hashFunc=Hashing.murmur3_128();

	public FrohmdMap(String path) throws IOException {
		accessData=new RandomAccessForLargeFileASYN(new File(path+".data"));
		accessIndexBody=new RandomAccessForLargeFileASYN(new File(path+".indexBody"));
		accessIndexHead=new RandomAccessForLargeFileASYN(new File(path+".indexHead"));
		
		DataInputStream dis=new DataInputStream(new FileInputStream(path+".mapProperties"));
		logNbSlots=dis.readInt();
		nbSlots=dis.readLong();
		dis.close();
	}
	
	
	
	public String getString(String key){
		byte[] b_key=key.getBytes(charset);
		byte[] b_data=get(b_key);
		if (b_data==null)
			return null;
		String s = new String(b_data, charset);
		return s;
	}
	
	
	public synchronized byte[] get(byte[] key){
		long hash=hashFunc.hashBytes(key).asLong();
		long positionInIndexHead=FrohmdMapBuilder.getBucketorSlotId(hash, logNbSlots, nbSlots);
		IndexLine headLine=new IndexLine(accessIndexHead.getBytes(positionInIndexHead*20, 20));
		
		byte[] slot=accessIndexBody.getBytes(headLine.position, headLine.length);
		//System.out.println(slot.length+"<-slot");
		IndexLine indexBodyLine=null;
		for (int i=0; i<slot.length; i+=20){
			byte[] buffer=Arrays.copyOfRange(slot, i, i+20);
			IndexLine line=new IndexLine(buffer);
			if (line.hash==hash){
				indexBodyLine=line;
				break;
			}
		}
		if (indexBodyLine==null)
			return null;
		
		byte[] data=accessData.getBytes(indexBodyLine.position, indexBodyLine.length);
		return data;
	}
	
	
	@Override
	public void close() throws IOException  {
		accessData.close();
		accessIndexBody.close();
		accessIndexHead.close();
	}
	
	public static void main(String[] args) throws IOException {
		FrohmdMap map=new FrohmdMap("testIndex");
		long start=System.nanoTime();
		Random rand=new Random();
		int nbError=0;
		for (int i=0; i<100000; i++){
			int ri=rand.nextInt(200_000_000);
			String s=map.getString("key"+ri);
			if (!s.endsWith(String.valueOf(ri)))
					nbError++;
		}
		
		System.out.println("10,000 random reads in "+(System.nanoTime()-start)/1e6+"ms");
		System.out.println(nbError);
		map.close();
	}
	
	
}
