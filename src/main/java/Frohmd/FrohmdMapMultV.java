package Frohmd;

import java.io.Closeable;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.*;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.util.LongBytes;

/**
 * fast read only disk map
 */


public class FrohmdMapMultV implements Closeable{
	
	private static final Charset charset=Charset.forName("UTF-8");
	private  RandomAccessForLargeFileASYN accessIndexHead;
	private RandomAccessForLargeFileASYN accessIndexBody;
	private RandomAccessForLargeFileASYN accessData;
	private long nbSlots;
	private int logNbSlots;
	private HashFunction hashFunc=Hashing.murmur3_128();

	public FrohmdMapMultV(String path) throws IOException {
		accessData=new RandomAccessForLargeFileASYN(new File(path+".data"));
		accessIndexBody=new RandomAccessForLargeFileASYN(new File(path+".indexBody"));
		accessIndexHead=new RandomAccessForLargeFileASYN(new File(path+".indexHead"));
		
		DataInputStream dis=new DataInputStream(new FileInputStream(path+".mapProperties"));
		logNbSlots=dis.readInt();
		nbSlots=dis.readLong();
		dis.close();
	}


	public List<Long> getLong(long key){
		byte[] b_key= LongBytes.long2Bytes(key);
		List<byte[]> b_data_list=get(b_key);
		if (b_data_list==null)
			return null;
		List<Long> sList=new Vector<>();
		for (byte[] b_data:b_data_list) {
			sList.add(LongBytes.bytes2Long(b_data));
		}
		return sList;

	}
	
	public List<String> getString(String key){
		byte[] b_key=key.getBytes(charset);
		//byte[] b_data=get(b_key);
		List<byte[]> b_data_list=get(b_key);
		if (b_data_list==null)
			return null;
		List<String> sList=new Vector<>();
		for (byte[] b_data:b_data_list) {
			//String s = new String(b_data, charset);
			sList.add(new String(b_data,charset));
		}
		//return s;
		return sList;
	}
	
	
	public  List<byte[]> get(byte[] key){
		long hash=hashFunc.hashBytes(key).asLong();
		long positionInIndexHead=FrohmdMapBuilder.getBucketorSlotId(hash, logNbSlots, nbSlots);
		IndexLine headLine=new IndexLine(accessIndexHead.getBytes(positionInIndexHead*20, 20));
		
		byte[] slot=accessIndexBody.getBytes(headLine.position, headLine.length);
		//System.out.println(slot.length+"<-slot");
		/**
		 * 此处改为返回字符串列表
		 */
		//IndexLine indexBodyLine=null;
		boolean begin=false;
		List<IndexLine> indexBodyLineList=new Vector<>();
		for (int i=0; i<slot.length; i+=20){
			byte[] buffer=Arrays.copyOfRange(slot, i, i+20);
			IndexLine line=new IndexLine(buffer);
			if(!begin){
				if(line.hash==hash){
					indexBodyLineList.add(line);
					begin=true;
				}
			}
			else if(begin){
				if(line.hash==hash){
					indexBodyLineList.add(line);
				}
				else {
					break;
				}

			}
//			if (line.hash==hash){
//				//indexBodyLine=line;
//				//break;
//				indexBodyLineList.add(line);
//			}
		}
		if (indexBodyLineList.size()==0)
			return null;
		List<byte[]> dataList=new Vector<>();
		for (IndexLine  indexBodyLine:indexBodyLineList){
			dataList.add(accessData.getBytes(indexBodyLine.position,indexBodyLine.length));
		}
		//byte[] data=accessData.getBytes(indexBodyLine.position, indexBodyLine.length);
		//return data;
		return dataList;
	}
	
	
	@Override
	public void close() throws IOException  {
		accessData.close();
		accessIndexBody.close();
		accessIndexHead.close();
	}
	
	public static void main(String[] args) throws IOException {
		FrohmdMapMultV map=new FrohmdMapMultV("testIndex");
		long start=System.nanoTime();
		Random rand=new Random();
		int nbError=0;
		for (int i=0; i<100000; i++){
			int ri=rand.nextInt(200_000_000);
//			String s=map.getString("key"+ri);
//			if (!s.endsWith(String.valueOf(ri)))
//					nbError++;
		}
		
		System.out.println("10,000 random reads in "+(System.nanoTime()-start)/1e6+"ms");
		System.out.println(nbError);
		map.close();
	}
	
	
}
