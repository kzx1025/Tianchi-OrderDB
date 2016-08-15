package Frohmd;

import java.util.Arrays;

import com.google.common.primitives.Ints;
import com.google.common.primitives.Longs;

public class IndexLine{
	long hash;
	public long position;
	public int length;
	public IndexLine(byte[] line) {
		hash=Longs.fromByteArray(Arrays.copyOfRange(line, 0, 8));
		position=Longs.fromByteArray(Arrays.copyOfRange(line, 8, 16));
		length=Ints.fromByteArray(Arrays.copyOfRange(line, 16, 20));
	}
	public static byte[] toLine(long l1, long l2, int i){
		byte[] line=new byte[20];
		byte[] l1_b=Longs.toByteArray(l1);
		byte[] l2_b=Longs.toByteArray(l2);
		byte[] i_b=Ints.toByteArray(i);
		for (int j=0; j<8; j++)
			line[j]=l1_b[j];
		for (int j=0; j<8; j++)
			line[j+8]=l2_b[j];
		for (int j=0; j<4; j++)
			line[j+16]=i_b[j];
		return line;
	}
	@Override
	public String toString() {
		return hash+","+position+","+length;
	}
}
