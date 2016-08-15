package com.db;

import java.util.Comparator;

/** Utilities for converting to and from arrays of bytes */

public class Bytes {

	/**
	 * converts a positive integer to a byte, treating the byte as an unsigned 8
	 * bits. If the integer is greater than 255, then the higher order bits will
	 * be chopped off If the integer is negative, then it will be inverted first
	 */
	public static byte intToByte(int number) {
		return (byte) ((number < 0 ? -number : number) & 0xff);
	}

	/**
	 * converts a byte (treated as an unsigned 8 bits) to an integer, If the
	 * integer is greater than 255, then the higher order bits will be chopped
	 * off
	 */
	public static int byteToInt(byte b) {
		if (b >= 0)
			return b;
		else
			return b + 256;
	}

	/** converts an integer to an array of four bytes. */
	public static byte[] intToBytes(int number) {
		byte[] ans = new byte[4];
		ans[0] = (byte) (number >>> 24);
		ans[1] = (byte) (number >>> 16 & 0xff);
		ans[2] = (byte) (number >>> 8 & 0xff);
		ans[3] = (byte) (number & 0xff);
		return ans;
	}

	/**
	 * converts an integer to four bytes and puts them at the specified offset
	 * in a byte array.
	 */
	public static void intToBytes(int number, byte[] block, int offset) {
		block[offset] = (byte) (number >>> 24);
		block[offset + 1] = (byte) (number >>> 16 & 0xff);
		block[offset + 2] = (byte) (number >>> 8 & 0xff);
		block[offset + 3] = (byte) (number & 0xff);
	}

	/** converts an array of four bytes to an integer. */
	public static int bytesToInt(byte[] bytes) {
		return (bytes[0] << 24) | ((bytes[1] & 0xff) << 16) | ((bytes[2] & 0xff) << 8) | (bytes[3] & 0xff);
	}

	/**
	 * converts the four bytes at the specified offset in a byte array into an
	 * integer.
	 */
	public static int bytesToInt(byte[] bytes, int offset) {
		return (bytes[offset] << 24) | ((bytes[offset + 1] & 0xff) << 16) | ((bytes[offset + 2] & 0xff) << 8) | (bytes[offset + 3] & 0xff);
	}

	/** converts four bytes to an integer. */
	public static int bytesToInt(byte b1, byte b2, byte b3, byte b4) {
		return (b1 << 24) | ((b2 & 0xff) << 16) | ((b3 & 0xff) << 8) | (b4 & 0xff);
	}

	/**
	 * Compares two byte arrays Must add 128 because < and > treat the byte as a
	 * signed integer; Adding 128 makes the values positive.
	 */
	public static int compare(byte[] a, byte[] b) {
		int shared = a.length;
		if (b.length < shared)
			shared = b.length;
		for (int i = 0; i < shared; i++) {
			if (a[i] + 128 < b[i] + 128)
				return -1;
			if (a[i] + 128 > b[i] + 128)
				return 1;
		}
		// they are the same up to index shared
		if (a.length < b.length)
			return -1;
		if (a.length > b.length)
			return 1;
		return 0;
	}

	public static class ByteArrayComparator implements Comparator<byte[]> {
		@Override
		public int compare(byte[] a, byte[] b) {
			return Bytes.compare(a, b);
		}
	}

	/**
	 * Compare a byte array with a segment of a (larger) byte array, starting at
	 * the specified offset in the second array. The second array must be large
	 * enough.
	 */
	public int compare(byte[] value, byte[] array, int offset) {
		if (offset + value.length > array.length) {
			throw new IllegalArgumentException();
		}
		for (int i = 0; i < value.length; i++) {
			if (value[i] + 128 < array[offset + i] + 128)
				return -1;
			if (value[i] + 128 > array[offset + i] + 128)
				return 1;
		}
		return 0;
	}

	// TESTING CODE HERE
	public static void main(String[] args) {
		byte[] a = new byte[] { (byte) 0134, (byte) 0345, (byte) 0456, (byte) 0753 };
		byte[] b = new byte[] { (byte) 0134, (byte) 0345, (byte) 0546, (byte) 0753 };
		byte[] c = new byte[] { (byte) 0134, (byte) 0345, (byte) 0546 };

		Comparator<byte[]> comp = new ByteArrayComparator();

		System.out.printf("a: %d.%d.%d.%d\n", Bytes.byteToInt(a[0]), Bytes.byteToInt(a[1]), Bytes.byteToInt(a[2]), Bytes.byteToInt(a[3]));
		System.out.printf("b: %d.%d.%d.%d\n", Bytes.byteToInt(b[0]), Bytes.byteToInt(b[1]), Bytes.byteToInt(b[2]), Bytes.byteToInt(b[3]));
		System.out.printf("c: %d.%d.%d \n", Bytes.byteToInt(c[0]), Bytes.byteToInt(c[1]), Bytes.byteToInt(c[2]));
		System.out.printf("a?a : %d\na?b : %d\na?c : %d\nb?c : %d\n", comp.compare(a, a), comp.compare(a, b), comp.compare(a, c), comp.compare(b, c));
		System.out.printf("b?b : %d\nb?a : %d\nc?a : %d\nc?b : %d\n", comp.compare(b, b), comp.compare(b, a), comp.compare(c, a), comp.compare(c, b));
		System.out.println(bytesToInt((byte) 0134, (byte) 0345, (byte) 0456, (byte) 0753));

		System.out.println("intToBytes:");
		int n = 0xa1b32ff3;
		b = intToBytes(n);
		System.out
				.printf("dec: %d\nhex: %h\nbytes : %3d.%3d.%3d.%3d\nint: %d\n", n, n, Bytes.byteToInt(b[0]), Bytes.byteToInt(b[1]), Bytes.byteToInt(b[2]), Bytes.byteToInt(b[3]), Bytes.bytesToInt(b));

		/*
		 * System.out.println("\n\nChecking intToBytes and bytesToInt");
		 * System.out.println("checking from 0 to "+ Integer.MAX_VALUE); int
		 * counter = 0; for(int n1=0;n1>=0;n1++){ byte[] bs =
		 * Bytes.intToBytes(n1); int n2 = Bytes.bytesToInt(bs); if (n1!=n2)
		 * System.out.println("\n"+n1+": "+n2); if (counter++ == 10000000)
		 * {counter=1;System.out.println(n1);} } System.out.println(" OK");
		 *
		 * System.out.println(
		 * "intToByte and byteToInt\ndec / hex -> to byte -> back"); for(int
		 * i=0;i<256;i++){ byte bt =Bytes.intToByte(i);
		 * System.out.printf("%d / %h -> %h -> %h\n", i, i, bt&0xff,
		 * Bytes.byteToInt(bt)); }
		 */
		System.out.println("Done");
	}
	// END OF DRAFT CODE

}
