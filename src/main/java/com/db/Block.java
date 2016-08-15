package com.db;

/* Code for COMP261 Assignment
 */

import java.util.Arrays;

/**
 * Block stores an array of bytes from a block in a BlockFile, and provides
 * methods to access and update values represented in the array of bytes.
 */

public class Block {
	private byte[] bytes;

	/** Construct a new Block object */
	public Block(byte[] b) {
		bytes = b;
	}

	public Block(int length) {
		bytes = new byte[length];
	}

	public byte getByte(int index) {
		return bytes[index];
	}

	public void setByte(byte value, int index) {
		bytes[index] = value;
	}

	/** Return the number of bytes in the block */
	public int length() {
		return bytes.length;
	}

	public int getInt(int offset) {
		return (bytes[offset] << 24) | ((bytes[offset + 1] & 0xff) << 16)
				| ((bytes[offset + 2] & 0xff) << 8)
				| (bytes[offset + 3] & 0xff);
	}

	public void setInt(int value, int offset) {
		bytes[offset] = (byte) (value >>> 24);
		bytes[offset + 1] = (byte) (value >>> 16 & 0xff);
		bytes[offset + 2] = (byte) (value >>> 8 & 0xff);
		bytes[offset + 3] = (byte) (value & 0xff);
	}

	// add by kzx
	public void setLong(long value, int offset){
		bytes[offset] = (byte) (value >>> 56);
		bytes[offset + 1] = (byte) (value >>> 48 & 0xff);
		bytes[offset + 2] = (byte) (value >>> 40 & 0xff);
		bytes[offset + 3] = (byte) (value >>> 32 & 0xff);
		bytes[offset + 4] = (byte) (value >>> 24 & 0xff);
		bytes[offset + 5] = (byte) (value >>> 16 & 0xff);
		bytes[offset + 6] = (byte) (value >>> 8 & 0xff);
		bytes[offset + 7] = (byte) (value & 0xff);

	}

	public String getString(int offset, int length) {
		return new String(Arrays.copyOfRange(bytes, offset, offset + length));
	}

	public void setString(String value, int offset) {
		byte[] stringBytes = value.getBytes();
		for (int i = 0; i < stringBytes.length; i++) {
			bytes[offset++] = stringBytes[i];
		}
	}

	public byte[] getBytes() {
		return bytes;
	}

	public byte[] getBytes(int offset, int length) {
		return Arrays.copyOfRange(bytes, offset, offset + length);
	}

	public void setBytes(byte[] value, int offset) {
		for (int i = 0; i < value.length; i++) {
			bytes[offset++] = value[i];
		}
	}

	/** compare two values stored in regions within the same block */
	public int compare(int offset1, int offset2, int length) {
		return compare(offset1, this, offset2, length);
	}

	/** compare two values stored in regions of two blocks */
	public int compare(int offset1, Block other, int offset2, int length) {
		if (offset1 + length > bytes.length
				|| offset2 + length > other.bytes.length) {
			throw new IllegalArgumentException();
		}
		for (int i = 0; i < length; i++) {
			if (bytes[offset1 + i] + 128 < other.bytes[offset2 + i] + 128)
				return -1;
			if (bytes[offset1 + i] + 128 > other.bytes[offset2 + i] + 128)
				return 1;
		}
		return 0;
	}

	/** compare a value in a byte[] with the value in a region of the block */
	public int compare(byte[] value, int offset) {
		if (offset + value.length > bytes.length) {
			throw new IllegalArgumentException();
		}
		for (int i = 0; i < value.length; i++) {
			if (value[i] + 128 < bytes[offset + i] + 128)
				return -1;
			if (value[i] + 128 > bytes[offset + i] + 128)
				return 1;
		}
		return 0;
	}

}
