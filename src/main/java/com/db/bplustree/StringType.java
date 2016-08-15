package com.db.bplustree;

import com.db.Block;
import com.db.Bytes;

public class StringType implements BPlusTreeType<String>{

  private int size;

  public StringType(int _size){
    size = _size;
  }

  @Override
  public int getSize() {
	  //Length of the string + 4 bytes for length info
    return size + 4;
  }

  @Override
  public void write(String data, Block block, int offset) {
    if(data.getBytes().length > size){
      throw new IllegalArgumentException("Invalid String: Too long"+data.getBytes().length+ "for " + size + " bytes");
    }

    block.setInt(data.length(), offset);
    block.setString(data, offset + 4);
  }

  @Override
  public String read(byte[] data, int offset) {
    int length = Bytes.bytesToInt(data, offset);
    byte[] stringBytes = new byte[length];
    for(int i = offset + 4;i < offset + length + 4;i ++){
      stringBytes[i - offset - 4] = data[i];
    }

    return new String(stringBytes).trim();
  }

}
