package com.db.bplustree;

import com.db.Block;

public class IntType implements BPlusTreeType<Integer>{

  @Override
  public int getSize() {
    return 4;
  }

  @Override
  public void write(Integer data, Block block, int offset) {
    block.setInt(data, offset);
  }

  @Override
  public Integer read(byte[] bytes, int offset) {
    return (bytes[offset] << 24) | ((bytes[offset + 1] & 0xff) << 16)
        | ((bytes[offset + 2] & 0xff) << 8)
        | (bytes[offset + 3] & 0xff);
  }

}
