package com.db;

import com.db.bplustree.BPlusTree;
import com.db.bplustree.IntType;
import com.db.bplustree.StringType;

/**
  Implements a B+ tree in which the keys are integers and the
  values are Strings (with maximum length 60 characters)
*/

public class BPlusTreeIntToString20 extends BPlusTree<Integer, String> {
	
  public BPlusTreeIntToString20(String fileName,int blockSize) {
    super(fileName, new IntType(), new StringType(20));
  }

}
