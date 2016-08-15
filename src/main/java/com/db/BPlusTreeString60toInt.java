package com.db;

import com.db.bplustree.BPlusTree;
import com.db.bplustree.IntType;
import com.db.bplustree.StringType;

/**
  Implements a B+ tree in which the keys  are Strings (with
  maximum length 60 characters) and the values are integers 
*/

public class BPlusTreeString60toInt extends BPlusTree<String, Integer>{

  public BPlusTreeString60toInt(String fileName, int blockSize) {
    super(fileName, new StringType(60), new IntType());
  }
	
}
