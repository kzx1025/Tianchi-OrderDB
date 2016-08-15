package com.db;

import com.db.bplustree.BPlusTree;
import com.db.bplustree.IntType;



/**
 * Created by iceke on 16/7/15.
 */
public class BplusTreeIntToInt extends BPlusTree<Integer, Integer> {
    public BplusTreeIntToInt(String fileName,int blockSize) {
        super(fileName, new IntType(), new IntType());
    }
}
