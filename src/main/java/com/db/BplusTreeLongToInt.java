package com.db;

import com.db.bplustree.BPlusTree;
import com.db.bplustree.BPlusTreeType;
import com.db.bplustree.IntType;
import com.db.bplustree.LongType;

/**
 * Created by iceke on 16/7/20.
 */
public class BplusTreeLongToInt extends BPlusTree<Long, Integer> {
    public BplusTreeLongToInt(String fileName,int blockSize) {
        super(fileName, new LongType(), new IntType());
    }
}
