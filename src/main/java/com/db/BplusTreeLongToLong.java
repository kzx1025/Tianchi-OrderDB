package com.db;

import com.db.bplustree.BPlusTree;
import com.db.bplustree.LongType;

/**
 * Created by iceke on 16/7/21.
 */
public class BplusTreeLongToLong extends BPlusTree<Long,Long> {
    public BplusTreeLongToLong(String fileName,int blockSize) {
        super(fileName, new LongType(), new LongType());
    }
}
