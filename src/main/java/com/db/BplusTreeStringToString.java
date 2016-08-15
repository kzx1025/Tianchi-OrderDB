package com.db;

import com.db.bplustree.BPlusTree;
import com.db.bplustree.IntType;
import com.db.bplustree.StringType;

/**
 * Created by iceke on 16/7/15.
 */
public class BplusTreeStringToString extends BPlusTree<String, String> {
    public BplusTreeStringToString(String fileName, int blockSize) {
        super(fileName, new StringType(20), new StringType(150));
    }
}
