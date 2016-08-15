package com.db;

import com.db.bplustree.BPlusTree;
import com.db.bplustree.StringType;

/**
 * Created by iceke on 16/7/18.
 */
public class BplusTreeStringToString20 extends BPlusTree<String, String> {
    public BplusTreeStringToString20(String fileName,int blockSize) {
        super(fileName, new StringType(50), new StringType(20));
    }
}
