package com.db;

import com.db.bplustree.BPlusTree;
import com.db.bplustree.StringType;

/**
 * Created by iceke on 16/7/19.
 */
public class BplusTreeString15ToString20 extends BPlusTree<String, String> {
    public BplusTreeString15ToString20(String fileName,int blockSize) {
        super(fileName, new StringType(15), new StringType(20));
    }
}
