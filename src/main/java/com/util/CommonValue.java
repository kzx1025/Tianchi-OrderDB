package com.util;

/**
 * Created by iceke on 16/7/16.
 */
public class CommonValue {
    public static final String ORDER_FILE = "data/order_records.txt";
    public static final String BUYER_FILE = "data/buyer_records.txt";
    public static final String GOOD_FILE = "data/good_records.txt";
    public static final String SPLIT_OUTPUT_DIR = "data/split/";
    public static final int ORDER_FILE_SPLIT_LINES = 200002;
    //public static final int ORDER_FILE_SPLIT_LINES = 5000002;
    public static final int BUYER_FILE_SPLIT_LINES= 5000002;
    public static final int GOOD_FILE_SPLIT_LINES = 5000002;

    public static final boolean MERGE_ORDER_FILE = false;

    public static final String ORDER_INDEX = "orderid.b+tree";
    public static final String BUYER_ORDER_INDEX = "buyer_order.b+tree";
    public static final String BUYER_INDEX = "buyer.b+tree";
    public static final String GOOD_ORDER_INDEX = "good_order.b+tree";
    public static final String GOOD_INDEX = "good.b+tree";

}
