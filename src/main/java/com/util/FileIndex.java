package com.util;

/**
 * Created by bbw on 16/7/10.
 */
public class FileIndex{
    private String orderid;
    private String fileName;
    public FileIndex(String orderid,String fileName){
        this.orderid=orderid;
        this.fileName=fileName;

    }
    public String getOrderid(){
        return orderid;
    }
    public String getFileName(){
        return fileName;
    }
}