package com.util;

import java.io.*;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

/**
 * Created by iceke on 16/7/16.
 */
public class FileSplit {
    /**
     *
     * @param filename  要切割的文件
     * @param outputPath 切割输出文件夹
     * @param lineNum  每个文件行数
     */
    public static List<String> splitToDir(String filename, String outputPath, int lineNum) throws IOException {
        ///输出为文件夹,如果文件夹不存在,先建文件夹
        List<String> splitFileList=new LinkedList<String>();
        File dir=new File(outputPath);
        if (!dir.exists())
            dir.mkdirs();

        BufferedReader bf = new BufferedReader(new FileReader(new File(filename)));
        int count = 0;
        String line;
        boolean complete = false;
        String[] s = filename.split("/");
        String fileLabel = s[s.length-1];
        while (!complete) {
            ++count;
            String partFileName = outputPath +fileLabel+"_"+ count;
            File newfile = new File(partFileName);
            FileWriter fw = new FileWriter(newfile);
            for (int i = 0; i < lineNum && !complete; i++) {
                line = bf.readLine();

                if (line == null) {
                    complete = true;
                    break;
                }
                fw.write(line+"\n");

            }

            fw.flush();
            fw.close();
            splitFileList.add(partFileName);

        }
        return splitFileList;

    }

    public static void mergerFiles(String[] filesrc,String fileresult){
        SequenceInputStream iStream = null;
        BufferedOutputStream bStream = null;
        Vector<InputStream > vector = new Vector<InputStream>();
        try {
            for (int i = 0; i < filesrc.length; i++) {
                vector.addElement(new FileInputStream(new File(filesrc[i])));
            }
            Enumeration<InputStream> enumeration = vector.elements();
            iStream = new SequenceInputStream(enumeration);
            bStream = new BufferedOutputStream(new FileOutputStream(new File(fileresult)));
            byte[] arr = new byte[1024*20];
            int len = 0;
            while ((len = iStream.read(arr))!=-1) {
                bStream.write(arr, 0, len);
                bStream.flush();//刷新此缓冲的输出流。
            }
            iStream.close();
            bStream.close();
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static void main(String args[]){
        try {
            long start = System.currentTimeMillis();
            //System.out.println(FileSplit.splitToDir("data/order_record_4g.txt", CommonValue.SPLIT_OUTPUT_DIR, 6000000));
            String[] files = {"data/buyer.txt","data/good.txt","data/order_record_4g.txt"};
            String result = "data/merger_file";
            FileSplit.mergerFiles(files,result);
            long end = System.currentTimeMillis();
            System.out.println((end-start)/1000);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
