package com.util;
import java.io.*;
import java.util.*;

/**
 * Created by bbw on 16/7/8.
 * 用于文件排序的类
 */
public class FileSort {

    private int BUFFER_LINES=800000;
    private String key;
    Collection<String> filecollection;
    Queue<String> splitCollection;
    String output;
    List<FileIndex> fileIndex;

    /**
     *
     * @param key
     * 比较的字段
     * @param fileInput
     * 文件输入集合
     * @param output
     * 输出文件名字
     */

    public FileSort(String key,Collection<String> fileInput,String output){
        this.filecollection=fileInput;
        this.key=key;
        this.splitCollection =new LinkedList<String>();
        this.output=output;
        this.fileIndex=new LinkedList<FileIndex>();
    }

    /**
     * 排序流程,先分给小文件排序,最后归并,返回文件索引
     * 文件索引key 为文件第一个ID,value为文件名字。
     * @throws IOException
     */

    public List<FileIndex> sort() throws IOException{
        for (String file:filecollection){
            split(file,file);
        }
        mergeFile("temp");
        splitToDir("temp",output);

        return fileIndex;
    }



    public void splitToDir(String filename,String outputPath) throws IOException {
        ///输出为文件夹,如果文件夹不存在,先建文件夹
        File dir=new File(outputPath);
        if (!dir.exists())
            dir.mkdirs();

        BufferedReader bf = new BufferedReader(new FileReader(new File(filename)));
        int count = 0;
        String line;
        String orderId="";
        boolean complete = false;
        while (!complete) {
            TreeSet<String> set = new TreeSet<String>(new KeyComparetor(key));
            int index = BUFFER_LINES;
            boolean firstLine=true;

            for (int i = 0; i < BUFFER_LINES && !complete; i++) {
                line = bf.readLine();
                if(firstLine){
                    orderId=line.substring(line.indexOf(key)+key.length()+1,line.indexOf("\t",line.indexOf(key)));
                    firstLine=false;

                }
                if (line == null) {
                    complete = true;
                    index = i;
                } else {
                    set.add(line);
                }
            }
            if (index > 0) {
                String partFileName = outputPath +"/"+key+".part" + count;
                count++;
                splitCollection.add(partFileName);
                File newfile = new File(partFileName);
                FileWriter fw = new FileWriter(newfile);
                for (String sortLine : set) {
                    fw.write(sortLine + "\n");

                }
                fw.flush();
                fw.close();
                // 添加索引
                //fileIndex.put(orderId,partFileName);
                fileIndex.add(new FileIndex(orderId,partFileName));

            }

        }
    }

    /**
     * 分割文件,每个文件一定的行数
     * @param fileName
     * @throws IOException
     */

    private   void split(String fileName,String outputPath)
            throws IOException{

        BufferedReader bf=new BufferedReader(new FileReader(new File(fileName)));
        int count=0;
        String line;
        boolean complete=false;
        while (!complete){
            TreeSet<String> set=new TreeSet<String>(new KeyComparetor(key));
            int index=BUFFER_LINES;
            for (int i=0;i<BUFFER_LINES&&!complete;i++){
                line=bf.readLine();
                if(line==null){
                    complete=true;
                    index=i;
                }
                else {
                    set.add(line);
                }
            }
            if(index>0){
                String partFileName=outputPath+"part"+count;
                count++;
                splitCollection.add(partFileName);
                File newfile=new File(partFileName);
                FileWriter fw=new FileWriter(newfile);
                for (String sortLine:set){
                    fw.write(sortLine+"\n");

                }
                fw.flush();
                fw.close();

            }
        }





    }

    /**
     * 归并分割文件
     * @param outPath
     */

    private void  mergeFile(String outPath){
        Queue<String> fileList=splitCollection;
        Queue<String> newList =new LinkedList<String>();
        int count=1;
        while (fileList.size()!=1){
            while (fileList.size()>=2){
                String newfilename="merge"+count;
                count++;
                mergeTwoFile(fileList.poll(),fileList.poll(),newfilename);
                newList.add(newfilename);

            }
            if (fileList.size()==1){
                newList.add(fileList.poll());
            }

            while (newList.size()!=0){
                fileList.add(newList.poll());
            }


        }
        File lastFile=new File(fileList.poll());
        lastFile.renameTo(new File(outPath));

    }

    private void mergeTwoFile(String file1,String file2,String outFile){
        try {
            File f1=new File(file1);
            File f2=new File(file2);
            BufferedReader bf1 = new BufferedReader(new FileReader(f1));
            BufferedReader bf2=new BufferedReader(new FileReader(f2));
            FileWriter fw=new FileWriter(new File(outFile));
            String l1,l2;
            l1=bf1.readLine();
            l2=bf2.readLine();
            while (l1!=null&&l2!=null){
                if(l1.substring(8,15).compareTo(l2.substring(8,15))>0){
                    fw.write(l2+"\n");
                    l2=bf2.readLine();
                }
                else {
                    fw.write(l1 + "\n");
                    l1=bf1.readLine();
                }

            }

            if(l1==null){
                while (l2!=null){
                    fw.write(l2+"\n");
                    l2=bf2.readLine();
                }
            }
            else if(l2==null){
                while (l1!=null){
                    fw.write(l1+"\n");
                    l1=bf1.readLine();
                }
            }
            fw.flush();
            fw.close();

            bf1.close();
            bf2.close();
            f1.delete();
            f2.delete();



        }
        catch (Exception e){
            e.printStackTrace();
        }


    }





    class KeyComparetor implements Comparator<String>{
        private String key;
        public KeyComparetor(String key){
            this.key=key;


        }
        @Override
        public int compare(String o1, String o2) {
            int a1=o1.indexOf(key);
            int b1=o1.indexOf("\t",a1);
            int a2=o2.indexOf(key);
            int b2=o2.indexOf("\t",a2);
            if(o1.substring(a1+key.length()+1,b1).compareTo(o2.substring(a2+key.length()+1,b2))>0)
                return 1;
            return -1;

        }
    }



}




