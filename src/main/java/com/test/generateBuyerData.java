package com.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.*;

/**
 * Created by bbw on 16/7/21.
 */
public class generateBuyerData {


    public static void main(String[]args){
        try {
            BufferedReader fr=new BufferedReader(new FileReader(new File("data/buyer_records.txt")));
            List<String> set1=new ArrayList<>();
            List<String> set2=new ArrayList<>();
            List<String> set3=new ArrayList<>();
            List<String> set4=new ArrayList<>();
            List<String> set5=new ArrayList<>();
            List<String> set6=new ArrayList<>();
            List<String> set7=new ArrayList<>();

            String line;
            while ((line=fr.readLine())!=null){
                String buyerid=line.substring(line.indexOf("buyer")+"buyerid".length()+1,line.indexOf("\t"));
                String [] temp=buyerid.split("_");
                set1.add(temp[0]);
                String [] temp2=temp[1].split("-");
                set2.add(temp2[0]);
                set3.add(temp2[1]);
                set4.add(temp2[2]);
                set5.add(temp2[3]);
                set6.add(temp2[4]);
                set7.add(line.substring(line.indexOf("\t")));
            }
            int size=set1.size();
            Random random=new Random();

            Set<String> buyerSet=new HashSet<>();
            while (buyerSet.size()<4000000){
                buyerSet.add(set1.get(random.nextInt(size))+"_"+
                        set2.get(random.nextInt(size))+"-"+
                        set3.get(random.nextInt(size))+"-"+
                        set4.get(random.nextInt(size))+"-"+
                        set5.get(random.nextInt(size))+"-"+
                        set6.get(random.nextInt(size))
                );

            }
            FileWriter fw=new FileWriter(new File("data/buyer.txt"));
            for(String s:buyerSet){
                fw.write("buyerid:"+s+set7.get(random.nextInt(size))+"\n");
            }
            fw.flush();
            fw.close();


        }catch (Exception e){
            e.printStackTrace();
        }

    }
}