package com.test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Created by bbw on 16/7/21.
 */
public class generate_order4g {
    public static void main(String[] args){
        List<Long> timelist=new ArrayList<>();
        try {
            FileWriter fw=new FileWriter(new File("data/order_record_4g.txt"));
            BufferedReader bf=new BufferedReader(new FileReader(new File("data/order_records.txt")));
            String  line;
            while ((line=bf.readLine())!=null){
                timelist.add(Long.parseLong(line.substring(line.indexOf("createtime")+"createtime".length()+1,
                        line.indexOf("\t",line.indexOf("createtime")))));
            }

            List<String> goodList=new ArrayList<>();
            List<String> buyerList=new ArrayList<>();

            BufferedReader bf1=new BufferedReader(new FileReader(new File("data/good.txt")));

            while ((line=bf1.readLine())!=null){
                goodList.add(line.substring(line.indexOf("goodid")+"goodid".length()+1,
                        line.indexOf("\t",line.indexOf("goodid"))));
            }

            BufferedReader bf2=new BufferedReader(new FileReader(new File("data/buyer.txt")));

            while ((line=bf2.readLine())!=null){
                buyerList.add(line.substring(line.indexOf("buyerid")+"buyerid".length()+1,
                        line.indexOf("\t",line.indexOf("buyerid"))));
            }

            Long orderid=3009312l;
            int buyersize=buyerList.size();
            int goodsize=goodList.size();
            int timesize=timelist.size();
            Random random=new Random();
            Long time;
            for (int i=0;i<10000000;i++){
                time=timelist.get(random.nextInt(timesize))+random.nextInt(100);

                fw.write("orderid:"+orderid+"\tgoodid:"+goodList.get(random.nextInt(goodsize))+
                "\tbuyerid:"+buyerList.get(random.nextInt(buyersize))+"\tcreatetime:"+time+"\n");
                orderid++;
            }
            fw.flush();
            fw.close();
            bf.close();
            bf1.close();
            bf2.close();

        }
        catch (Exception e){
            e.printStackTrace();
        }
    }
}
