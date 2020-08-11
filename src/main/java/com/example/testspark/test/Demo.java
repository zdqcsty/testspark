package com.example.testspark.test;

import cn.binarywang.tools.generator.*;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Random;
import java.util.StringJoiner;

public class Demo {

    public static void main(String[] args) throws FileNotFoundException {
        //身份证号码
        ChineseIDCardNumberGenerator cidcng = (ChineseIDCardNumberGenerator) ChineseIDCardNumberGenerator.getInstance();
        //中文姓名
        ChineseNameGenerator cng = ChineseNameGenerator.getInstance();
        //英文姓名
        EnglishNameGenerator eng = EnglishNameGenerator.getInstance();
        //手机号
        ChineseMobileNumberGenerator cmng = ChineseMobileNumberGenerator.getInstance();
        //电子邮箱
        EmailAddressGenerator eag = (EmailAddressGenerator) EmailAddressGenerator.getInstance();
        //居住地址
        ChineseAddressGenerator cag = (ChineseAddressGenerator) ChineseAddressGenerator.getInstance();

        //时间
        Calendar cal = Calendar.getInstance();
        Random random=new Random();


        PrintWriter pw = new PrintWriter("E:\\data.csv");
        for (int i = 0; i < 100; i++) {

            int d = random.nextInt(30);
            cal.setTime(new Date());
            cal.add(Calendar.DATE, -d);
            Date time = cal.getTime();
            SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
            String date=format.format(time);


            StringJoiner sj = new StringJoiner(",");
            sj.add(date);
            sj.add(cidcng.generate());
            sj.add(cng.generate());
            sj.add(eng.generate());
            sj.add(cmng.generate());
            sj.add(eag.generate());
            sj.add(cag.generate());
            pw.println(sj.toString());
        }
        pw.close();







    }

}
