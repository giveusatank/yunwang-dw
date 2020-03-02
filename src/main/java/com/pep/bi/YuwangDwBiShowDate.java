package com.pep.bi;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Properties;

/**
 * @Autor: QZ
 * @Description:
 * @Date: 2019/7/25
 */
public class YuwangDwBiShowDate {
    public static void main(String[] args) {

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DATE,-1);
        String yestodayStr = format.format(calendar.getTime());

        String insertSql = "insert into show.bi_show (bi_show_time) VALUES (?); ";

        //读取数据源文件
        InputStream input = YuwangDwBiShowDate.class.getClassLoader().getResourceAsStream("datasource.properties");
        Properties prop = new Properties();
        try {
            prop.load(input);
            String username = prop.getProperty("username");
            String password = prop.getProperty("password");
            String url = prop.getProperty("url");
            Connection conn = DriverManager.getConnection(url,username,password);
            PreparedStatement prepStat = conn.prepareStatement(insertSql);
            prepStat.setString(1,yestodayStr);
            boolean res = prepStat.execute();
            input.close();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
