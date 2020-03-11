package com.pep.bi;

import com.pep.common.DbProperties;

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
public class YunwangDwBiShowDate {
    public static void main(String[] args) {

        SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(new Date());
        calendar.add(Calendar.DATE,-1);
        String yesterdayStr = format.format(calendar.getTime());

        String insertSql = "insert into show.bi_show (bi_show_time) VALUES (?); ";

        //读取数据源文件
        Properties prop = DbProperties.propScp;
        try {
            String username = prop.getProperty("user");
            String password = prop.getProperty("password");
            String url = prop.getProperty("url");
            Connection conn = DriverManager.getConnection(url,username,password);
            PreparedStatement prepStat = conn.prepareStatement(insertSql);
            prepStat.setString(1,yesterdayStr);
            boolean res = prepStat.execute();
            conn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
