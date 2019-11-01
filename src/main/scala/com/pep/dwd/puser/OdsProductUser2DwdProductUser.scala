package com.pep.dwd.puser

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

/**
  * Ods历史累计全局用户注册表到Dwd历史累计全局用户注册表
  */
object OdsProductUser2DwdProductUser {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-OdsProductUser2DwdProductUser")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val regPatten = "^[0-9]{8}$".r
    val flag = args.length > 0
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE,-1)
    var yesStr = format.format(cal.getTime)

    loop.breakable{
      for(i <- 0 until (if(args.length > 1) args.length else 1)){
        if(flag) {
          if(regPatten.findPrefixOf(args(i))==None) loop.break()
          yesStr = args(i)
        }
        doAction(spark,yesStr)
      }
      spark.stop()
    }
  }

  def doAction(spark:SparkSession, yesStr:String) = {
    writeOdsProductUser2DwdProductUser(spark,yesStr)
  }

  def writeOdsProductUser2DwdProductUser(spark: SparkSession, yesStr: String) = {

    spark.sql("use dwd")

    val createSql =
      s"""
        |CREATE TABLE if not exists dwd_product_user(
        |user_id string,
        |product_id string,
        |company string,
        |reg_name string,
        |nick_name string,
        |real_name string,
        |phone string,
        |email string,
        |sex string,
        |birthday string,
        |address string,
        |org_id string,user_type string,
        |first_access_time string,
        |last_access_time string,
        |last_access_ip string,
        |country string,
        |province string,
        |city string,
        |location string,
        |row_timestamp string,
        |row_status string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      """.stripMargin

    spark.sql(createSql)

    spark.sql("msck repair table ods.ods_product_user")

    val etlSql =
      s"""
        |insert overwrite table dwd.dwd_product_user
        |select if(instr(user_id,'_')!=0,split(user_id,'_')[1],user_id),
        |if(company='pep_click','121301',product_id) as product_id,company,reg_name,nick_name,real_name,phone,email,sex,
        |birthday,address,org_id,user_type,first_access_time,last_access_time,last_access_ip,
        |country,province,city,location,row_timestamp,row_status
        |from
        |(select *,row_number() over(partition by product_id,company,user_id order by row_timestamp desc) as rank
        |from ods.ods_product_user) as t1 where t1.rank=1 and t1.row_status='1'
      """.stripMargin

    spark.sql(etlSql)


  }

}
