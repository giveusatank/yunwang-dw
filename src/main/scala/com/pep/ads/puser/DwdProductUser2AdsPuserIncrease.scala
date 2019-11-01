package com.pep.ads.puser

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.control.Breaks

object DwdProductUser2AdsPuserIncrease {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-DwdProductUser2AdsPuserIncrease")
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
    writeDwdProductUser2AdsPUserIncrease(spark,yesStr)
    writeAdsPUserIncrease2PostgreSql(spark,yesStr)
  }

  def writeDwdProductUser2AdsPUserIncrease(spark: SparkSession, yesStr: String) = {

    spark.sql("use ads")

    val createSql =
      s"""
         |create table if not exists ads_puser_increase(
         |product_id string,
         |company string,
         |country string,
         |province string,
         |user_count string,
         |count_date string
         |) stored as parquet
      """.stripMargin

    spark.sql(createSql)

    val etlSql =
      s"""
         |insert into table ads_puser_increase
         |select ress.pid,ress.com,ress.country,ress.province,count(distinct(ress.user_id)),'${yesStr}' from
         |(select ress.pid,ress.com,ress.user_id,ress.country,ress.province,ress.city from
         |(select temp.user_id,temp.pid,temp.com,temp.country,temp.province,temp.city,
         |row_number() over(partition by temp.user_id,temp.pid,temp.com order by temp.cou desc) as rkk from
         |(select t1.user_id,t1.product_id as pid,t1.company as com,t2.country,t2.province,t2.city,
         |count(1) over(partition by t1.product_id,t1.company,t1.user_id,t2.country,t2.province,
         |t2.city) as cou from
         |(select * from dwd.dwd_product_user where from_unixtime(cast(row_timestamp as bigint) / 1000,'yyyyMMdd')='${yesStr}')
         |as t1 left join dwd.dwd_user_area as t2 on
         |t1.product_id=t2.product_id and t1.company=t2.company and t1.user_id=t2.active_user) as temp )
         |as ress) group by ress.pid,ress.com,ress.country,ress.province
      """.stripMargin

    spark.sql(etlSql)
  }

  def writeAdsPUserIncrease2PostgreSql(spark: SparkSession, yesStr: String) = {

    val props = new java.util.Properties()
    props.setProperty("user","pgadmin")
    props.setProperty("password","szkf2019")
    props.setProperty("url","jdbc:postgresql://172.30.0.9:5432/bi")
    props.setProperty("tableName","ads_puser_increase")
    props.setProperty("write_mode","Append")

    spark.sql("use ads")

    val selectSql =
      s"""
        |select product_id,company,country,province,user_count,count_date from ads_puser_increase
      """.stripMargin

    val df_1: Dataset[Row] = spark.sql(selectSql).coalesce(2)

    df_1.write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName"),props)
  }
}
