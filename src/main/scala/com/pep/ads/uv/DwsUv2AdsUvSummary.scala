package com.pep.ads.uv

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._

object DwsUv2AdsUvSummary {

  /**
    * ads 每日uv pv统计com.pep.ads.textbook.DwsUv2AdsUvSummary
    *
    * @param spark
    * @param yestStr
    */
  def dwsUvDaily2AdsUvDaily(spark: SparkSession, yestStr: String): Unit = {
    spark.sql("use ads")
    val createSql =
      """
        |create table if not exists ads_uv_daily
        |(
        |    product_id    string,
        |    company       string,
        |    country       string,
        |    province      string,
        |    city          string,
        |    location      string,
        |    user_count    bigint,
        |    action_count  bigint,
        |    session_count bigint,
        |    mark_date     string
        |)
        |    partitioned by (count_date int)
        |    stored as textfile
      """.stripMargin
    spark.sql(s"alter table ads.ads_uv_daily drop if exists partition(count_date=${yestStr})")
    spark.sql(createSql)

    val insertSql =
      s"""
         |insert into ads.ads_uv_daily partition(count_date)
         |select product_id,
         |       company,
         |       country,
         |       province,
         |       city,
         |       location,
         |       count(user_count)  as user_count,
         |       sum(action_count)  as action_count,
         |       sum(session_count) as session_count,
         |       '$yestStr',
         |       '$yestStr'
         |from (
         |         select product_id,
         |                company,
         |                country,
         |                province,
         |                city,
         |                location,
         |                count(1)           as user_count,
         |                sum(action_count)  as action_count,
         |                sum(session_count) as session_count
         |         from dws.dws_uv_daily
         |         where count_date = '$yestStr'
         |         group by product_id, company, country, province, city, location, device_id)
         |group by product_id, company, country, province, city, location
      """.stripMargin

    spark.sql(insertSql)
  }

  /**
    * ads 每日uv pv统计
    *
    * @param spark
    * @param yestStr
    */
  def dwsUvTotal2AdsUvTotal(spark: SparkSession, yestStr: String): Unit = {
    val cal = Calendar.getInstance
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -180)
    val hyTimestamp = cal.getTime.getTime
    spark.sql("use ads")
    val createSql =
      """
        |create table if not exists ads_uv_total
        |(
        |    product_id    string,
        |    company       string,
        |    country       string,
        |    province      string,
        |    city          string,
        |    location      string,
        |    user_count    bigint,
        |    action_count  bigint,
        |    session_count bigint,
        |    mark_date     string
        |)
        |    partitioned by (count_date int)
        |    stored as textfile
      """.stripMargin

    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_uv_total drop if exists partition(count_date=${yestStr})")
    val insertSql =
      s"""
         |insert into ads.ads_uv_total partition(count_date)
         |select if(company='pep_click','121301',product_id) as product_id,
         |       company,
         |       country,
         |       province,
         |       city,
         |       location,
         |       count(user_count)  as user_count,
         |       sum(action_count)  as action_count,
         |       sum(session_count) as session_count,
         |       '$yestStr',
         |       '$yestStr'
         |from (
         |         select product_id,
         |                company,
         |                country,
         |                province,
         |                city,
         |                location,
         |                count(1)           as user_count,
         |                sum(action_count)  as action_count,
         |                sum(session_count) as session_count
         |         from dws.dws_uv_total where last_access_time > '$hyTimestamp'
         |         group by product_id, company, country, province, city, location, device_id)
         |group by product_id, company, country, province, city, location
      """.stripMargin

    spark.sql(insertSql)

  }

  /**
    * ads 每日uv pv统计
    *
    * @param spark
    * @param yestStr
    */
  def dwsUvIncrease2AdsUvIncrease(spark: SparkSession, yestStr: String): Unit = {
    spark.sql("use ads")
    val createSql =
      """
        |create table if not exists ads_uv_increase(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |city string,
        |location string,
        |user_count bigint,
        |action_count bigint,
        |session_count bigint,
        |mark_date string
        |)
        |partitioned by (count_date int)
        |stored as textfile
      """.stripMargin

    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_uv_increase drop if exists partition(count_date=${yestStr})")
    val insertSql =
      s"""
         |insert into ads.ads_uv_increase partition(count_date)
         |select product_id,
         |       company,
         |       country,
         |       province,
         |       city,
         |       location,
         |       count(user_count)  as user_count,
         |       sum(action_count)  as
         |                             action_count,
         |       sum(session_count) as session_count,
         |       '$yestStr',
         |       '$yestStr'
         |from (
         |         select product_id,
         |                company,
         |                country,
         |                province,
         |                city,
         |                location,
         |                count(1)           as user_count,
         |                sum(action_count)  as action_count,
         |                sum(session_count) as session_count
         |         from dws.dws_uv_increase
         |         where count_date = '$yestStr'
         |         group by product_id, company, country, province, city, location, device_id)
         |group by product_id, company, country, province, city, location
      """.stripMargin

    spark.sql(insertSql)

  }

  /**
    * ads 每日uv pv统计
    *
    * @param spark
    * @param yestStr
    */
  def dwsUvIncrease2AdsUvConversion(spark: SparkSession, yestStr: String): Unit = {
    spark.sql("use ads")
    val createSql =
      """
        |create table if not exists ads_uv_conversion(
        |product_id string,
        |company string,
        |bus_reg string,
        |new_reg string,
        |tou_reg string,
        |new_reg_ratio string,
        |tou_active_reg_ratio string
        |)
        |partitioned by (count_date int)
        |stored as textfile
      """.stripMargin

    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_uv_conversion drop if exists partition(count_date=${yestStr})")
    val insertSql =
      s"""
         |select t.product_id,t.company,c.cu as bus_reg,t.new_reg ,(c.cu-t.new_reg) as tou_reg,round(new_reg/c.cu,2) as new_reg_ratio,round((c.cu-t.new_reg)/d.tou_cu,2) as tou_active_reg_ratio from (
         |select b.product_id,count(DISTINCT (b.user_id)) as new_reg,b.company from (-- 新用户注册数
         |select aa.product_id,aa.company,aa.active_user from dws.dws_uv_total aa join ( -- 采集注册用户中设备当日首次出现为新用户ID
         |select device_id,product_id,company from dws.dws_uv_increase where nvl(active_user,'')!='' and count_date='$yestStr' group by device_id,product_id,company -- 新用户表中的注册用户（新注册、游客注册）的设备ID
         |) bb on aa.device_id=bb.device_id and from_unixtime(cast(substring(aa.first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' and nvl(aa.active_user,'')!=''-- 筛选出当天出现的设备
         |) a join (
         |select user_id as user_id,product_id,company from dwd.dwd_product_user where -- 当天注册用户
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' group by user_id,product_id,company
         |) b on a.active_user=b.user_id and a.product_id=b.product_id  and a.company=b.company
         |group by b.product_id,b.company) t join (
         |select count(distinct(user_id)) as cu,product_id,company from dwd.dwd_product_user where
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr'  group by product_id,company
         |) c on t.product_id=c.product_id and t.company=c.company
         |join (
         |select t.product_id,t.company,count(distinct(t.device_id)) as tou_cu from (
         |select a.product_id,a.company,a.device_id,nvl(b.device_id,'0000') as nvlid from (
         |select device_id,company,product_id from dws.dws_uv_daily where count_date='$yestStr' group by device_id,company,product_id having nvl(max(active_user),'')='' --游客+今日新用户
         |) a left join (
         |select device_id,company,product_id from dws.dws_uv_increase where count_date='$yestStr' group by device_id,company,product_id having nvl(max(active_user),'')='' --今日新用户
         |) b on a.company=b.company and a.device_id=b.device_id and a.product_id=b.product_id
         |) t where t.nvlid='0000' group by t.product_id,t.company
         |) d on  t.product_id=d.product_id and t.company=d.company
      """.stripMargin

    spark.sql(insertSql)

  }

  //4 将Ads层UV相关数据写入PostgreSQL
  def writeAdsUvRelated2PostgreSQL(spark: SparkSession, yesStr: String): Unit = {

    val props = new java.util.Properties()
    props.setProperty("user","pgadmin")
    props.setProperty("password","szkf2019")
    props.setProperty("url","jdbc:postgresql://172.30.0.9:5432/bi")
    props.setProperty("tableName_1","ads_uv_daily")
    props.setProperty("tableName_2","ads_uv_increase")
    props.setProperty("tableName_3","ads_uv_total")
    props.setProperty("tableName_4","ads_uv_conversion")
    props.setProperty("write_mode","Append")

    //使用Ads库
    spark.sql("use ads")

    //ads_uv_daily
    val querySql_1 =
      s"""
         |select product_id,company,country,province,city,location,user_count,action_count,
         |session_count,count_date as mark_date from ads.ads_uv_daily where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_1).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_1"),props)

    //ads_uv_increase
    val querySql_2 =
      s"""
         |select product_id,company,country,province,city,location,user_count,action_count,
         |session_count,count_date as mark_date from ads.ads_uv_increase where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_2).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_2"),props)

    //ads_uv_total
    val querySql_3 =
      s"""
         |select product_id,company,country,province,city,location,user_count,action_count,
         |session_count,count_date as mark_date from ads.ads_uv_total where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_3).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_3"),props)

    //ads_uv_conversion
    val querySql_4 =
      s"""
         |select product_id,company,bus_reg,new_reg,tou_reg,new_reg_ratio,tou_active_reg_ratio,count_date as mark_date from ads.ads_uv_conversion where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_4).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_3"),props)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdsUvSummary").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //获取今日、昨天的日期
    val format = new SimpleDateFormat("yyyyMMdd")
    var withoutParameter = true
    if (args.length > 0) withoutParameter = false
    breakable{
      //参数内容校验 一次性对所有参数进行校验，若有非yyyyMMdd格式的参数，均不执行
      if (!withoutParameter) {
        for (i <- 0 until (if (args.length > 0) args.length else 1)) {
          if (None == "^[0-9]{8}$".r.findPrefixOf(args(i))) {
            break()
          }
        }
      }
      for (i <- 0 until (if (args.length > 0) args.length else 1)) {
        var today = new Date()
        if (!withoutParameter) {
          //如果带参数，重置today，以参数中的变量为today执行t-1业务
          today = format.parse(args(i).toString())
        }
        val cal = Calendar.getInstance
        cal.setTime(today)
        cal.add(Calendar.DATE, -1)
        if (!withoutParameter) {
          //按参数执行，执行参数当天的
          cal.add(Calendar.DATE, 1)
        }
        val yestStr: String = format.format(cal.getTime)
        //执行业务逻辑
        action(spark, yestStr)
      }
    }
    spark.stop()
  }

  def action(spark: SparkSession, yestStr: String): Unit = {
    //1 每日增量
    dwsUvDaily2AdsUvDaily(spark, yestStr)

    //2 历史累计的增量
    dwsUvTotal2AdsUvTotal(spark, yestStr)

    //3 每日新增统计
    dwsUvIncrease2AdsUvIncrease(spark, yestStr)

    //4 将Ads层UV相关数据写入PostgreSQL
    writeAdsUvRelated2PostgreSQL(spark,yestStr)
  }
}
