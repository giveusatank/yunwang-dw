package com.pep.ads.rj

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks



object DwdActionDoLog2AdsRjWebSummary {


  def writeDwdActionDoLog2AdsRjDpiDaily(spark: SparkSession, yestStr: String): Unit = {

    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_rj_dpi_daily
        |(
        |    product_id string,
        |    company    string,
        |    dpi_name   string,
        |    dpi_count  bigint,
        |    mark_date  string
        |)
        |    partitioned by (count_date string)
        |    stored as textfile
      """.stripMargin
    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_rj_dpi_daily drop if exists partition(count_date=${yestStr})")
    val insertSql =
      s"""
         |insert into ads_rj_dpi_daily partition (count_date)
         |select product_id,
         |       company,
         |       str_to_map(nvl(hardware, "dpi:null"), ',', ':')['dpi'],
         |       count(1),
         |       put_date,
         |       put_date
         |from dwd.action_do_log
         |where put_date = "$yestStr"
         |  and product_id = "300"
         |  and str_to_map(nvl(hardware, "dpi:null"), ',', ':')['dpi'] != 'null'
         |group by put_date, product_id, company, str_to_map(nvl(hardware, "dpi:null"), ',', ':')['dpi']
      """.stripMargin
    spark.sql(insertSql)
  }


  def writeDwdActionDoLog2AdsRjBrowserDaily(spark: SparkSession, yestStr: String): Unit = {

    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_rj_browser_daily
        |(
        |    product_id    string,
        |    company       string,
        |    browser_name  string,
        |    browser_count bigint,
        |    mark_date     string
        |)
        |    partitioned by (count_date string)
        |    stored as textfile
      """.stripMargin
    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_rj_browser_daily drop if exists partition(count_date=${yestStr})")
    val insertSql =
      s"""
         |insert into ads_rj_browser_daily partition (count_date)
         |select product_id,
         |       company,
         |       str_to_map(nvl(soft, "b-type:null"), '\\/', ':')['b-type'],
         |       count(1),
         |       put_date,
         |       put_date
         |from dwd.action_do_log
         |where put_date = "${yestStr}"
         |  and product_id = "300"
         |  and not (str_to_map(nvl(soft, "b-type:null"), '\\/', ':')['b-type'] = 'null' or soft = 'null' or soft = '')
         |group by put_date, product_id, company, str_to_map(nvl(soft, "b-type:null"), '\\/', ':')['b-type']
      """.stripMargin
    spark.sql(insertSql)
  }


  def writeDwsUVSessionDaily2AdsRjAccessCountDaily(spark: SparkSession, yestStr: String): Unit = {

    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_rj_access_count_daily
        |(
        |    product_id string,
        |    company    string,
        |    country    string,
        |    province   string,
        |    city       string,
        |    access_tie string,
        |    access_uv  bigint,
        |    mark_date  string
        |)
        |    partitioned by (count_date string)
        |    stored as textfile
      """.stripMargin
    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_rj_access_count_daily drop if exists partition(count_date=${yestStr})")
    val insertSql =
      s"""
         |insert into ads_rj_access_count_daily partition (count_date)
         |select t.product_id,
         |       t.company,
         |       t.country,
         |       t.province,
         |       t.city,
         |       t.tie,
         |       count(1),
         |       '${yestStr}',
         |       '${yestStr}'
         |from (select product_id,
         |             company,
         |             country,
         |             province,
         |             city,
         |             active_user,
         |             (case
         |                  when count(1) = 1 then 'A'
         |                  when count(1) = 2 then 'B'
         |                  when count(1) = 3
         |                      then 'C'
         |                  when count(1) = 4 then 'D'
         |                  else 'E' END) as tie
         |      from dws.dws_uv_session_daily
         |      where count_date = '${yestStr}'
         |        and product_id = '300'
         |      group by product_id, company, country, province, city, active_user) as t
         |group by t.product_id, t.company, t.country, t.province, t.city, t.tie
      """.stripMargin

    spark.sql(insertSql)
  }


  def writeDwsUVSessionDaily2AdsRjAccessBrowsePagesDaily(spark: SparkSession, yestStr: String): Unit = {

    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_rj_access_browse_pages_daily
        |(
        |    product_id        string,
        |    company           string,
        |    country           string,
        |    province          string,
        |    city              string,
        |    browse_tie        string,
        |    browse_page_count bigint,
        |    mark_date         string
        |)
        |    partitioned by (count_date string)
        |    stored as textfile
      """.stripMargin
    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_rj_access_browse_pages_daily drop if exists partition(count_date=${yestStr})")
    val insertSql =
      s"""
         |insert into ads_rj_access_browse_pages_daily partition (count_date)
         |select t.product_id,
         |       t.company,
         |       t.country,
         |       t.province,
         |       t.city,
         |       t.tie,
         |       count(1),
         |       '${yestStr}',
         |       '${yestStr}'
         |from (select product_id,
         |             company,
         |             country,
         |             province,
         |             city,
         |             (case
         |                  when count(1) <= 2 then 'A'
         |                  when count(1) <= 5 then 'B'
         |                  when count(1) <= 9
         |                      then 'C'
         |                  when count(1) <= 15 then 'D'
         |                  else 'E' END) as tie
         |      from dws.dws_uv_session_daily
         |      where count_date = '${yestStr}'
         |        and product_id = '300'
         |      group by product_id, company, country, province, city, group_id) as t
         |group by t.product_id, t.company, t.country, t.province, t.city, t.tie
       """.stripMargin
    spark.sql(insertSql)
  }


  def writeDwsUvDaily2AdsRjIpDaily(spark: SparkSession, yestStr: String): Unit = {

    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_rj_ip_daily
        |(
        |    product_id string,
        |    company    string,
        |    country    string,
        |    province   string,
        |    city       string,
        |    ip_count   bigint,
        |    mark_date  string
        |)
        |    partitioned by (count_date string)
        |    stored as textfile
      """.stripMargin
    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_rj_ip_daily drop if exists partition(count_date=${yestStr})")
    val insertSql =
      s"""
         |insert into ads_rj_ip_daily partition (count_date)
         |select product_id,
         |       company,
         |       country,
         |       province,
         |       city,
         |       count(distinct remote_addr),
         |       '${yestStr}',
         |       '${yestStr}'
         |from dws.dws_uv_daily
         |where count_date = "$yestStr"
         |  and product_id = "300"
         |group by product_id, company, country, province, city
      """.stripMargin
    spark.sql(insertSql)
  }


  def writeAdsUvDaily2AdsRjAccessStatisticDaily(spark: SparkSession, yestStr: String): Unit = {

    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_rj_access_statistic_daily
        |(
        |    product_id    string,
        |    company       string,
        |    country       string,
        |    province      string,
        |    city          string,
        |    sess_per_user bigint,
        |    acts_per_user bigint,
        |    acts_per_sess bigint,
        |    mark_date     string
        |)
        |    partitioned by (count_date string)
        |    stored as textfile
      """.stripMargin
    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_rj_access_statistic_daily drop if exists partition(count_date=${yestStr})")
    val insertSql =
      s"""
         |insert into ads_rj_access_statistic_daily partition (count_date)
         |select product_id,
         |       company,
         |       country,
         |       province,
         |       city,
         |       round(session_count / user_count),
         |       round(action_count / user_count),
         |       round(action_count / session_count),
         |       '${yestStr}',
         |       '${yestStr}'
         |from ads_uv_daily
         |where count_date = '$yestStr'
         |  and product_id = "300"
      """.stripMargin
    spark.sql(insertSql)
  }

  //将人教网相关的Ads层数据写入PostgreSQL
  def writeAdsRjWebRelated2PostgreSQL(spark: SparkSession, yestStr: String): Unit = {

    val props = new java.util.Properties()
    props.setProperty("user","pgadmin")
    props.setProperty("password","szkf2019")
    props.setProperty("url","jdbc:postgresql://172.30.0.9:5432/bi")
    props.setProperty("tableName_1","ads_rj_access_browse_pages_daily")
    props.setProperty("tableName_2","ads_rj_access_count_daily")
    props.setProperty("tableName_3","ads_rj_access_statistic_daily")
    props.setProperty("tableName_4","ads_rj_browser_daily")
    props.setProperty("tableName_5","ads_rj_dpi_daily")
    props.setProperty("tableName_6","ads_rj_ip_daily")
    props.setProperty("write_mode","Append")

    //使用Ads库
    spark.sql("use ads")

    //ads_rj_access_browse_pages_daily
    val querySql_1 =
      s"""
         |select product_id,company,country,province,city,browse_tie,browse_page_count,count_date
         |as mark_date from ads.ads_rj_access_browse_pages_daily where count_date='${yestStr}'
      """.stripMargin

    spark.sql(querySql_1).coalesce(10).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_1"),props)

    //ads_rj_access_count_daily
    val querySql_2 =
      s"""
         |select product_id,company,country,province,city,access_tie,access_uv as access_count,count_date
         |as mark_date from ads.ads_rj_access_count_daily where count_date='${yestStr}'
      """.stripMargin

    spark.sql(querySql_2).coalesce(10).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_2"),props)

    //ads_rj_access_statistic_daily
    val querySql_3 =
      s"""
         |select product_id,company,country,province,city,sess_per_user,acts_per_user,acts_per_sess,
         |count_date as mark_date from ads.ads_rj_access_statistic_daily where count_date='${yestStr}'
      """.stripMargin

    spark.sql(querySql_3).coalesce(10).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_3"),props)

    //ads_rj_browser_daily
    val querySql_4 =
      s"""
         |select product_id,company,browser_name,browser_count,count_date as mark_date
         |from ads.ads_rj_browser_daily where count_date='${yestStr}'
      """.stripMargin

    spark.sql(querySql_4).coalesce(10).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_4"),props)

    //ads_rj_dpi_daily
    val querySql_5 =
      s"""
         |select product_id,company,dpi_name,dpi_count,count_date as mark_date from ads.ads_rj_dpi_daily
         |where count_date='${yestStr}'
      """.stripMargin

    spark.sql(querySql_5).coalesce(10).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_5"),props)

    //ads_rj_ip_daily
    val querySql_6 =
      s"""
         |select product_id,company,country,province,city,ip_count,count_date as mark_date from ads.ads_rj_ip_daily
         |where count_date='${yestStr}'
      """.stripMargin

    spark.sql(querySql_6).coalesce(10).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_6"),props)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JOB-DwdActionDoLog2AdsRjWebSummary").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val arrLength = args.length
    val withParam = if (args.length > 0) true else false
    val regPattern = "^[0-9]{8}$".r
    val loop = new Breaks
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -1)
    var yesToday: String = format.format(cal.getTime)

    //如果没传入参数,就是Azkaban调度的,操作Yestoday数据,插入的分区也是Yestoday
    //如果传入参数,就是手动调度的,操作的是传入时间的数据,插入分区也是传入时间
    loop.breakable {
      for (i <- 0 until (if (withParam) {
        arrLength
      } else 1)) {
        //是手动传参
        if (withParam) {
          //使用正则判断传入日期是否合法
          val res: Option[String] = regPattern.findPrefixOf(args(i))
          //如果输入参数非法,则跳出循环.否则将yesToday时间替换为传入的参数
          if (res == None) {
            loop.break()
          } else {
            //传入20号 20 19 18 17 16 15 14
            yesToday = args(i)
            cal.setTime(format.parse(yesToday))
          }
        }
        doAction(spark, yesToday)
      }
      spark.stop()
    }
    spark.stop()
  }

  def doAction(spark: SparkSession, yestStr: String): Unit = {

    //统计每日的Dpi使用情况
    writeDwdActionDoLog2AdsRjDpiDaily(spark,yestStr)

    //统计每日的浏览器使用情况
    writeDwdActionDoLog2AdsRjBrowserDaily(spark,yestStr)

    //统计每日用户浏览次数
    writeDwsUVSessionDaily2AdsRjAccessCountDaily(spark,yestStr)

    //统计每个会话 访问的页面情况
    writeDwsUVSessionDaily2AdsRjAccessBrowsePagesDaily(spark,yestStr)

    //统计每天的独立Ip数
    writeDwsUvDaily2AdsRjIpDaily(spark,yestStr)

    //统计  人均访问次数：session / uv
    //      人均浏览次数：action_count / uv
    //      平均每次浏览数：action_count / session_count
    writeAdsUvDaily2AdsRjAccessStatisticDaily(spark,yestStr)

    //将人教网相关的Ads层数据写入PostgreSQL
    writeAdsRjWebRelated2PostgreSQL(spark,yestStr)
  }
}
