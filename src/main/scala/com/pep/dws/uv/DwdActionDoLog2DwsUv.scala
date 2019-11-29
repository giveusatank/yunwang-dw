package com.pep.dws.uv

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks._

object DwdActionDoLog2DwsUv {


  def writeActionDoLog2DwsUvSessionDaily(spark: SparkSession, yestStr: String): Unit = {
    spark.sql("use dws")
    val createSql =
      """
        |create table if not exists dws_uv_session_daily(
        |product_id string,
        |company string,
        |remote_addr string,
        |country string,
        |province string,
        |city string,
        |location string,
        |region string,
        |active_user string,
        |device_id string,
        |group_id string,
        |group_actions string,
        |first_access_time string,
        |last_access_time string,
        |action_count bigint,
        |launch_used_time string,
        |launch_count string
        |)
        |partitioned by (count_date int)
        |STORED AS parquet
      """.stripMargin
    spark.sql(createSql)
    //启动次数统计launch_count: 访问会话数和同一会话内如果超过15分钟发生访问间隔为新启动。
    val _sql1 =
      s"""
         |insert overwrite table dws.dws_uv_session_daily partition (count_date)
         |select product_id,
         |       dws.yunwangDateFormat('company',company) as company,
         |       split(max(concat(start_time, '-', remote_addr)), '-')[1] as remote_addr,
         |       split(max(concat(start_time, '-', country)), '-')[1] as country,
         |       split(max(concat(start_time, '-', province)), '-')[1] as province ,
         |       split(max(concat(start_time, '-', city)), '-')[1] as city,
         |       split(max(concat(start_time, '-', location)), '-')[1] as location,
         |       split(max(concat(start_time, '-', region)), '-')[1] as region,
         |       active_user,
         |       device_id,
         |       group_id,
         |       ''              as group_actions,
         |       min(start_time) as first_access_time,
         |       max(start_time) as last_access_time,
         |       count(1)        as action_count,
         |       round(dws.productTimeUsed(str_to_map(concat_ws(",", collect_set(concat_ws(':', cast(start_time as string), cast(action_title as string))))),900,0)/1000,0)  as launch_used_time,
         |       dws.productTimeUsed(str_to_map(concat_ws(",", collect_set(concat_ws(':', cast(start_time as string), cast(action_title as string))))),900,2)  as launch_count,
         |       put_date
         |from dwd.action_do_log
         |where put_date = '$yestStr' and nvl(product_id,'')!='' and nvl(device_id,'')!='' and not(action_title like 'sys_1%' or action_title like 'sys_4%')
         |group by product_id, remote_addr, dws.yunwangDateFormat('company',company),
         |country, province, city, location,region, active_user, device_id, group_id, put_date
     """.stripMargin

    spark.sql(_sql1)
  }

  /**
    * 每日UV PV
    *
    * @param spark
    * @param yestStr
    */
  def writeActionDoLog2DwsUvDaily(spark: SparkSession, yestStr: String): Unit = {
    spark.sql("use dws")
    val createSql =
      """
        |create table if not exists dws_uv_daily(
        |product_id string,
        |company string,
        |remote_addr string,
        |country string,
        |province string,
        |city string,
        |location string,
        |region string,
        |active_user string,
        |device_id string,
        |first_access_time string,
        |last_access_time string,
        |action_count bigint,
        |session_count bigint,
        |launch_used_time string,
        |launch_count string
        |)
        |partitioned by (count_date int)
        |STORED AS parquet
      """.stripMargin
    spark.sql(createSql)
    val _sql1 =
      s"""
         |insert overwrite table dws.dws_uv_daily partition (count_date)
         |select product_id,
         |       company,
         |       split(max(concat(last_access_time, '-', remote_addr)), '-')[1] as remote_addr,
         |       split(max(concat(last_access_time, '-', country)), '-')[1] as country,
         |       split(max(concat(last_access_time, '-', province)), '-')[1] as province ,
         |       split(max(concat(last_access_time, '-', city)), '-')[1] as city,
         |       split(max(concat(last_access_time, '-', location)), '-')[1] as location,
         |       split(max(concat(last_access_time, '-', region)), '-')[1] as region,
         |       active_user,
         |       device_id,
         |       min(first_access_time) as first_access_time,
         |       max(last_access_time)  as last_access_time,
         |       sum(action_count)      as action_count,
         |       count(1)               as session_count,
         |       sum(launch_used_time),
         |       sum(launch_count),
         |       count_date
         |from dws.dws_uv_session_daily
         |where count_date = '$yestStr'
         |group by product_id, remote_addr, company, country, province, city,
         |location,region, active_user, device_id, count_date
         |
      """.stripMargin

    spark.sql(_sql1)
  }

  /**
    * 历史累计uv,每日一算，保留7日
    *
    * @param spark
    * @param yestStr
    * @param _7DaysBefore
    */
  def writeDwsUvDaily2DwsUvTotal(spark: SparkSession, yestStr: String, _7DaysBefore: String): Unit = {
    spark.sql("use dws")

    val createSql =
      """
        |create table if not exists dws_uv_total(
        |product_id string,
        |company string,
        |remote_addr string,
        |country string,
        |province string,
        |city string,
        |location string,
        |region string,
        |active_user string,
        |device_id string,
        |first_access_time string,
        |last_access_time string,
        |action_count bigint,
        |session_count bigint,
        |launch_used_time string,
        |launch_count string,
        |count_date int
        |)
        |STORED AS parquet
      """.stripMargin

    spark.sql(createSql)

    //1.插入增量数据
    val insertSql =
      s"""
         |insert overwrite table dws.dws_uv_total
         |select product_id,
         |       company,
         |       split(max(concat(last_access_time, '-', remote_addr)), '-')[1] as last_remote_addr,
         |       split(max(concat(last_access_time, '-', country)), '-')[1] as last_country,
         |       split(max(concat(last_access_time, '-', province)), '-')[1] as last_province ,
         |       split(max(concat(last_access_time, '-', city)), '-')[1] as last_city,
         |       split(max(concat(last_access_time, '-', location)), '-')[1] as last_location,
         |       split(max(concat(last_access_time, '-', region)), '-')[1] as last_region,
         |       active_user,
         |       device_id,
         |       min(first_access_time)                                         as first_access_time,
         |       max(last_access_time)                                          as last_access_time,
         |       sum(action_count)                                              as action_count,
         |       sum(session_count)                                             as session_count,
         |       sum(launch_used_time)                                          as action_count,
         |       sum(launch_count)                                              as session_count,
         |       '$yestStr'
         |from dws.dws_uv_daily
         |group by product_id, company, active_user, device_id
      """.stripMargin

    spark.sql(insertSql)

    /*
    //2.修改标明 dws_uv_total_20190504
    val dropYestStrSql =
      s"""
         |drop table if exists dws_uv_total_${yestStr}
      """.stripMargin
    spark.sql(dropYestStrSql)

    val alterSql =
      s"""
         |alter table dws_uv_total  rename to dws_uv_total_$yestStr
      """.stripMargin
    spark.sql(alterSql)

    //3.再创建total表,去重计算后插入total表 最后一次访问IP
    spark.sql(createSql)

    val insertTotalSql =
      s"""
         |insert into  dws.dws_uv_total
         |select product_id,
         |       company,
         |       split(max(concat(last_access_time, '-', remote_addr)), '-')[1] as last_remote_addr,
         |       split(max(concat(last_access_time, '-', country)), '-')[1] as last_country,
         |       split(max(concat(last_access_time, '-', province)), '-')[1] as last_province ,
         |       split(max(concat(last_access_time, '-', city)), '-')[1] as last_city,
         |       split(max(concat(last_access_time, '-', location)), '-')[1] as last_location,
         |       split(max(concat(last_access_time, '-', region)), '-')[1] as last_region,
         |       active_user,
         |       device_id,
         |       min(first_access_time) as first_access_time,
         |       max(last_access_time)  as last_access_time,
         |       sum(action_count)      as action_count,
         |       sum(session_count)     as session_count,
         |       '$yestStr'
         |from dws.dws_uv_total_$yestStr
         |group by product_id, company, active_user, device_id
      """.stripMargin
    spark.sql(insertTotalSql)

    //4. 默认规则就是保留7天
    val dropSql =
      s"""
         |drop table if exists dws_uv_total_${_7DaysBefore}
      """.stripMargin
    spark.sql(dropSql)
    */


  }

  /**
    * 新增用户表
    *
    * @param spark
    * @param yestStr
    */
  def writeDwsUvDaily2DwsUvIncrease(spark: SparkSession, yestStr: String): Unit = {
    spark.sql("use dws")

    val createSql =
      """
        |create table if not exists dws_uv_increase(
        |product_id string,
        |company string,
        |remote_addr string,
        |country string,
        |province string,
        |city string,
        |location string,
        |region string,
        |active_user string,
        |device_id string,
        |first_access_time string,
        |last_access_time string,
        |action_count bigint,
        |session_count bigint
        |)
        |partitioned by (count_date int)
        |STORED AS parquet
      """.stripMargin

    spark.sql(createSql)
    //1.插入增量数据
    val insertSql =
      s"""
         |insert overwrite table dws.dws_uv_increase partition (count_date)
         |select product_id,
         |       company,
         |       remote_addr,
         |       country,
         |       province,
         |       city,
         |       location,
         |       region ,
         |       active_user,
         |       device_id,
         |       first_access_time,
         |       last_access_time,
         |       action_count,
         |       session_count,
         |       '$yestStr'
         |from dws.dws_uv_total
         |where from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd') = '$yestStr'
      """.stripMargin
    spark.sql(insertSql)
  }


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JOB-DwdActionDoLog2DwsUV").set("spark.sql.shuffle.partitions", Constants.dws_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //获取今日、昨天的日期
    val format = new SimpleDateFormat("yyyyMMdd")
    var withoutParameter = true
    if (args.length > 0) withoutParameter = false

    breakable {
      //参数内容校验 一次性对所有参数进行校验，若有非yyyyMMdd格式的参数，均不执行
      if (!withoutParameter) {
        for (i <- 0 until (if (args.length > 0) args.length else 1)) {
          if (None == "^[0-9]{8}$".r.findPrefixOf(args(i))) {
            println("日期校验错误"+ args(i))
            break()
          }
        }
      }
      for (i <- 0 until (if (args.length > 0) args.length else 1)) {
        var today = new Date()
        if (!withoutParameter) {
          today = format.parse(args(i).toString())
        }
        val todayStr = format.format(today)
        val cal = Calendar.getInstance
        cal.setTime(today)
        cal.add(Calendar.DATE, -1)
        if (!withoutParameter) {
          //按参数执行，执行参数当天的
          cal.add(Calendar.DATE, 1)
        }
        val yestStr: String = format.format(cal.getTime)
        cal.add(Calendar.DATE, -6)
        val _7DaysBefore: String = format.format(cal.getTime)
        //执行业务逻辑
        action(spark, yestStr, todayStr, _7DaysBefore)
      }
    }
    spark.stop()
  }

  def action(spark: SparkSession, yestStr: String, todayStr: String, _7DaysBefore: String): Unit = {
    //方法0：将ActionDoLog数据洗到DwsUvSessionDaily
    writeActionDoLog2DwsUvSessionDaily(spark, yestStr)

    //方法1：将DwsUvSessionDaily数据洗到DwsUvDaily
    writeActionDoLog2DwsUvDaily(spark, yestStr)

    //方法2：将DwsUvDaily洗到DwsUvTotal中
    writeDwsUvDaily2DwsUvTotal(spark, yestStr, _7DaysBefore)

    //方法3：新增用户表
    writeDwsUvDaily2DwsUvIncrease(spark, yestStr)
  }
}
