package com.pep.ads.uv

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.{Constants, DbProperties}
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
    spark.sql(createSql)

    val insertSql =
      s"""
         |insert overwrite table ads.ads_uv_daily partition(count_date)
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
         |insert overwrite table ads.ads_uv_total partition(count_date)
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
         |insert overwrite table ads.ads_uv_increase partition(count_date)
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
    * 方法4：每周活跃用户数和新增用户数
    */
  def writeDwsUvTotal2AdsUvAreaUntilWeek(spark: SparkSession, now: String): Unit = {

    //今天是周一，获取上周一
    val format = new SimpleDateFormat("yyyyMMdd")
    val parseTime = format.parse(now)
    val endWeekTs = parseTime.getTime
    val cal = Calendar.getInstance()
    cal.setTime(parseTime)
    cal.add(Calendar.DATE, -7)
    val beginWeekTs = cal.getTime.getTime

    cal.setTime(format.parse(now))
    cal.add(Calendar.DATE,-1)
    val todayStr = format.format(cal.getTime)

    spark.sql("use ads")

    val week_type = "week"
    val insertSql_1 =
      s"""
         |insert overwrite table ads_uv_area_until_week_month partition(count_date='${todayStr}')
         |select product_id,company,country,province,count(distinct(temp1.device_id))
         |as act_uv,'${week_type}' from (select product_id,company,country,province,device_id from dws.dws_uv_total where
         |(last_access_time>='${beginWeekTs}' and last_access_time<='${endWeekTs}' ) or
         |(first_access_time>='${beginWeekTs}' and first_access_time<='${endWeekTs}') )
         |as temp1 group by product_id,company,country,province
      """.stripMargin
    spark.sql(insertSql_1)

    val insertSql_2 =
      s"""
         |insert overwrite table ads_uv_incr_area_until_week_month partition(count_date='${todayStr}')
         |select temp1.product_id,temp1.company,temp1.country,temp1.province,count(distinct(temp1.device_id))
         |as inc_uv,'${week_type}' from (select product_id,company,country,province,device_id from dws.dws_uv_total where
         |first_access_time>='${beginWeekTs}' and first_access_time<='${endWeekTs}')
         |as temp1 group by product_id,company,country,province
      """.stripMargin
    spark.sql(insertSql_2)

    val insertSql_3 =
      s"""
         |insert overwrite table ads_puser_area_until_week_month partition(count_date='${todayStr}')
         |select product_id,company,country,province,count(distinct(temp1.device_id))
         |as act_uv,'${week_type}' from (select product_id,company,country,province,device_id from dws.dws_uv_total where
         |((last_access_time>='${beginWeekTs}' and last_access_time<='${endWeekTs}' ) or
         |(first_access_time>='${beginWeekTs}' and first_access_time<='${endWeekTs}')) and nvl(active_user,'')!='' )
         |as temp1 group by product_id,company,country,province
      """.stripMargin
    spark.sql(insertSql_3)

  }

  /**
    * 方法5：每月活跃用户数和新增用户数
    */
  def writeDwsUvTotal2AdsUvAreaUntilMonth(spark: SparkSession, now: String) = {

    //今天是月初第一天，获取上个月第一天
    val format = new SimpleDateFormat("yyyyMMdd")
    val parseTime = format.parse(now)
    val endMonthTs = parseTime.getTime
    val cal = Calendar.getInstance()
    cal.setTime(parseTime)
    cal.add(Calendar.MONTH, -1)
    val beginMonthTs = cal.getTime.getTime

    cal.setTime(format.parse(now))
    cal.add(Calendar.DATE,-1)
    val todayStr = format.format(cal.getTime)

    println(todayStr)
    println(beginMonthTs)
    println(endMonthTs)
    spark.sql("use ads")
    val month_type = "month"

    val insertSql_1 =
      s"""
         |insert overwrite table ads_uv_area_until_week_month partition(count_date='${todayStr}')
         |select product_id,company,country,province,count(distinct(temp1.device_id))
         |as act_uv,'${month_type}' from (select product_id,company,country,province,device_id from dws.dws_uv_total where
         |(last_access_time>='${beginMonthTs}' and last_access_time<='${endMonthTs}' ) or
         |(first_access_time>='${beginMonthTs}' and first_access_time<='${endMonthTs}') )
         |as temp1 group by product_id,company,country,province
      """.stripMargin
    spark.sql(insertSql_1)

    val insertSql_2 =
      s"""
         |insert overwrite table ads_uv_incr_area_until_week_month partition(count_date='${todayStr}')
         |select temp1.product_id,temp1.company,temp1.country,temp1.province,count(distinct(temp1.device_id))
         |as inc_uv,'${month_type}' from (select product_id,company,country,province,device_id from dws.dws_uv_total where
         |first_access_time>='${beginMonthTs}' and first_access_time<='${endMonthTs}')
         |as temp1 group by product_id,company,country,province
      """.stripMargin
    spark.sql(insertSql_2)


    val insertSql_3 =
      s"""
         |insert overwrite table ads_puser_area_until_week_month partition(count_date='${todayStr}')
         |select product_id,company,country,province,count(distinct(temp1.device_id))
         |as act_uv,'${month_type}' from (select product_id,company,country,province,device_id from dws.dws_uv_total where
         |((last_access_time>='${beginMonthTs}' and last_access_time<='${endMonthTs}' ) or
         |(first_access_time>='${beginMonthTs}' and first_access_time<='${endMonthTs}')) and nvl(active_user,'')!='' )
         |as temp1 group by product_id,company,country,province
      """.stripMargin
    spark.sql(insertSql_3)

  }


  /**
    * 方法6：近半年、近一个月的活跃注册用户统计
    *
    * @param spark
    */
  def writeDwsUvTotal2AdsActiveRegUser(spark: SparkSession, now: String, lastMonth: String, lastHalfYear: String) = {

    spark.sql("use ads")
    val format = new SimpleDateFormat("yyyyMMdd")
    val last_mon_st = format.parse(lastMonth).getTime
    val half_year_st = format.parse(lastHalfYear).getTime
    val last_day_st = format.parse(now).getTime


    val etlSql =
      s"""
         |insert overwrite table ads.ads_active_reg_user partition(count_date='${now}')
         |select * from
         |(select product_id as pp,company,country,province,'last_month',count(distinct(active_user)) as uv
         |from dws.dws_uv_total where last_access_time>='${last_mon_st}'
         |and nvl(active_user,'')!='' group by product_id,company,country,province) union all
         |(select product_id as pp2,company as ddd,country,province,'half_year',count(distinct(active_user)) as uv
         |from dws.dws_uv_total where last_access_time>='${half_year_st}'
         |and nvl(active_user,'')!='' group by product_id,company,country,province) union all
         |(select product_id as pp2,company as ddd,country,province,'today',count(distinct(active_user)) as uv
         |from dws.dws_uv_total where last_access_time>='${last_day_st}'
         |and nvl(active_user,'')!='' group by product_id,company,country,province)
      """.stripMargin
    spark.sql(etlSql)

  }

  //4 将Ads层UV相关数据写入PostgreSQL
  def writeAdsUvRelated2PostgreSQL(spark: SparkSession, yesStr: String,now :String): Unit = {

    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(now))
    cal.add(Calendar.DATE,-1)
    val todayStr = format.format(cal.getTime)

    val props = DbProperties.propScp
    props.setProperty("tableName_1", "ads_uv_daily")
    props.setProperty("tableName_2", "ads_uv_increase")
    props.setProperty("tableName_3", "ads_uv_total")
    props.setProperty("tableName_5", "ads_uv_incr_area_until_week_month")
    props.setProperty("tableName_6", "ads_uv_area_until_week_month")
    props.setProperty("tableName_7", "ads_active_reg_user")
    props.setProperty("tableName_8", "ads_puser_area_until_week_month")
    props.setProperty("write_mode", "Append")


    //使用Ads库
    spark.sql("use ads")

    //ads_uv_daily
    val querySql_1 =
      s"""
         |select product_id,company,country,province,city,location,user_count,action_count,
         |session_count,count_date as mark_date from ads.ads_uv_daily where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_1).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), props.getProperty("tableName_1"), props)

    //ads_uv_increase
    val querySql_2 =
      s"""
         |select product_id,company,country,province,city,location,user_count,action_count,
         |session_count,count_date as mark_date from ads.ads_uv_increase where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_2).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), props.getProperty("tableName_2"), props)

    //ads_uv_total
    val querySql_3 =
      s"""
         |select product_id,company,country,province,city,location,user_count,action_count,
         |session_count,count_date as mark_date from ads.ads_uv_total where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_3).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), props.getProperty("tableName_3"), props)


    val querySql_5 =
      s"""
         |select product_id,company,country,province,increa_count,row_type,count_date
         |from ads.ads_uv_incr_area_until_week_month where count_date='${todayStr}'
      """.stripMargin

    spark.sql(querySql_5).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), props.getProperty("tableName_5"), props)

    val querySql_6 =
      s"""
         |select product_id,company,country,province,user_count,row_type,count_date
         |from ads.ads_uv_area_until_week_month where count_date='${todayStr}'
      """.stripMargin

    spark.sql(querySql_6).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), props.getProperty("tableName_6"), props)


    val querySql_7 =
      s"""
         |select product_id,company,country,province,count_type,user_count,count_date
         |from ads.ads_active_reg_user where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_7).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), props.getProperty("tableName_7"), props)


    val querySql_8 =
      s"""
         |select product_id,company,country,province,user_count,row_type,count_date
         |from ads.ads_puser_area_until_week_month where count_date='${todayStr}'
      """.stripMargin

    spark.sql(querySql_8).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), props.getProperty("tableName_8"), props)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("AdsUvSummary").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
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
        //传入参数、或者不传参数都是执行的当天
        val now = format.format(cal.getTime)
        cal.add(Calendar.DATE, -1)
        if (!withoutParameter) {
          //按参数执行，执行参数当天的
          cal.add(Calendar.DATE, 1)
        }
        //传入参数就是当天
        //不传入参数就是昨天
        val real_today: String = format.format(cal.getTime)
        cal.add(Calendar.DATE, -30)
        val lastMonth = format.format(cal.getTime)
        cal.add(Calendar.DATE, 30)
        cal.add(Calendar.MONTH, -6)
        val lastHalfYear = format.format(cal.getTime)
        println(s"real_today : $real_today real_today :$real_today ")
        //执行业务逻辑
        action(spark, real_today, now, lastMonth, lastHalfYear)
      }
    }
    spark.stop()
  }


  def action(spark: SparkSession, todayStr: String, now: String, lastMonth: String, lastHalfYear: String): Unit = {

    createTable(spark)
    //1 每日增量
    dwsUvDaily2AdsUvDaily(spark, todayStr)

    //2 历史累计的增量
    dwsUvTotal2AdsUvTotal(spark, todayStr)

    //3 每日新增统计
    dwsUvIncrease2AdsUvIncrease(spark, todayStr)

    //方法4：每周活跃用户数和新增用户数
    if (judgeTodayDate(now, "week")) writeDwsUvTotal2AdsUvAreaUntilWeek(spark, now)

    //方法5：每月活跃用户数和新增用户数
    if (judgeTodayDate(now, "month")) writeDwsUvTotal2AdsUvAreaUntilMonth(spark, now)

    //方法6：近半年、近一个月的活跃注册用户统计
    writeDwsUvTotal2AdsActiveRegUser(spark, todayStr, lastMonth, lastHalfYear)

    //4 将Ads层UV相关数据写入PostgreSQL
    writeAdsUvRelated2PostgreSQL(spark ,todayStr,now)

  }

  //判断今天是不是月初 或者周一
  def judgeTodayDate(todayStr: String, dateType: String): Boolean = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val todayDate = format.parse(todayStr)
    val cal = Calendar.getInstance()
    cal.setTime(todayDate)
    if ("week".equals(dateType)) cal.get(Calendar.DAY_OF_WEEK) == 2
    else if ("month".equals(dateType)) cal.get(Calendar.DAY_OF_MONTH) == 1
    else false
  }

  def createTable(spark: SparkSession) = {
    spark.sql("use ads")

    val createSql_3 =
      """
        |create table if not exists ads_puser_area_until_week_month(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |user_count string,
        |row_type string
        |) partitioned by (count_date string) stored as parquet
      """.stripMargin
    spark.sql(createSql_3)

    val createSql_1 =
      """
        |create table if not exists ads_uv_area_until_week_month(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |user_count string,
        |row_type string
        |) partitioned by (count_date string) stored as parquet
      """.stripMargin
    spark.sql(createSql_1)

    val createSql_2 =
      """
        |create table if not exists ads_uv_incr_area_until_week_month(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |increa_count string,
        |row_type string
        |) partitioned by (count_date string) stored as parquet
      """.stripMargin
    spark.sql(createSql_2)


    val createSql =
      """
        |create table if not exists ads_active_reg_user(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |count_type string,
        |user_count string
        |) partitioned by (count_date string) stored as parquet
      """.stripMargin
    spark.sql(createSql)
  }

}
