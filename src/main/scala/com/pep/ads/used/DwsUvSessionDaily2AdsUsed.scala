package com.pep.ads.used

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.{Constants, DbProperties}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks.{break, breakable}

object DwsUvSessionDaily2AdsUsed {


  def DwsUvSessionDaily2AdsUsed(spark: SparkSession, yestStr: String) = {
    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_used_session(
        |product_id                string,
        |company                   string,
        |province                  string,
        |cu_group                  string,
        |cu_device                 string,
        |sum_used_time             string,
        |avg_group                 string,
        |avg_user_used_time        string,
        |avg_session_used_time     string,
        |user_type                 string,
        |gid                       string,
        |week                      string
        |) partitioned by (count_date string)
        |stored as textfile
      """.stripMargin
    spark.sql(createSql)
    spark.sql("msck repair table dws.dws_uv_session_daily")


    val insertSql =
      s"""
         |insert overwrite table ads.ads_used_session partition(count_date)
         |select product_id,company,province,
         |sum(cu_group) as cu_group,
         |count(device_id) as cu_device,
         |sum(used_time) as sum_used_time,
         |round(sum(cu_group)/count(device_id),2) as avg_group ,
         |round(sum(used_time)/count(device_id),2) as avg_user_used_time ,
         |round(sum(used_time)/sum(cu_group),2) as avg_session_used_time ,
         |user_type,
         |grouping_id() as gid,
         |dws.dateUtilUDF('week',unix_timestamp('$yestStr', 'yyyyMMdd')) as week,
         |'$yestStr'
         |from(
         |select product_id,company,province,
         |sum(launch_count) as cu_group,
         |sum(launch_used_time) as used_time,
         |device_id ,
         |if(nvl(max(active_user),'')='',0,1) as user_type
         |from dws.dws_uv_session_daily where count_date='$yestStr' and country='中国'
         |and nvl(first_access_time,'')!='' and nvl(last_access_time,'')!='' and nvl(device_id,'')!='' and lower(device_id)!='null'
         |group by product_id,company,province,device_id
         |)t
         |group by product_id,user_type,company,province
         |grouping sets ((product_id,user_type,company,province),(product_id,user_type,company),(product_id,user_type,province),(product_id,user_type))
      """.stripMargin

    spark.sql(insertSql)

    val createSql1 =
      """
        |create table if not exists ads_used_session_user(
        |product_id                     string,
        |company                        string,
        |province                       string,
        |user_type                      string,
        |cu_group                       string,
        |cu_group_name                  string,
        |avg_user_used_time             string,
        |avg_user_used_time_name        string,
        |avg_session_used_time          string,
        |avg_session_used_time_name     string,
        |cu_device                      string,
        |gid                            string,
        |week                           string
        |) partitioned by (count_date string)
        |stored as textfile
      """.stripMargin
    spark.sql(createSql1)
    spark.sql("msck repair table dws.dws_uv_session_daily")

    val insertSql1 =
      s"""
         |insert overwrite table ads.ads_used_session_user partition(count_date)
         |select product_id,company,province,user_type,sum(cu_group) as cu_group,
         |case when cu_group  > 5 then '5+' else cast(cu_group as string) end as cu_group_name,
         |round(avg(avg_user_used_time),0) as avg_user_used_time,
         |dws.GroupNameUDF('avg_user_used_time', avg_user_used_time) as avg_user_used_time_name ,
         |round(avg(avg_session_used_time),0) as avg_session_used_time,
         |dws.GroupNameUDF('avg_user_used_time', avg_session_used_time) as avg_session_used_time_name,
         |count(distinct(device_id)) as cu_device,
         |grouping_id() as gid,
         |dws.dateUtilUDF('week',unix_timestamp('$yestStr', 'yyyyMMdd')) as week,
         |'$yestStr'
         |from (
         |select product_id,company,province,
         |sum(launch_count) as cu_group,
         |sum(launch_used_time) as avg_user_used_time,
         |round(sum(launch_used_time)/sum(launch_count),0) as avg_session_used_time,
         |device_id ,
         |if(nvl(max(active_user),'')='',0,1) as user_type
         |from dws.dws_uv_session_daily where count_date='$yestStr' and country='中国'
         |and nvl(first_access_time,'')!='' and nvl(last_access_time,'')!='' and nvl(device_id,'')!='' and lower(device_id)!='null'
         |group by product_id,company,province,device_id
         |) t
         |group by
         |product_id,user_type,company,province ,
         |case when cu_group  > 5 then '5+' else cast(cu_group as string) end,
         |dws.GroupNameUDF('avg_user_used_time', avg_user_used_time) ,
         |dws.GroupNameUDF('avg_user_used_time', avg_session_used_time)
         |grouping sets (
         |(product_id,user_type,case when cu_group  > 5 then '5+' else cast(cu_group as string) end,company,province),
         |(product_id,user_type,case when cu_group  > 5 then '5+' else cast(cu_group as string) end,company),
         |(product_id,user_type,case when cu_group  > 5 then '5+' else cast(cu_group as string) end,province),
         |(product_id,user_type,case when cu_group  > 5 then '5+' else cast(cu_group as string) end ),
         |(product_id,user_type,dws.GroupNameUDF('avg_user_used_time', avg_user_used_time),company,province),
         |(product_id,user_type,dws.GroupNameUDF('avg_user_used_time', avg_user_used_time),company),
         |(product_id,user_type,dws.GroupNameUDF('avg_user_used_time', avg_user_used_time),province),
         |(product_id,user_type,dws.GroupNameUDF('avg_user_used_time', avg_user_used_time) ),
         |(product_id,user_type,dws.GroupNameUDF('avg_user_used_time', avg_session_used_time),company,province),
         |(product_id,user_type,dws.GroupNameUDF('avg_user_used_time', avg_session_used_time),company),
         |(product_id,user_type,dws.GroupNameUDF('avg_user_used_time', avg_session_used_time),province),
         |(product_id,user_type,dws.GroupNameUDF('avg_user_used_time', avg_session_used_time))
         |)
      """.stripMargin
    spark.sql(insertSql1)


    val selectSql =
      s"""
        |select * from ads_used_session where count_date='${yestStr}'
      """.stripMargin
    val readDate = spark.sql(selectSql)
    var writeDF = readDate.coalesce(5)

    val selectSql1 =
      s"""
         |select * from ads_used_session_user where count_date='${yestStr}'
      """.stripMargin
    val readDate1 = spark.sql(selectSql1)
    var writeDF1 = readDate1.coalesce(5)

    val props = DbProperties.propScp
    writeDF.write.format("jdbc").
      mode("append").
      jdbc(props.getProperty("url"),"ads_used_session",props)

    writeDF1.write.format("jdbc").
      mode("append").
      jdbc(props.getProperty("url"),"ads_used_session_user",props)


  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DwsUvSessionDaily2AdsUsed").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
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
    DwsUvSessionDaily2AdsUsed(spark, yestStr)

  }
}
