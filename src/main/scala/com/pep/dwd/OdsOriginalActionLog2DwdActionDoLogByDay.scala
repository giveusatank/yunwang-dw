package com.pep.dwd

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * 参数说明：
  * args(0):reserveDateNum  清洗过程中保留多少天
  * args(1...n): 指定清洗日期清洗，
  */
object OdsOriginalActionLog2DwdActionDoLogByDay {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JOB-OdsOriginalActionLog2DwdActionDoLogByDay").set("spark.sql.shuffle.partitions", Constants.dws_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //获取当前时间的前一天
    val format = new SimpleDateFormat("yyyyMMdd")
    var startDate = new Date()
    var reserveDateNum = 1
    val reg = "^[0-9]{1,2}$".r
    if (None != reg.findPrefixOf(args(0))) {
      reserveDateNum = Integer.valueOf(args(0))
      for (i <- 1 until (if (args.length > 1) args.length else 2)) {
        if (args.length > 1 && args(i).toString.length == 8) {
          startDate = format.parse(args(i).toString())
        }
        val calendar = Calendar.getInstance()
        calendar.setTime(startDate)
        calendar.add(Calendar.DATE, -1)
        if (args.length > 1) {
          //传参数执行，put_date时间为当天
          calendar.add(Calendar.DATE, 1)
        }
        //获取昨天的时间
        val yesterdayStr = format.format(calendar.getTime)
        //获取昨天的最近三天时间
        val reserveDateStr = getReserveDate(yesterdayStr, reserveDateNum)
        //执行任务
        action(spark, yesterdayStr)
      }
    }
    spark.stop()

  }
  def action(spark: SparkSession, ymdStart: String): Unit = {
    //使用dwd数据库
    spark.sql("use dwd")
    //刷新一下action_log_ot的分区信息
    spark.sql("msck repair table ods.original_action_log")
    val sql =
      s"""
         |insert overwrite table action_do_log PARTITION (put_date)
         |select *
         |from (select '',
         |             remote_addr,
         |             country,
         |             province,
         |             city,
         |             location,
         |             request_time,
         |             log_version,
         |             start_time,
         |             end_time,
         |             region,
         |             if(company=='pep_click','1214' ,product_id) as product_id,
         |             os,
         |             soft,
         |             hardware,
         |             device_id,
         |             active_user,
         |             active_org,
         |             active_type,
         |             passive_obj,
         |             passive_type,
         |             from_prod,
         |             from_pos,
         |             company,
         |             action_title,
         |             action_type,
         |             request,
         |             request_param,
         |             group_type,
         |             group_id,
         |             result_flag,
         |             result,
         |             row_number() over (partition by group_id,action_title,start_time order by start_time,action_title,group_id) num,
         |             put_date
         |      from (select '',
         |                   remote_addr,
         |                   split(dwd.ip2region(regexp_extract(remote_addr, '(?<=(\\\\[))\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}', 0)), '~')[0] as country,
         |                   split(dwd.ip2region(regexp_extract(remote_addr, '(?<=(\\\\[))\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}', 0)), '~')[1] as province,
         |                   split(dwd.ip2region(regexp_extract(remote_addr, '(?<=(\\\\[))\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}', 0)), '~')[2] as city,
         |                   split(dwd.ip2region(regexp_extract(remote_addr, '(?<=(\\\\[))\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}\\\\.\\\\d{1,3}', 0)), '~')[3] as location,
         |                   request_time,
         |                   log_version,
         |                   start_time,
         |                   end_time,
         |                   region,
         |                   product_id,
         |                   if(instr(os, 'Windows') != 0, os, if(log_version > 1, os, null))                                                                       os,
         |                   soft                                                                                                                                   soft,
         |                   if(instr(os, 'Windows') != 0, hardware, if(log_version > 1, hardware, null))                                                           hardware,
         |                   if(instr(os, 'Windows') != 0, nvl(str_to_map(hardware, ',', ':')['deviceId'], split(hardware, ',')[6]),
         |                      if(log_version > 1, if(nvl(str_to_map(os, ',', ':')['deviceId'], '') != '', str_to_map(os, ',', ':')['deviceId'], group_id),
         |                         if(length(regexp_extract(os, '(?<=(DeviceId\\\\(IMEI\\\\)\\\\:))\\\\d+', 0)) != 0, regexp_extract(os, '(?<=(DeviceId\\\\(IMEI\\\\)\\\\:))\\\\d+', 0),
         |                            if(length(group_id) != 13, group_id, ''))))                                                                                as device_id,
         |                   active_user,
         |                   active_org,
         |                   active_type,
         |                   passive_obj,
         |                   passive_type,
         |                   from_prod,
         |                   from_pos,
         |                   if(company=='c1860a61705f36538055c0955c327079','110000006' ,company) as company,
         |                   action_title,
         |                   action_type,
         |                   request,
         |                   request_param,
         |                   group_type,
         |                   group_id,
         |                   result_flag,
         |                   result,
         |                   from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyyMMdd')                                                             as put_date
         |            from ods.original_action_log
         |            where put_date = '$ymdStart'
         |              and start_time is not null
         |              and not (product_id = '1213' and action_title = 'sys_100001')
         |              and from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyyMMdd') ='$ymdStart' distribute by substring(start_time, 8, 10))
         |               as tmp)
         |         as t
         |where t.num = 1
       """.stripMargin
    spark.sql(sql)

  }

  def getReserveDate(putDate: String, dateSize: Int): String = {
    val sdf = new SimpleDateFormat("yyyyMMdd")
    val data = sdf.parse(putDate)
    val putDateArray = new Array[String](dateSize)
    val cal: Calendar = Calendar.getInstance()
    cal.setTime(data)
    for (i <- 1 to dateSize) {
      if (i > 1) {
        cal.add(Calendar.DATE, -1)
      }
      val putDateStr = sdf.format(cal.getTime)
      putDateArray(i - 1) = putDateStr
    }
    var putDateStr = ""
    for (i <- 0 to putDateArray.length - 1) {
      putDateStr = putDateStr + "'" + putDateArray(i) + "',"
    }
    putDateStr.substring(0, putDateStr.length - 1)
  }
}
