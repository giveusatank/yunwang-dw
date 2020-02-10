package com.pep.dwd

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object OdsOriginalActionLog2DwdActionDoLog {

  //获取当前时间的前一天
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JOB-OdsOriginalActionLog2DwdActionDoLog").set("spark.sql.shuffle.partitions", Constants.dws_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val ym = args(0)
    val ymdStart = ym + "00"
    val ymdEnd = ym + "32"
    action(spark, ymdStart, ymdEnd,ym)
    spark.stop()

  }

  def action(spark: SparkSession, ymdStart: String, ymdEnd: String,ym :String): Unit = {
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
         |            where put_date > '$ymdStart' and put_date< '$ymdEnd'
         |              and start_time is not null
         |              and not (product_id = '1213' and action_title = 'sys_100001')
         |              and from_unixtime(cast(substring(start_time, 1, 10) as bigint), 'yyyyMM') ='$ym' distribute by substring(start_time, 8, 10))
         |               as tmp)
         |         as t
         |where t.num = 1
       """.stripMargin
    spark.sql(sql)

  }

}
