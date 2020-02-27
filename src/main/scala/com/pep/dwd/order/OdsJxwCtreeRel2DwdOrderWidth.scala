package com.pep.dwd.order

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks


object OdsJxwCtreeRel2DwdOrderWidth {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-OdsJxwCtreeRel2DwdOrderWidth")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val regPatten = "^[0-9]{8}$".r
    val flag = args.length > 0
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -1)
    var yesStr = format.format(cal.getTime)

    loop.breakable {
      for (i <- 0 until (if (args.length > 1) args.length else 1)) {
        if (flag) {
          if (regPatten.findPrefixOf(args(i)) == None) loop.break()
          yesStr = args(i)
        }
        doAction(spark, yesStr, getBeforeDay(yesStr))
      }
      spark.stop()
    }
  }

  def getBeforeDay(timeStr:String) = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(timeStr))
    cal.add(Calendar.DATE,-1)
    format.format(cal.getTime)
  }


  def doAction(spark:SparkSession, yesStr:String, beforeYesStr:String): Unit ={
    writeOdsJxwCtreeRel2DwdOrderWidth(spark,yesStr,beforeYesStr)
  }

  def writeOdsJxwCtreeRel2DwdOrderWidth(spark: SparkSession, yesStr: String, beforeYesStr: String): Unit = {
    spark.sql("use dwd")
    val createSql =
      """
        |create table if not exists dwd_order_related_width(
        |order_id string,
        |detail_id string,
        |app_id string,
        |app_order_id string,
        |product_id string,
        |product_name string,
        |quantity string,
        |type string,
        |code string,
        |user_id string,
        |sale_channel_id string,
        |sale_channel_name string,
        |state string,
        |create_time string,
        |del_time string,
        |start_time string,
        |end_time string,
        |pay_time string,
        |discount string,
        |beans string,
        |material_code string,
        |material_name string,
        |pay_channel string,
        |pay_price string,
        |order_price string,
        |price string,
        |pay_tradeno string,
        |coupons string,
        |bean_type string,
        |remark string,
        |row_timestamp string,
        |row_status string,
        |authorization_way string,
        |count_date string )
        |stored as parquet
      """.stripMargin
    spark.sql(createSql)

    spark.sql("use ods")

    val createSql1 =
      """
        |CREATE EXTERNAL TABLE  if not exists  ods.ods_jxw_platform_user_ctree_rel(`id` string, `user_id` string, `user_name` string, `user_seting` string, `org_id` string, `org_name` string, `edu_code` string, `rkxd` string, `zxxkc` string, `publisher` string, `nj` string, `fascicule` string, `year` string, `keywords` string, `ctree_id` string, `ctree_name` string, `sub_heading` string, `s_state` string, `score` string, `s_version` string, `range_type` string, `ctree_related_object` string,
        | `view_numb`
        |string, `down_numb` string, `s_creator` string, `s_creator_name` string, `s_create_time` string, `valid_time` string, `authorization_code` string, `authorization_type` string, `authorization_way` string, `end_time` string, `reg_time` string, `row_timestamp` string, `row_status` string)
        |PARTITIONED BY (`count_date` string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        |WITH SERDEPROPERTIES (
        |  'serialization.format' = '1'
        |)
        |STORED AS
        |  INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        |OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
        |LOCATION '/pep_cloud/business/ods/ods_jxw_platform_user_ctree_rel'
      """.stripMargin
    spark.sql(createSql1)



    spark.sql("msck repair table ods_jxw_platform_user_ctree_rel")

    val insertSql =
      s"""
        |insert into table dwd.dwd_order_related_width
        |select id,NULL,'11120101',NULL,ctree_id,ctree_name,'1',NULL,
        |NULL,user_id,'110000006',NULL,s_state,s_create_time,NULL,NULL,
        |end_time,from_unixtime(unix_timestamp(split(s_create_time,' ')[0],'yyyy-MM-dd'),'yyyyMMdd'),NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
        |NULL,NULL,NULL,NULL,row_timestamp,row_status,authorization_way,'${yesStr}'
        |from
        |(select * from (
        |select *, row_number() over (partition by id order by row_timestamp desc ) num from ods.ods_jxw_platform_user_ctree_rel
        |) where num=1 and row_status='1')
        | where s_state='110' and count_date<='${yesStr}'
      """.stripMargin

    spark.sql(insertSql)
  }

}
