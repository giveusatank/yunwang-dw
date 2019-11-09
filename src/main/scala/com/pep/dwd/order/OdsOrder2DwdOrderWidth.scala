package com.pep.dwd.order

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

object OdsOrder2DwdOrderWidth {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-OdsOrder2DwdOrderWidth")
      /*.set("spark.sql.shuffle.partitions","30")*/

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
        doAction(spark,yesStr,getBeforeDay(yesStr))
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

  def doAction(spark:SparkSession, yesStr:String,beforeYesStr:String): Unit ={

    writeOdsOrderInfoAndDetail2DwdOrderWidth(spark,yesStr,beforeYesStr)
  }

  def writeOdsOrderInfoAndDetail2DwdOrderWidth(spark: SparkSession, yesStr: String,beforeYesStr:String): Unit = {

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

    val createSqlOrderInfo =
      """
        |CREATE TABLE if not exists ods.ods_order_info(`id` string COMMENT 'from deserializer', `app_id` string COMMENT 'from deserializer', `app_order_id` string COMMENT 'from deserializer', `user_id` string COMMENT 'from deserializer', `user_name` string COMMENT 'from deserializer', `sale_channel_id` string COMMENT 'from deserializer', `sale_channel_name` string COMMENT 'from deserializer', `s_state` string COMMENT 'from deserializer', `s_create_time` string COMMENT 'from deserializer',
        |`s_delete_time`
        |string COMMENT 'from deserializer', `order_price` string COMMENT 'from deserializer', `discount` string COMMENT 'from deserializer', `pay_channel` string COMMENT 'from deserializer', `pay_time` string COMMENT 'from deserializer', `pay_price` string COMMENT 'from deserializer', `pay_tradeno` string COMMENT 'from deserializer', `remark` string COMMENT 'from deserializer', `beans` string COMMENT 'from deserializer', `bean_type` string COMMENT 'from deserializer', `coupons` string COMMENT 'from deserializer', `row_timestamp` string COMMENT 'from deserializer', `row_status` string COMMENT 'from deserializer')
        |    PARTITIONED BY (`count_date` string)
        |    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        |    WITH SERDEPROPERTIES (
        |      'serialization.format' = '1'
        |    )
        |    STORED AS
        |      INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        |    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      """.stripMargin
    spark.sql(createSqlOrderInfo)

    val createSqlOrderDetail =
      """
        |CREATE TABLE if not exists ods.ods_order_detail(`id` string COMMENT 'from deserializer', `app_id` string COMMENT 'from deserializer', `app_order_id` string COMMENT 'from deserializer', `product_id` string COMMENT 'from deserializer', `product_name` string COMMENT 'from deserializer', `price` string COMMENT 'from deserializer',
        | `quantity` string COMMENT 'from deserializer', `type` string COMMENT 'from deserializer', `code` string COMMENT 'from deserializer', `start_time` string COMMENT
        |'from
        |deserializer', `end_time` string COMMENT 'from deserializer', `beans` string COMMENT 'from deserializer', `materiel_code` string COMMENT 'from deserializer', `materiel_name` string COMMENT 'from deserializer', `row_timestamp` string COMMENT 'from deserializer', `row_status` string COMMENT 'from deserializer')
        |    PARTITIONED BY (`count_date` string)
        |    ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
        |    WITH SERDEPROPERTIES (
        |      'serialization.format' = '1'
        |    )
        |    STORED AS
        |      INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat'
        |    OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
      """.stripMargin
    spark.sql(createSqlOrderDetail)

    spark.sql("msck repair table ods_order_info")
    spark.sql("msck repair table ods_order_detail")

    val insertSql =
      s"""
         |insert into dwd.dwd_order_related_width
         |select tt.infoId,tt.detailId,if(tt.company='pep_click','121301',tt.appId),tt.appOrderId,dws.yunwangdateformat("tbid",trim(tt.productId)),tt.productName,
         |tt.quantity,tt.type,tt.code,tt.userId,tt.company,tt.companyName,tt.state,tt.create_time,
         |tt.del_time,tt.start_time,tt.end_time,tt.pay_time,tt.discount,tt.beans,tt.materielCode,
         |tt.materielName,tt.payChannel,tt.payPrice,tt.orderPrice,tt.price,tt.payTradeno,tt.coupons,
         |tt.beanType,tt.remark,tt.rt,tt.rs,'01','${yesStr}' from (select in.id as infoId,de.id as detailId,dws.yunwangdateformat("order",in.app_id) as appId,
         |in.app_order_id as appOrderId,de.product_id as productId,de.product_name as productName,de.quantity as quantity,
         |de.type as type,de.code as code,in.user_id as userId,dws.yunwangdateformat("order",in.sale_channel_id) as company,
         |in.sale_channel_name as companyName,in.s_state as state,
         |from_unixtime(unix_timestamp(split(in.s_create_time,' ')[0],'yyyy-MM-dd'),'yyyyMMdd') as create_time,
         |from_unixtime(unix_timestamp(split(in.s_delete_time,' ')[0],'yyyy-MM-dd'),'yyyyMMdd') as del_time,
         |from_unixtime(unix_timestamp(split(de.start_time,' ')[0],'yyyy-MM-dd'),'yyyyMMdd') as start_time,
         |from_unixtime(unix_timestamp(split(de.end_time,' ')[0],'yyyy-MM-dd'),'yyyyMMdd') as end_time,
         |from_unixtime(unix_timestamp(split(in.pay_time,' ')[0],'yyyy-MM-dd'),'yyyyMMdd') as pay_time,
         |in.discount as discount,in.beans as beans,de.materiel_code as materielCode,de.materiel_name as materielName,
         |in.pay_channel as payChannel,in.pay_price as payPrice,in.order_price as orderPrice,de.price as price,
         |in.pay_tradeno as payTradeno,in.coupons as coupons,in.bean_type as beanType,in.remark as remark,
         |de.count_date as countDate,rt,rs,row_number() over(partition by in.id,de.id,in.app_id,in.app_order_id,in.user_id
         |order by in.id,de.id,in.app_id,in.app_order_id,in.user_id) as rank from
         |(select * from (select *,row_number() over(partition by app_id,app_order_id,product_id order by app_id) as rank
         |from ods_order_detail where count_date<='${yesStr}') as tmp2
         |where tmp2.rank=1) as de inner join
         |(select id,app_id,app_order_id,user_id,user_name,sale_channel_id,sale_channel_name,s_state,s_create_time,
         |s_delete_time,order_price,discount,pay_channel,pay_time,pay_price,pay_tradeno,remark,beans,bean_type,
         |coupons,row_timestamp as rt,row_status as rs from (select *,row_number() over(partition by app_id,app_order_id order by app_id) as rank
         |from ods_order_info where count_date<='${yesStr}') as tmp1
         |where tmp1.rank=1) as in on
         |de.app_order_id=in.app_order_id) as tt where tt.rank=1
      """.stripMargin

    spark.sql(insertSql)

  }

}