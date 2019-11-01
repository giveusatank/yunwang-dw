package com.pep.ods.extract

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *   将172.30.0.9上Mysql中order_db库中的
  *
  *   p_order_detail 2018、部分2019的订单详情导入ods层ods_order_detail的count_date=20180000分区（需要过滤掉2019部分）
  *
  *   p_order_info 2018、部分2019的订单信息导入ods层ods_order_info的count_date=20180000分区 （需要过滤掉2019部分）
  *
  *   p_order_detail_2019  2019的订单详情导入ods层ods_order_detail的count_date=20190000分区
  *
  *   p_order_info_2019  2019的订单详情导入ods层ods_order_detail的count_date=20190000分区
  */
object MysqlOrderRelatedTotal2DataWarehouse {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-MysqlOrderRelatedTotal2DataWarehouse")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val props = new java.util.Properties
    val orderDetailTable2018 = "p_order_detail"
    val orderInfoTable2018 = "p_order_info"
    val orderDetailTable = "p_order_detail_2019"
    val orderInfoTable = "p_order_info_2019"

    props.setProperty("user","root")
    props.setProperty("password","rjszgs2019")
    props.setProperty("url","jdbc:mysql://172.30.0.9:3306/order_db")

    val OrderDetail2019predicates = Array("substring(start_time,1,7)<='2019-01'",
    "substring(start_time,1,7)>='2019-02' and substring(start_time,1,7)<='2019-08'",
    "substring(start_time,1,7)>='2019-09'")


    val OrderDetail2018predicates = Array("substring(start_time,1,7)<='2017-09'",
      "substring(start_time,1,7)>='2017-10' and substring(start_time,1,7)<='2018-08'",
      "substring(start_time,1,7)='2018-09'","substring(start_time,1,7)>='2018-10'")


    val orderInfo2019predicates = Array("substring(pay_time,1,7)<='2019-02'",
      "substring(pay_time,1,7)>='2019-03' and substring(pay_time,1,7)<='2019-07'",
      "substring(pay_time,1,7)>='2019-08'")


    val orderInfo2018predicates = Array("substring(pay_time,1,7)<='2017-10'",
      "substring(pay_time,1,7)>='2017-11' and substring(pay_time,1,7)<='2018-08'",
      "substring(pay_time,1,7)>='2018-08'")

    val etlOrderDetail2018Sql =
      """
        |insert into table ods.ods_order_detail partition(count_date='20180000')
        |select id,app_id,app_order_id,product_id,product_name,price,quantity,type,
        |code,start_time,end_time,beans,materiel_code,materiel_name from order_detail_2018_tmp
      """.stripMargin

    val etlOrderDetail2019Sql =
      """
        |insert into table ods.ods_order_detail partition(count_date='20190000')
        |select id,app_id,app_order_id,product_id,product_name,price,quantity,type,
        |code,start_time,end_time,beans,materiel_code,materiel_name from order_detail_2019_tmp
      """.stripMargin

    val etlOrderInfo2018Sql =
      """
        |insert into table ods.ods_order_info partition(count_date='20180000')
        |select id,app_id,app_order_id,user_id,user_name,sale_channel_id,sale_channel_name,
        |s_state,s_create_time,s_delete_time,order_price,discount,pay_channel,pay_time,pay_price,
        |pay_tradeno,remark,beans,NULL,NULL from order_info_2018_tmp
      """.stripMargin

    val etlOrderInfo2019Sql =
      """
        |insert into table ods.ods_order_info partition(count_date='20190000')
        |select id,app_id,app_order_id,user_id,user_name,sale_channel_id,sale_channel_name,
        |s_state,s_create_time,s_delete_time,order_price,discount,pay_channel,pay_time,pay_price,
        |pay_tradeno,remark,beans,bean_type,coupons from order_info_2019_tmp
      """.stripMargin

    val orderDetail2019Df: DataFrame = spark.read.format("jdbc").
      jdbc(props.getProperty("url"),orderDetailTable,OrderDetail2019predicates,props)

    val orderDetail2018Df: DataFrame = spark.read.format("jdbc").
      jdbc(props.getProperty("url"),orderDetailTable2018,OrderDetail2018predicates,props)

    val orderInfo2019Df: DataFrame = spark.read.format("jdbc").
      jdbc(props.getProperty("url"),orderInfoTable,orderInfo2019predicates,props)

    val orderInfo2018Df: DataFrame = spark.read.format("jdbc").
      jdbc(props.getProperty("url"),orderInfoTable2018,orderInfo2018predicates,props)

    orderDetail2018Df.createOrReplaceTempView("order_detail_2018_tmp")
    orderDetail2019Df.createOrReplaceTempView("order_detail_2019_tmp")
    orderInfo2018Df.createOrReplaceTempView("order_info_2018_tmp")
    orderInfo2019Df.createOrReplaceTempView("order_info_2019_tmp")

    spark.sql(etlOrderDetail2018Sql)
    spark.sql(etlOrderDetail2019Sql)
    spark.sql(etlOrderInfo2018Sql)
    spark.sql(etlOrderInfo2019Sql)

    spark.stop()
  }
}
