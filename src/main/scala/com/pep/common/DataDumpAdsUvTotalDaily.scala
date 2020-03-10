package com.pep.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DataDumpAdsUvTotalDaily {
  //4 将Ads层UV相关数据写入PostgreSQL
  def writeAdsData2PostgreSQL(spark: SparkSession, yestStr: String): Unit = {
    val props = DbProperties.propScp
    props.setProperty("write_mode", "Append")
    spark.sql("use ads")

    val insert =
      s"""
         |insert overwrite table ads.ads_uv_total_daily partition(count_date)
         |select product_id,
         |       company,
         |       count(distinct(device_id)),
         |       sum(action_count)      as action_count,
         |       sum(session_count)     as session_count,
         |       '$yestStr' as count_date
         |from dws.dws_uv_total_$yestStr
         |group by product_id, company
      """.stripMargin
    print(insert)
    spark.sql(insert)
    val querySql_333 =
      s"""
         |select * from ads.ads_uv_total_daily where count_date='${yestStr}'
      """.stripMargin
    spark.sql(querySql_333).write.mode(props.getProperty("write_mode")).jdbc(props.getProperty("url"), "ads_uv_total_daily", props)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataDumpAdsUvTotalDaily").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    args.foreach(s => {
      action(spark, s)
    })
    spark.stop()
  }

  def action(spark: SparkSession, dateStr: String): Unit = {
    writeAdsData2PostgreSQL(spark, dateStr)
  }
}
