package com.pep.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DataDump2PG {
  //4 将Ads层UV相关数据写入PostgreSQL
  def writeAdsData2PostgreSQL(spark: SparkSession, tableName: String, yesStr: String): Unit = {
    val props = DbProperties.propScp
    props.setProperty("write_mode", "Append")
    spark.sql("use ads")
    val querySql_1 =
      s"""
         |select * from ${tableName} where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_1).coalesce(5).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), tableName, props)

  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataDump2PG").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    action(spark, args(0), args(1))
    spark.stop()
  }

  def action(spark: SparkSession, tableName: String, yestStr: String): Unit = {
    writeAdsData2PostgreSQL(spark,tableName,yestStr)
  }
}
