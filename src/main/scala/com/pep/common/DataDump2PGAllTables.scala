package com.pep.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DataDump2PGAllTables {
  //4 将Ads层UV相关数据写入PostgreSQL
  def writeAdsData2PostgreSQL(spark: SparkSession): Unit = {
    val props = DbProperties.propScp
    props.setProperty("write_mode", "overwrite")
    spark.sql("use ads")
    val tables = spark.sql("show tables").collect()
    tables.foreach( t => {
      val querySql_1 =
        s"""
           |select * from ${t.get(1)}
      """.stripMargin
      print(querySql_1)
      spark.sql(querySql_1).coalesce(20).write.mode(props.getProperty("write_mode")).jdbc(props.getProperty("url"), t.get(1).toString, props)
    })


  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataDump2PGAllTables").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    action(spark)
    spark.stop()
  }

  def action(spark: SparkSession): Unit = {
    writeAdsData2PostgreSQL(spark)
  }
}
