package com.pep.dwd

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.control.Breaks

object DwsUvTotal2DwdUserArea {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-DwsUvTotal2DwdUserArea")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val withParams = args.length > 0
    val regPattern = "^[0-9]{9}$".r
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -1)
    var yestodayStr = format.format(cal.getTime)

    loop.breakable {
      for (i <- 0 until (if (withParams) args.length else 1)) {
        if (withParams) {
          if (regPattern.findPrefixOf(args(i)) == None) loop.break()
          yestodayStr = args(i)
        }
        doAction(spark, yestodayStr)
      }
      spark.stop()
    }

  }

  def doAction(spark: SparkSession, yestodayStr: String) = {
    writeDwsUvTotal2DwdUserArea(spark,yestodayStr)
  }

  def writeDwsUvTotal2DwdUserArea(spark: SparkSession, yestodayStr: String) = {

    spark.sql("use dwd")

    val createSql =
      """
        |create table if not exists dwd.dwd_user_area(
        |product_id string,
        |company string,
        |remote_addr string,
        |country string,
        |province string,
        |city string,
        |location string,
        |active_user string,
        |count_date string
        |) STORED AS parquet
      """.stripMargin

    spark.sql(createSql)

    val etlSql =
      s"""
        |insert overwrite table dwd.dwd_user_area
        |select product_id,company,remote_addr,country,province,city,
        |location,active_user,count_date from dws.dws_uv_total where nvl(active_user,'')!=''
      """.stripMargin

    println(etlSql)

    spark.sql(etlSql)

  }

}
