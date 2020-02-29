package com.pep.ads.resource

import com.pep.common.Constants
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf

import scala.util.control.Breaks

/**
  * 一期无用任务
  */
object DwdResourceDetailWidth2AdsResourceSummary {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RUN-DwdResourceDetailWidth2AdsResourceSummary").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val patternReg = "^[0-9]{8}$".r
    //传入一个参数 且能够通过正则匹配上则为true 否则为false
    val checkFlag = if(args.length==1&&patternReg.findPrefixOf(args(0))!=None) true else false
    loop.breakable{
      if(!checkFlag) loop.break();
      //doAction(spark,args(0))
    }
  }
  def doAction(spark: SparkSession, todayStr: String) = {
    writeDwdResourceDetailWidth2AdsResourceSummary(spark, todayStr)
  }
  def writeDwdResourceDetailWidth2AdsResourceSummary(spark: SparkSession, todayStr: String) = {
    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_resource_summary(
        |zxxkc string,
        |nj string,
        |textbook_name string,
        |resource_count bigint,
        |resource_size bigint,
        |mark_date string
        |) partitioned by (count_date string)
        |stored as textfile
      """.stripMargin
    spark.sql(createSql)
    spark.sql(s"alter table ads.ads_resource_summary drop if exists partition(count_date=${todayStr})")
    val insertSql =
      s"""
         |insert into ads_resource_summary partition(count_date)
         |select zxxkc,nj,text_name,count(1),sum(file_size),${todayStr},${todayStr}
         |from dwd.dwd_resource_detail_width
         |where not(nvl(zxxkc,'')=='' or nvl(nj,'')=='' or nvl(text_name,'')=='')
         |group by zxxkc,nj,text_name
      """.stripMargin
    spark.sql(insertSql)
  }
}
