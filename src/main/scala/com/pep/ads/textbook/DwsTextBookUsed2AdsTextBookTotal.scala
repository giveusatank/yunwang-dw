package com.pep.ads.textbook

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.{Constants, DbProperties}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

object DwsTextBookUsed2AdsTextBookTotal {

  def writeDwsTextBookUsedTotal2AdsTextBookUsedTotal(spark: SparkSession, yesStr: String): Unit = {
    spark.sql("use ads")
    val sql_c1 =
    """
      |create table if not exists ads.ads_textbook_used_total_cube
      |(
      |    product_id            string,
      |    company               string,
      |    province              string,
      |    passive_obj           string,
      |    rkxd                  string,
      |    zxxkc                 string,
      |    nj                    string,
      |    sum_time_consume      string,
      |    avg_time_consume      string,
      |    start_action_count    string,
      |    action_count          string,
      |    user_count            string,
      |    gid                   string,
      |    count_date            string
      |)
      |stored as textfile
    """.stripMargin

    spark.sql(sql_c1)
    spark.sql("use dws")

    val sql_i1 =
      s"""
         |insert overwrite table ads.ads_textbook_used_total_cube
         |select product_id,
         |company,
         |province,
         |passive_obj,
         |rkxd,zxxkc,nj,
         |sum(sum_time_consume)                           as sum_time_consume,
         |round(sum(sum_time_consume) / sum(start_action_count),0) as avg_time_consume,
         |sum(start_action_count)                         as start_action_count,
         |sum(action_count)                               as action_count,
         |count(distinct (user_id))                       as user_count,
         |grouping_id()                                   as gid,
         |'$yesStr'                                      as count_date
         |from dws.dws_textbook_used_total_wide where country='中国'
         |group by product_id,company,province,passive_obj,rkxd,zxxkc,nj
         |grouping sets(
         |(product_id,company,province,rkxd),
         |(product_id,company,province,zxxkc),
         |(product_id,company,province,nj),
         |(product_id,company,rkxd),
         |(product_id,company,zxxkc),
         |(product_id,company,nj),
         |(product_id,province,rkxd),
         |(product_id,province,zxxkc),
         |(product_id,province,nj),
         |(product_id,rkxd),
         |(product_id,zxxkc,nj),
         |(product_id,passive_obj,zxxkc),
         |(product_id,zxxkc),
         |(product_id,nj))
      """.stripMargin
    spark.sql(sql_i1)

    spark.sql("use ads")
    val sql_c2 =
      """
        |create table if not exists ads.ads_textbook_user_area
        |(
        |    product_id            string,
        |    company               string,
        |    province              string,
        |    user_count            string,
        |    sum_time_consume      string,
        |    start_action_count    string,
        |    action_count          string,
        |    gid                   string,
        |    count_date            string
        |)
        |stored as textfile
      """.stripMargin

    spark.sql(sql_c2)
    spark.sql("use dws")

    val sql_i2 =
      s"""
         |insert overwrite table ads.ads_textbook_user_area
         |select product_id,company,province,
         |count(distinct(user_id)) as user_count,
         |sum(sum_time_consume)as sum_time_consume,
         |sum(start_action_count) as start_action_count,
         |sum(action_count) as action_count,
         |grouping_id()                           as gid,
         |'$yesStr'
         |from dws.dws_textbook_used_total where country='中国'
         |group by product_id,company,province
         |grouping sets(
         |(product_id,company,province),
         |(product_id,company),
         |(product_id,province),
         |(product_id)
         |)
      """.stripMargin
    spark.sql(sql_i2)


    spark.sql("use ads")
    val sql_c3 =
      """
        |create table if not exists ads.ads_textbook_used_daily
        |(
        |    product_id            string,
        |    company               string,
        |    sum_time_consume      string,
        |    avg_time_consume      string,
        |    start_action_count    string,
        |    action_count          string,
        |    user_count            string
        |)
        |    partitioned by (count_date string)
        |stored as textfile
      """.stripMargin

    spark.sql(sql_c1)
    spark.sql("use dws")
    val sql_i3 =
      s"""
         |insert overwrite table ads.ads_textbook_used_daily partition(count_date)
         |select product_id,company,
         |sum(sum_time_consume)                           as sum_time_consume,
         |round(sum(sum_time_consume) / sum(start_action_count),0) as avg_time_consume,
         |sum(start_action_count)                         as start_action_count,
         |sum(action_count)                               as action_count,
         |sum(user_count)                     as user_count,
         |'$yesStr'
         |from dws.dws_textbook_used_daily where count_date='$yesStr'
         |group by product_id,company
      """.stripMargin
    spark.sql(sql_i3)

  }


  //将教材相关的Ads层数据写到PostgreSQL中
  def writeAdsTextBookRelated2PostgreSQL(spark: SparkSession, yesStr: String) = {

    val props = DbProperties.propScp
    props.setProperty("tableName_1","ads_textbook_used_total_cube")
    props.setProperty("tableName_2","ads_textbook_user_area")
    props.setProperty("tableName_3","ads_textbook_used_daily")
    props.setProperty("write_mode","Overwrite")

    //使用Ads库
    spark.sql("use ads")

    //ads_textbook_nj_used_total
    val querySql_1 =
      s"""
        |select * from ads.ads_textbook_used_total_cube
        |where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_1).coalesce(5).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_1"),props)

    //ads_textbook_per_used_total
    val querySql_2 =
      s"""
         |select * from ads.ads_textbook_user_area
         |where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_2).coalesce(5).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_2"),props)

    val querySql_3 =
      s"""
         |select * from ads.ads_textbook_used_daily
         |where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_3).coalesce(5).write.mode("Append").
      jdbc(props.getProperty("url"),props.getProperty("tableName_3"),props)



  }

  def doAction(spark: SparkSession, yesStr: String) = {

    //方法1:洗到AdsTextBookUsedTotal，按照产品、渠道对uv pv累计就是历史所有教材的uv pv（因为不同的产品渠道的uv可以累加）
    writeDwsTextBookUsedTotal2AdsTextBookUsedTotal(spark, yesStr)

    //将教材相关的Ads层数据写到PostgreSQL中
    writeAdsTextBookRelated2PostgreSQL(spark,yesStr)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JOB-DwsTextBookTotal2AdsTextBookTotal").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    //获取昨日日期
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance();
    cal.setTime(new Date())
    cal.add(Calendar.DATE,-1)
    var yesStr = format.format(cal.getTime)

    val break = new Breaks

    var withParams = true
    if(args.length == 0) withParams = false

    break.breakable{
      for(i <- 0 until (if(args.length>0) args.length else 1)){
        if(withParams) {
          val reg = "^[0-9]{8}$".r
          if(None == reg.findPrefixOf(args(i))) break.break()
          yesStr = args(i)
        }
        doAction(spark,yesStr)
      }
    }
    spark.stop()
  }
}
