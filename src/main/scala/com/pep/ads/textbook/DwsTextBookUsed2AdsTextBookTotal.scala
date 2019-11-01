package com.pep.ads.textbook

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

object DwsTextBookUsed2AdsTextBookTotal {

  //方法1:洗到AdsTextBookUsedTotal，按照产品、渠道对uv pv累计就是历史所有教材的uv pv（因为不同的产品渠道的uv可以累加）
  def writeDwsTextBookUsedTotal2AdsTextBookUsedTotal(spark: SparkSession, yesStr: String): Unit = {

    //使用ads数据库
    spark.sql("use ads")

    //创建ads_textbook_used_total
    //ads这张表也默认保留7天的分区数据
    val sql =
    """
      |create table if not exists ads_textbook_used_total
      |(
      |    product_id            string,
      |    company               string,
      |    sum_time_consume      bigint,
      |    avg_time_consume      bigint,
      |    simulate_time_consume bigint,
      |    start_action_count    bigint,
      |    action_count          bigint,
      |    user_count            bigint,
      |    mark_date             string
      |) partitioned by (count_date bigint)
      |    stored as textfile
    """.stripMargin
    spark.sql(sql)
    spark.sql(s"alter table ads.ads_textbook_used_total drop if exists partition(count_date=${yesStr})")
    //使用dws数据库
    spark.sql("use dws")

    //从dws_textbook_used_T通过insert到ads_textbook_used_total的今天的分区中
    val sql2 =
      s"""
         |insert into ads.ads_textbook_used_total partition (count_date)
         |select product_id,
         |       company,
         |       sum(sum_time_consume),
         |       sum(sum_time_consume) / sum(start_action_count),
         |       avg(avg_time_consume) * sum(action_count),
         |       sum(start_action_count),
         |       sum(action_count)         as pv,
         |       count(distinct (user_id)) as uv,
         |       count_date,
         |       ${yesStr}
         |from dws_textbook_used_total where count_date='${yesStr}'
         |group by count_date, product_id, company
      """.stripMargin
    spark.sql(sql2)
  }

  //方法2:当dws_textbook_used_total_wide完成后 统计每个学科的历史累计uv pv
  def writeDwsTextBookUsedTotalWide2AdsTextBookZxxkcUsedTotal(spark: SparkSession, yesStr: String) = {

    //使用ads数据库
    spark.sql("use ads")

    //创建 ads_textbook_zxxkc_used_total
    val sql =
    """
      |create table if not exists ads_textbook_zxxkc_used_total
      |(
      |    product_id            string,
      |    company               string,
      |    zxxkc                 string,
      |    sum_time_consume      bigint,
      |    avg_time_consume      bigint,
      |    simulate_time_consume bigint,
      |    start_action_count    bigint,
      |    action_count          bigint,
      |    user_count            bigint,
      |    mark_date             string
      |) partitioned by (count_date bigint)
      |    stored as textfile
    """.stripMargin
    spark.sql(sql)
    spark.sql(s"alter table ads.ads_textbook_zxxkc_used_total drop if exists partition(count_date=${yesStr})")
    //使用dws数据库
    spark.sql("use dws")

    //从dws_textbook_used_kc_T通过insert到ads_textbook_zxxkc_used_total的今天的分区中
    val sql2 =
    s"""
       |insert into ads.ads_textbook_zxxkc_used_total partition (count_date)
       |select product_id,
       |       company,
       |       zxxkc,
       |       sum(sum_time_consume),
       |       sum(sum_time_consume) / sum(start_action_count),
       |       avg(avg_time_consume) * sum(action_count),
       |       sum(start_action_count),
       |       sum(action_count)         as pv,
       |       count(distinct (user_id)) as uv,
       |       count_date,
       |       count_date
       |from dws_textbook_used_total_wide where count_date='${yesStr}'
       |group by count_date, product_id, company, zxxkc
      """.stripMargin
    spark.sql(sql2)
  }

  //方法3：当dws_textbook_used_total_wide完成后,统计各个学科下 各个年级 历史累计情况
  def writeDwsTextBookUsedTotalWide2AdsTextBookNjUsedTotal(spark: SparkSession, yesStr: String) = {

    //使用ads数据库
    spark.sql("use ads")

    //创建ads_textbook_per_used_total
    val sql =
      """
        |create table if not exists ads_textbook_nj_used_total
        |(
        |    product_id            string,
        |    company               string,
        |    zxxkc                 string,
        |    nj                    string,
        |    sum_time_consume      bigint,
        |    avg_time_consume      bigint,
        |    simulate_time_consume bigint,
        |    start_action_count    bigint,
        |    action_count          bigint,
        |    user_count            bigint,
        |    mark_date             string
        |) partitioned by (count_date bigint)
        |    stored as textfile
      """.stripMargin
    spark.sql(sql)

    //使用dws数据库
    spark.sql("use dws")
    spark.sql(s"alter table ads.ads_textbook_nj_used_total drop if exists partition(count_date=${yesStr})")
    //从dws_textbook_used_total_wide通过insert到ads_textbook_nj_used_total的今天的分区中
    val sql2 =
      s"""
         |insert into ads.ads_textbook_nj_used_total partition (count_date)
         |select product_id,
         |       company,
         |       zxxkc,
         |       nj,
         |       sum(sum_time_consume),
         |       sum(sum_time_consume) / sum(start_action_count),
         |       avg(avg_time_consume) * sum(action_count),
         |       sum(start_action_count),
         |       sum(action_count)         as pv,
         |       count(distinct (user_id)) as uv,
         |       count_date,
         |       count_date
         |from dws_textbook_used_total_wide where count_date='${yesStr}'
         |group by count_date, product_id, company, zxxkc, nj
      """.stripMargin
    spark.sql(sql2)

  }

  //方法4：当dws_textbook_used_total_wide完成后,统计各个学科下的每本教材的uv pv情况
  def writeDwsTextBookUsedTotalWide2AdsTextBookPerUsedTotal(spark: SparkSession, yesStr: String) = {

    //使用ads数据库
    spark.sql("use ads")

    //创建ads_textbook_per_used_total
    val sql =
    """
      |create table if not exists ads_textbook_per_used_total
      |(
      |    product_id            string,
      |    company               string,
      |    passive_obj           string,
      |    rkxd                  string,
      |    zxxkc                 string,
      |    nj                    string,
      |    fascicule             string,
      |    sum_time_consume      bigint,
      |    avg_time_consume      bigint,
      |    simulate_time_consume bigint,
      |    start_action_count    bigint,
      |    action_count          bigint,
      |    user_count            bigint,
      |    mark_date             string
      |) partitioned by (count_date bigint)
      |    stored as textfile
    """.stripMargin
    spark.sql(sql)

    //使用dws数据库
    spark.sql("use dws")
    spark.sql(s"alter table ads.ads_textbook_per_used_total drop if exists partition(count_date=${yesStr})")
    //从dws_textbook_used_total_wide通过insert到ads_textbook_per_used_total的今天的分区中
    val sql2 =
      s"""
         |insert into ads.ads_textbook_per_used_total partition (count_date)
         |select t.product_id,
         |       t.company,
         |       t.passive_obj,
         |       dws.getEduCode(t.passive_obj, 'rkxd'),
         |       t.zxxkc,
         |       dws.getEduCode(t.passive_obj, 'nj'),
         |       dws.getEduCode(t.passive_obj, 'fascicule'),
         |       t.sum,
         |       t.avg,
         |       t.sim,
         |       t.sac,
         |       t.pv,
         |       t.uv,
         |       t.cd,
         |       t.cd
         |from (select product_id,
         |             company,
         |             passive_obj,
         |             zxxkc,
         |             sum(sum_time_consume)                           as sum,
         |             sum(sum_time_consume) / sum(start_action_count) as avg,
         |             avg(avg_time_consume) * sum(action_count)       as sim
         |              ,
         |             sum(start_action_count)                         as sac,
         |             sum(action_count)                               as pv,
         |             count(distinct (user_id))                       as uv,
         |             count_date                                      as cd
         |      from dws_textbook_used_total_wide where count_date='${yesStr}'
         |      group by count_date, product_id, company, zxxkc, passive_obj) t
      """.stripMargin
    spark.sql(sql2)
  }

  //将教材相关的Ads层数据写到PostgreSQL中
  def writeAdsTextBookRelated2PostgreSQL(spark: SparkSession, yesStr: String) = {

    val props = new java.util.Properties()
    props.setProperty("user","pgadmin")
    props.setProperty("password","szkf2019")
    props.setProperty("url","jdbc:postgresql://172.30.0.9:5432/bi")
    props.setProperty("tableName_1","ads_textbook_nj_used_total")
    props.setProperty("tableName_2","ads_textbook_per_used_total")
    props.setProperty("tableName_3","ads_textbook_used_total")
    props.setProperty("tableName_4","ads_textbook_zxxkc_used_total")
    props.setProperty("write_mode","Append")

    //使用Ads库
    spark.sql("use ads")

    //ads_textbook_nj_used_total
    val querySql_1 =
      s"""
        |select product_id,company,zxxkc,nj,sum_time_consume,avg_time_consume,simulate_time_consume,
        |start_action_count,action_count,user_count,count_date as mark_date from ads.ads_textbook_nj_used_total
        |where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_1).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_1"),props)

    //ads_textbook_per_used_total
    val querySql_2 =
      s"""
         |select product_id,company,passive_obj,rkxd,zxxkc,nj,fascicule,sum_time_consume,
         |avg_time_consume,simulate_time_consume,start_action_count,action_count,user_count,
         |count_date as mark_date from ads.ads_textbook_per_used_total
         |where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_2).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_2"),props)

    //ads_textbook_used_total
    val querySql_3 =
      s"""
         |select product_id,company,sum_time_consume,avg_time_consume,simulate_time_consume,
         |start_action_count,action_count,user_count,count_date as mark_date
         |from ads.ads_textbook_used_total where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_3).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_3"),props)


    //ads_textbook_zxxkc_used_total
    val querySql_4 =
      s"""
         |select product_id,company,zxxkc,sum_time_consume,avg_time_consume,
         |simulate_time_consume,start_action_count,action_count,user_count,
         |count_date as mark_date from ads.ads_textbook_zxxkc_used_total
         |where count_date='${yesStr}'
      """.stripMargin

    spark.sql(querySql_4).coalesce(20).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName_4"),props)

  }

  def doAction(spark: SparkSession, yesStr: String) = {

    //方法1:洗到AdsTextBookUsedTotal，按照产品、渠道对uv pv累计就是历史所有教材的uv pv（因为不同的产品渠道的uv可以累加）
    writeDwsTextBookUsedTotal2AdsTextBookUsedTotal(spark, yesStr)

    //方法2:洗到AdsTextBookZxxkcUsedTotal，按照产品、渠道、学科对uv pv累加就是历史各个学科的uv pv（因为不同的产品渠道的uv可以累加）
    writeDwsTextBookUsedTotalWide2AdsTextBookZxxkcUsedTotal(spark, yesStr)

    //方法3：洗到AdsTextBookNjUsedTotal，按照产品、渠道、学科、年级对uv pv累计就是历史各个学科下的各个年级的uv pv（因为不同的产品渠道的uv可以累加）
    writeDwsTextBookUsedTotalWide2AdsTextBookNjUsedTotal(spark, yesStr)

    //方法4:洗到AdsTextBookPerUsedTotal，按照产品、渠道、学科、教材对uv pv累加就是历史各个学科下教材的uv pv（因为不同的产品渠道的uv可以累加）
    writeDwsTextBookUsedTotalWide2AdsTextBookPerUsedTotal(spark, yesStr)

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
