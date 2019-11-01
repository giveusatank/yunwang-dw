package com.pep.ads.textbook

import java.text.SimpleDateFormat
import java.util
import java.util.{ArrayList, Calendar, Date, List}

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks.{break, breakable}

object DwsTextBookUsed2AdsTextBookAvgTime {

  /**
    * 学期教材用户统计
    * @param spark
    * @param yestStr
    */
  def DwsTextBookUsed2AdsTextBookAvgTime(spark: SparkSession, yestStr: String) = {
    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads.ads_textbook_used_avg_time_consume
        |(
        |    product_id   string,
        |    company      string,
        |    province     string,
        |    zxxkc        string,
        |    nj           string,
        |    sum_time_consume           string,
        |    start_action_count           string,
        |    avg_time_consume  string
        |)
        |    partitioned by (data_type string,count_date string)
        |    stored as textfile
      """.stripMargin
    spark.sql(createSql)
    spark.sql("msck repair table dwd.dwd_order_related_width")

    spark.sql(s"alter table ads.ads_textbook_used_avg_time_consume drop if exists partition(count_date='${yestStr}')")
    var ysList : List[String] = getCurrentSchoolYear(yestStr)

    val insertSql1 =
      s"""
         |insert into table ads.ads_textbook_used_avg_time_consume partition(data_type,count_date)
         |select t.product_id,t.company,t.province,'',t.nj,cast(sum(t.sum_time_consume) as decimal(32,0)) as sum_time_consume,cast(sum(t.start_action_count) as decimal(32,0)) as start_action_count,cast(sum(t.sum_time_consume)/sum(t.start_action_count) as decimal(32,0)) as avg_time_consume,'nj',${yestStr} from (
         |select product_id,company,user_id,sum(sum_time_consume) sum_time_consume,sum(start_action_count) as start_action_count,nj ,province
         |from dws.dws_textbook_used_total_wide where count_date>='${ysList.get(0)}' and count_date<='${ysList.get(1)}' and country='中国'
         | group by user_id,nj,province,product_id,company) t join
         |(select user_id from dwd.dwd_order_related_width where pay_time>='${ysList.get(0)}' and pay_time<='${ysList.get(1)}' group by user_id) t1
         |on t.user_id=t1.user_id group by t.nj,t.province,t.product_id,t.company
      """.stripMargin

    val insertSql2 =
      s"""
         |insert into table ads.ads_textbook_used_avg_time_consume partition(data_type,count_date)
         |select t.product_id,t.company,t.province,t.zxxkc,'',cast(sum(t.sum_time_consume) as decimal(32,0)) as sum_time_consume,cast(sum(t.start_action_count) as decimal(32,0)) as start_action_count,cast(sum(t.sum_time_consume)/sum(t.start_action_count) as decimal(32,0)) as avg_time_consume,'zxxkc',${yestStr} from (
         |select product_id,company,user_id,sum(sum_time_consume) sum_time_consume,sum(start_action_count) as start_action_count,zxxkc ,province
         |from dws.dws_textbook_used_total_wide where count_date>='${ysList.get(0)}' and count_date<='${ysList.get(1)}' and country='中国'
         | group by user_id,zxxkc,province,product_id,company) t join
         |(select user_id from dwd.dwd_order_related_width where pay_time>='${ysList.get(0)}' and pay_time<='${ysList.get(1)}' group by user_id) t1
         |on t.user_id=t1.user_id group by t.zxxkc,t.province,t.product_id,t.company
      """.stripMargin

    val insertSql3 =
      s"""
         |insert into table ads.ads_textbook_used_avg_time_consume partition(data_type,count_date)
         |select t.product_id,t.company,t.province,t.zxxkc,t.nj,cast(sum(t.sum_time_consume) as decimal(32,0)) as sum_time_consume,cast(sum(t.start_action_count) as decimal(32,0)) as start_action_count,cast(sum(t.sum_time_consume)/sum(t.start_action_count) as decimal(32,0)) as avg_time_consume,'zxxkc_nj',${yestStr} from (
         |select product_id,company,user_id,sum(sum_time_consume) sum_time_consume,sum(start_action_count) as start_action_count,zxxkc ,nj,province
         |from dws.dws_textbook_used_total_wide where count_date>='${ysList.get(0)}' and count_date<='${ysList.get(1)}' and country='中国'
         | group by user_id,zxxkc,nj,province,product_id,company) t join
         |(select user_id from dwd.dwd_order_related_width where pay_time>='${ysList.get(0)}' and pay_time<='${ysList.get(1)}' group by user_id) t1
         |on t.user_id=t1.user_id group by t.zxxkc,t.nj,t.province,t.product_id,t.company
      """.stripMargin

    spark.sql(insertSql1)
    spark.sql(insertSql2)
    spark.sql(insertSql3)

    spark.sql("msck repair table ads.ads_textbook_used_avg_time_consume")
    val selectSql =
      s"""
        |select product_id,company,province,zxxkc,nj,sum_time_consume,start_action_count,avg_time_consume,data_type,count_date
        |from ads_textbook_used_avg_time_consume where count_date='${yestStr}'
      """.stripMargin
    val readDate = spark.sql(selectSql)

    val props = new java.util.Properties()
    props.setProperty("user","pgadmin")
    props.setProperty("password","szkf2019")
    props.setProperty("url","jdbc:postgresql://172.30.0.9:5432/bi")
    var writeDF = readDate.coalesce(5)
    writeDF.write.format("jdbc").
      mode("append").
      jdbc(props.getProperty("url"),"ads_textbook_used_avg_time_consume",props)



    val createSql2 =
      """
        |create table if not exists ads_textbook_used_avg_use_day
        |(
        |    product_id   string,
        |    company      string,
        |    province     string,
        |    zxxkc        string,
        |    nj           string,
        |    day_count    string,
        |    user_count   string,
        |    avg_use_day  string
        |)
        |    partitioned by (data_type string,count_date string)
        |    stored as textfile
      """.stripMargin
    spark.sql(createSql2)
    spark.sql("msck repair table dwd.dwd_order_related_width")

    spark.sql(s"alter table ads.ads_textbook_used_avg_use_day drop if exists partition(count_date='${yestStr}')")

    //创建时间小于20191016为资源加工库元数据
    //select split(chapter_ids,'\,')[size(split(chapter_ids,'\,'))-1] as chapter_id,rid from (select explode(split(tid1_path,'\\|')) as chapter_ids,rid  from ods.ods_zyk_pep_cn_resource where tid1_path like  '%|%' limit 1);
    val insertSql11 =
    s"""
       |insert into table ads.ads_textbook_used_avg_use_day partition(data_type,count_date)
       |select tt.product_id,tt.company,tt.province,tt.zxxkc,'',cast(count(1) as decimal(32,0)) as day_count,cast(count(distinct tt.user_id) as decimal(32,0)) as user_count,cast(count(1)/count(distinct tt.user_id) as decimal(32,0)) as avg_use_day,'zxxkc',${yestStr} from (
       |select a.product_id,a.company,a.user_id,dws.getEduCode(passive_obj, 'zxxkc') as zxxkc,a.province,count_date from dws.dws_textbook_used_session a
       |join
       |(select user_id from dwd.dwd_order_related_width where pay_time>='${ysList.get(0)}' and pay_time<='${ysList.get(1)}'  group by user_id) t1 on a.user_id=t1.user_id
       | where count_date>='${ysList.get(0)}' and count_date<='${ysList.get(1)}' and country='中国'
       |group by a.product_id,a.company,a.user_id,count_date,a.province,dws.getEduCode(passive_obj, 'zxxkc')) tt group by tt.zxxkc,tt.province,tt.product_id,tt.company
      """.stripMargin

    val insertSql22 =
      s"""
         |insert into table ads.ads_textbook_used_avg_use_day partition(data_type,count_date)
         |select tt.product_id,tt.company,tt.province,'',tt.nj,cast(count(1) as decimal(32,0)) as day_count,cast(count(distinct tt.user_id) as decimal(32,0)) as user_count,cast(count(1)/count(distinct tt.user_id) as decimal(32,0)) avg_use_day,'nj',${yestStr} from (
         |select a.product_id,a.company,a.user_id,dws.getEduCode(passive_obj, 'nj') as nj,a.province,count_date from dws.dws_textbook_used_session a
         |join
         |(select user_id from dwd.dwd_order_related_width where pay_time>='${ysList.get(0)}' and pay_time<='${ysList.get(1)}'  group by user_id) t1 on a.user_id=t1.user_id
         | where count_date>='${ysList.get(0)}' and count_date<='${ysList.get(1)}' and country='中国'
         |group by a.product_id,a.company,a.user_id,count_date,a.province,dws.getEduCode(passive_obj, 'nj')) tt group by tt.nj,tt.province,tt.product_id,tt.company
      """.stripMargin

    val insertSql33 =
      s"""
         |insert into table ads.ads_textbook_used_avg_use_day partition(data_type,count_date)
         |select tt.product_id,tt.company,tt.province,tt.zxxkc,tt.nj,cast(count(1) as decimal(32,0)) as day_count,cast(count(distinct tt.user_id) as decimal(32,0)) as user_count,cast(count(1)/count(distinct tt.user_id) as decimal(32,0)) avg_use_day,'zxxkc_nj',${yestStr} from (
         |select a.product_id,a.company,a.user_id,dws.getEduCode(passive_obj, 'nj') as nj,dws.getEduCode(passive_obj, 'zxxkc') as zxxkc,a.province,count_date from dws.dws_textbook_used_session a
         |join
         |(select user_id from dwd.dwd_order_related_width where pay_time>='${ysList.get(0)}' and pay_time<='${ysList.get(1)}'  group by user_id) t1 on a.user_id=t1.user_id
         | where count_date>='${ysList.get(0)}' and count_date<='${ysList.get(1)}' and country='中国'
         |group by a.product_id,a.company,a.user_id,count_date,a.province,dws.getEduCode(passive_obj, 'nj'),dws.getEduCode(passive_obj, 'zxxkc')) tt
         |group by tt.nj,tt.zxxkc,tt.province,tt.product_id,tt.company
      """.stripMargin

    spark.sql(insertSql11)
    spark.sql(insertSql22)
    spark.sql(insertSql33)

    spark.sql("msck repair table ads.ads_textbook_used_avg_use_day")
    val selectSql11 =
      s"""
         |select product_id,company,province,zxxkc,nj,day_count,user_count,avg_use_day,data_type,count_date
         |from ads_textbook_used_avg_use_day where count_date='${yestStr}'
      """.stripMargin
    val readDate11 = spark.sql(selectSql11)

    val props11 = new java.util.Properties()
    props11.setProperty("user","pgadmin")
    props11.setProperty("password","szkf2019")
    props11.setProperty("url","jdbc:postgresql://172.30.0.9:5432/bi")
    var writeDF11 = readDate11.coalesce(5)
    writeDF11.write.format("jdbc").
      mode("append").
      jdbc(props.getProperty("url"),"ads_textbook_used_avg_use_day",props)
  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DwsTextBookUsed2AdsTextBookAvgTime").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    //获取今日、昨天的日期
    val format = new SimpleDateFormat("yyyyMMdd")
    var withoutParameter = true
    if (args.length > 0) withoutParameter = false
    breakable{
      //参数内容校验 一次性对所有参数进行校验，若有非yyyyMMdd格式的参数，均不执行
      if (!withoutParameter) {
        for (i <- 0 until (if (args.length > 0) args.length else 1)) {
          if (None == "^[0-9]{8}$".r.findPrefixOf(args(i))) {
            break()
          }
        }
      }
      for (i <- 0 until (if (args.length > 0) args.length else 1)) {
        var today = new Date()
        if (!withoutParameter) {
          //如果带参数，重置today，以参数中的变量为today执行t-1业务
          today = format.parse(args(i).toString())
        }
        val cal = Calendar.getInstance
        cal.setTime(today)
        cal.add(Calendar.DATE, -1)
        if (!withoutParameter) {
          //按参数执行，执行参数当天的
          cal.add(Calendar.DATE, 1)
        }
        val yestStr: String = format.format(cal.getTime)
        //执行业务逻辑
        action(spark, yestStr)
      }
    }
    spark.stop()
  }

  def action(spark: SparkSession, yestStr: String): Unit = {
    //1 每日增量
    DwsTextBookUsed2AdsTextBookAvgTime(spark, yestStr)

  }

  def getCurrentSchoolYear(yestStr: String) :List[String] = {
    val result = new util.ArrayList[String]
    try {
      val syStartMonth = 9
      val syEndMonth = 8
      val ysds = yestStr
      val format = new SimpleDateFormat("yyyyMMdd")
      val yesterday = format.parse(ysds)
      val startCal = Calendar.getInstance
      val endCal = Calendar.getInstance
      startCal.setTime(yesterday)
      var start :String  = null
      var end :String = null
      val month = startCal.get(Calendar.MONTH)
      if (month >= 8) {
        startCal.set(Calendar.MONTH, 8)
        startCal.set(Calendar.DAY_OF_MONTH, 1)
        start = format.format(startCal.getTime)
        end = ysds
      }
      else {
        startCal.add(Calendar.YEAR, -1)
        startCal.set(Calendar.MONTH, 8)
        startCal.set(Calendar.DAY_OF_MONTH, 1)
        start = format.format(startCal.getTime)
        end = ysds
      }
      result.add(start)
      result.add(ysds)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }
    return result
  }
}
