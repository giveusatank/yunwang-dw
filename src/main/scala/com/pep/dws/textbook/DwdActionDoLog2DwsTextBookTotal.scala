package com.pep.dws.textbook

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks

object DwdActionDoLog2DwsTextBookTotal {

  //方法1：将ActionDoLog数据洗到dws_textbook_used_session
  def writeActionDoLog2DwsTextBookUsedSession(spark: SparkSession, yesStr: String) = {

    //使用dws数据库
    spark.sql("use dws")

    //创建dws_textbook_used_session表
    //这张表是会话级别的 用户在一次会话中的对教材的使用情况
    val _sql1 =
    """
      |create table if not exists dws_textbook_used_session
      |(
      |    product_id         string,
      |    company            string,
      |    country            string,
      |    province           string,
      |    city               string,
      |    location           string,
      |    user_id            string,
      |    device_id          string,
      |    group_id           string,
      |    passive_obj        string,
      |    sum_time_consume   bigint,
      |    avg_time_consume   bigint,
      |    start_action       string,
      |    start_action_count bigint,
      |    action_count       bigint,
      |    first_access_time  string,
      |    last_access_time   string
      |) partitioned by (count_date string)
      |    stored as parquet
    """.stripMargin
    spark.sql(_sql1)
    //将action_do_log中的数据导入dws_textbook_used_session中
    //if(locate(',',passive_obj) > 0 ,split(passive_obj,',')[1],passive_obj)
    //dws.yunwangdateformat('tbid','tape4b_002003') : 将老版本的教材Id转换为新版本的教材Id
    val sql =
    s"""
       |insert overwrite table dws.dws_textbook_used_session partition (count_date)
       |select product_id,
       |       dws.yunwangDateFormat('company',company),
       |       split(max(concat(start_time, '-', country)), '-')[1] as country,
       |       split(max(concat(start_time, '-', province)), '-')[1] as province ,
       |       split(max(concat(start_time, '-', city)), '-')[1] as city,
       |       '' as location,
       |       max(active_user),
       |       device_id,
       |       group_id,
       |       dws.yunwangdateformat('tbid', if(locate('[Id:{',passive_obj)>0,substr(passive_obj, 6,locate('}',passive_obj)-6),if(locate(',', passive_obj) > 0, split(passive_obj, ',')[1], if(instr(passive_obj,'{')=1,substring(passive_obj,2,13),passive_obj))) )                    as passive_obj,
       |       ods.TimeConsume(str_to_map(concat_ws(",", collect_set(concat_ws(':', cast(start_time as string), cast(action_title as string))))), "dd100001,jx200001,jx200175", "dd100002,jx200184,jx200016",
       |                       0)                                                                                                                                                              as sum_time_consume,
       |       ods.TimeConsume(str_to_map(concat_ws(",", collect_set(concat_ws(':', cast(start_time as string), cast(action_title as string))))), "dd100001,jx200001,jx200175", "dd100002,jx200184,jx200016",
       |                       1)                                                                                                                                                              as avg_time_consume,
       |       if(action_title in ('dd100001', 'jx200001','jx200175'), action_title, case when action_title = 'dd100002' then 'dd100001' when action_title = 'jx200184' then 'jx200001' when action_title = 'jx200016' then 'jx200175' end)              as start_action,
       |       sum(if(action_title in ('dd100001', 'jx200001','jx200175'), 1, 0))                                                                                                                         as start_action_count,
       |       count(1)                                                                                                                                                                        as action_count,
       |       min(start_time) as first_access_time,
       |       max(start_time) as last_access_time,
       |       put_date
       |from dwd.action_do_log
       |where put_date = '${yesStr}'
       |  and action_title in ('dd100001', 'dd100002', 'jx200001', 'jx200184','jx200175','jx200016')
       |  and group_id != ''
       |  and country='中国'
       |group by product_id, dws.yunwangDateFormat('company',company),country,province,city, device_id, group_id,
       |         dws.yunwangdateformat('tbid', if(locate('[Id:{',passive_obj)>0,substr(passive_obj, 6,locate('}',passive_obj)-6),if(locate(',', passive_obj) > 0, split(passive_obj, ',')[1], if(instr(passive_obj,'{')=1,substring(passive_obj,2,13),passive_obj))) )  ,
       |         if(action_title in ('dd100001', 'jx200001','jx200175'), action_title, case when action_title = 'dd100002' then 'dd100001' when action_title = 'jx200184' then 'jx200001'  when action_title = 'jx200016' then 'jx200175' end),
       |         put_date
     """.stripMargin

    spark.sql(sql)
  }

  //方法2：将dws_textbook_used_session数据洗到dws_textbook_used_daily（按天分区每天进行一次，统计的是每天教材的pv,uv,累计阅读时长等）
  def writeDwsTextBookUsed2DwsTextBookUsedDaily(spark: SparkSession, yesStr: String): Unit = {

    //使用dws数据库
    spark.sql("use dws")

    //创建dws_textbook_used_daily表
    val sql =
      """
        |create table if not exists dws_textbook_used_daily
        |(
        |    product_id         string,
        |    company            string,
        |    country            string,
        |    province           string,
        |    city               string,
        |    location           string,
        |    passive_obj        string,
        |    zxxkc              string,
        |    nj                 string,
        |    fascicule          string,
        |    rkxd               string,
        |    year               string,
        |    publisher          string,
        |    sum_time_consume   bigint,
        |    avg_time_consume   bigint,
        |    start_action       string,
        |    start_action_count bigint,
        |    action_count       bigint,
        |    user_count         bigint,
        |    first_access_time string,
        |    last_access_time string
        |) partitioned by (count_date string) stored as parquet
      """.stripMargin
    spark.sql(sql)
    //将dws_textbook_used_session表中的数据清洗到dws_textbook_used_daily表
    val sql1 =
      s"""
         |insert overwrite table dws_textbook_used_daily partition (count_date)
         |select product_id,
         |       company,
         |       split(max(concat(last_access_time, '-', country)), '-')[1] as country,
         |       split(max(concat(last_access_time, '-', province)), '-')[1] as province ,
         |       split(max(concat(last_access_time, '-', city)), '-')[1] as city,
         |       '',
         |       passive_obj,
         |       dws.getEduCode(passive_obj, 'zxxkc'),
         |       dws.getEduCode(passive_obj, 'nj'),
         |       dws.getEduCode(passive_obj, 'fascicule'),
         |       dws.getEduCode(passive_obj, 'rkxd'),
         |       dws.getEduCode(passive_obj, 'year'),
         |       dws.getEduCode(passive_obj, 'publisher'),
         |       sum(sum_time_consume),
         |       sum(sum_time_consume) / sum(start_action_count),
         |       start_action,
         |       sum(start_action_count),
         |       sum(action_count)         as pv,
         |       count(distinct (user_id)) as uv,
         |       min(first_access_time) as first_access_time,
         |       max(last_access_time) as last_access_time,
         |       count_date
         |from dws_textbook_used_session
         |where count_date = '${yesStr}' and nvl(user_id,'')!=''
         |group by count_date, start_action, product_id, company, country, province, city, passive_obj
      """.stripMargin

    spark.sql(sql1)
  }

  //方法3：将DwsTextBookUsedSession洗到DwsTextBookUsedTotal中（用户对于教材的历史使用情况）
  def writeDwsTextBookUsedSession2DwsTextBookUsedTotal(spark: SparkSession, yesStr: String,_7DaysBefore:String,theDayBeforeYesterday:String): Unit = {

    //使用dws数据库
    spark.sql("use dws")

    //创建dws_textbook_used_total
    val sql =
      s"""
         |create table if not exists dws_textbook_used_total
         |(
         |    product_id         string,
         |    company            string,
         |    country            string,
         |    province           string,
         |    city               string,
         |    location           string,
         |    user_id            string,
         |    passive_obj        string,
         |    zxxkc              string,
         |    nj                 string,
         |    fascicule          string,
         |    rkxd               string,
         |    year               string,
         |    publisher          string,
         |    sum_time_consume   bigint,
         |    avg_time_consume   bigint,
         |    start_action       string,
         |    start_action_count bigint,
         |    action_count       bigint,
         |    first_access_time  string,
         |    last_access_time   string
         |) partitioned by (count_date string) stored as parquet
      """.stripMargin
    spark.sql(sql)

    val sql1 =
      s"""
         |insert overwrite table dws_textbook_used_total
         |select product_id,
         |       company,
         |       country,
         |       province,
         |       city,
         |       '',
         |       user_id,
         |       passive_obj,
         |       dws.getEduCode(passive_obj, 'zxxkc'),
         |       dws.getEduCode(passive_obj, 'nj'),
         |       dws.getEduCode(passive_obj, 'fascicule'),
         |       dws.getEduCode(passive_obj, 'rkxd'),
         |       dws.getEduCode(passive_obj, 'year'),
         |       dws.getEduCode(passive_obj, 'publisher'),
         |       sum(sum_time_consume),
         |       sum(sum_time_consume) / sum(start_action_count),
         |       start_action,
         |       sum(start_action_count),
         |       sum(action_count) as action_count,
         |       min(first_access_time) as first_access_time,
         |       max(last_access_time) as last_access_time,
         |       '${yesStr}'
         |from (
         |select
         |    product_id        ,
         |    company           ,
         |    country           ,
         |    province          ,
         |    city              ,
         |    ''       as location   ,
         |    user_id           ,
         |    passive_obj       ,
         |    dws.getEduCode(passive_obj, 'zxxkc') as zxxkc,
         |    dws.getEduCode(passive_obj, 'nj') as nj,
         |    dws.getEduCode(passive_obj, 'fascicule') as fascicule,
         |    dws.getEduCode(passive_obj, 'rkxd') as rkxd,
         |    dws.getEduCode(passive_obj, 'year') as year,
         |    dws.getEduCode(passive_obj, 'publisher') as publisher,
         |    sum(sum_time_consume) as sum_time_consume,
         |    sum(sum_time_consume) / sum(start_action_count),
         |    start_action,
         |    sum(start_action_count) as start_action_count,
         |    sum(action_count) as action_count,
         |    min(first_access_time) as first_access_time,
         |    max(last_access_time) as last_access_time,
         |    '${yesStr}' as count_date
         |    from dws_textbook_used_session
         |    where count_date = '${yesStr}' and nvl(user_id,'')!=''
         |    group by product_id, company, country, province, city,location,user_id, passive_obj, start_action
         | UNION ALL
         | select * from dws_textbook_used_total where count_date='${theDayBeforeYesterday}')
         |group by start_action, count_date, product_id, company, country, province, city, location, passive_obj, user_id
         """.stripMargin
    spark.sql(sql1)
  /*
    val sql55 =
      s"""
         |drop table if exists dws_textbook_used_${yesStr}
      """.stripMargin
    spark.sql(sql55)

    //将dws_textbook_used_total改名为dws_textbook_used_T-1
    val sql2 =
      s"""
        |alter table dws_textbook_used_total rename to dws_textbook_used_${yesStr}
      """.stripMargin
    spark.sql(sql2)

    //再次创建dws_textbook_used_total
    val sql3 =
      s"""
         |create table if not exists dws_textbook_used_total
         |(
         |    product_id         string,
         |    company            string,
         |    country            string,
         |    province           string,
         |    city               string,
         |    location           string,
         |    user_id            string,
         |    passive_obj        string,
         |    sum_time_consume   bigint,
         |    avg_time_consume   bigint,
         |    start_action       string,
         |    start_action_count bigint,
         |    action_count       bigint,
         |    count_date         bigint
         |)stored as parquet
      """.stripMargin
    spark.sql(sql3)

    val sql4 =
    s"""
       |insert into dws_textbook_used_total
       |select product_id,
       |       company,
       |       country,
       |       province,
       |       city,
       |       location,
       |       user_id,
       |       passive_obj,
       |       sum(sum_time_consume),
       |       sum(sum_time_consume) / sum(start_action_count),
       |       start_action,
       |       sum(start_action_count),
       |       sum(action_count) as pv,
       |       '${yesStr}'
       |from dws_textbook_used_${yesStr}
       |group by start_action, product_id, company, country, province,
       |         city, location, passive_obj, user_id
      """.stripMargin
    spark.sql(sql4)

    //默认规则就是保留7天
    val sql5 =
    s"""
       |drop table if exists dws_textbook_used_${_7DaysBefore}
      """.stripMargin
    spark.sql(sql5)*/
  }

  //新增用户
  def writeDwsTextBookUsed2DwsTextBookUsedIncrease(spark: SparkSession, yesStr: String): Unit = {

    //使用dws数据库
    spark.sql("use dws")

    //创建dws_textbook_used_daily表
    val sql =
      """
        |create table if not exists dws_textbook_used_increase
        |(
        |    product_id         string,
        |    company            string,
        |    country            string,
        |    province           string,
        |    city               string,
        |    location           string,
        |    user_id            string,
        |    passive_obj        string,
        |    zxxkc              string,
        |    nj                 string,
        |    fascicule          string,
        |    rkxd               string,
        |    year               string,
        |    publisher          string,
        |    sum_time_consume   bigint,
        |    avg_time_consume   bigint,
        |    start_action       string,
        |    start_action_count bigint,
        |    action_count       bigint,
        |    first_access_time  string,
        |    last_access_time   string
        |) partitioned by (count_date string) stored as parquet
      """.stripMargin
    spark.sql(sql)
    //将dws_textbook_used_session表中的数据清洗到dws_textbook_used_daily表
    val sql1 =
      s"""
         |insert overwrite table dws_textbook_used_increase partition (count_date)
         |select * from dws_textbook_used_total
         |where count_date = '${yesStr}' and from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd') = '${yesStr}'

      """.stripMargin

    spark.sql(sql1)
  }

  //教材下载
  def writeDwsTextBookUsed2DwsTextBookDownloadDaily(spark: SparkSession, yesStr: String): Unit = {

    //使用dws数据库
    spark.sql("use dws")

    //创建dws_textbook_used_daily表
    val sql =
      """
        |create table if not exists dws_textbook_download_daily
        |(
        |    product_id         string,
        |    company            string,
        |    country            string,
        |    province           string,
        |    city               string,
        |    location           string,
        |    passive_obj        string,
        |    zxxkc              string,
        |    nj                 string,
        |    fascicule          string,
        |    rkxd               string,
        |    year               string,
        |    publisher          string,
        |    download_count   bigint,
        |    user_count   bigint
        |) partitioned by (count_date string) stored as parquet
      """.stripMargin
    spark.sql(sql)
    //将dws_textbook_used_session表中的数据清洗到dws_textbook_used_daily表
    val sql1 =
      s"""
         |insert overwrite table dws.dws_textbook_download_daily partition(count_date)
         |select
         |product_id,
         |company,
         |country,
         |province,
         |city,
         |'',
         |passive_obj,
         |dws.getEduCode(passive_obj, 'zxxkc'),
         |dws.getEduCode(passive_obj, 'nj'),
         |dws.getEduCode(passive_obj, 'fascicule'),
         |dws.getEduCode(passive_obj, 'rkxd'),
         |dws.getEduCode(passive_obj, 'year'),
         |dws.getEduCode(passive_obj, 'publisher'),
         |count(action_title),
         |count(distinct(active_user)),
         |'$yesStr'
         |from dwd.action_do_log where put_date ='$yesStr' and action_title in('jx200218','dd100009')
         |group by product_id,company,country,province,city,passive_obj

      """.stripMargin

    spark.sql(sql1)
  }

  def doAction(spark: SparkSession, yesStr: String, todayStr: String, _7DaysBefore: String,theDayBeforeYesterday:String) = {
    println(yesStr)
    //方法1：将ActionDoLog数据洗到dws_textbook_used_session（会话粒度）
    writeActionDoLog2DwsTextBookUsedSession(spark, yesStr)

    //方法2：将dws_textbook_used数据洗到dws_textbook_used_daily（按天分区每天进行一次，统计的是每天教材的pv,uv等）
    writeDwsTextBookUsed2DwsTextBookUsedDaily(spark, yesStr)

    //方法3：将DwsTextBookUsedSession洗到DwsTextBookUsedTotal中（用户对于教材的历史使用情况）
    writeDwsTextBookUsedSession2DwsTextBookUsedTotal(spark, yesStr,_7DaysBefore,theDayBeforeYesterday)

    //新增用户设置
    writeDwsTextBookUsed2DwsTextBookUsedIncrease(spark, yesStr)

    //教材下载
    writeDwsTextBookUsed2DwsTextBookDownloadDaily(spark, yesStr)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("JOB-OdsActionDoLog2DwsTextBook").set("spark.sql.shuffle.partitions", Constants.dws_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val break = new Breaks
    //标记为判断是否带参
    var withParams = true
    if(args.length == 0) withParams = false
    val format = new SimpleDateFormat("yyyyMMdd")
    break.breakable {
      //如果传参了就是循环参数个遍数
      for(i <- 0 until (if (args.length>0) args.length else 1)){
        //判断传入的参数是否合法，如果不是8位数字就跳出循环
        if(withParams){
          val reg = "^[0-9]{8}$".r
          if(None == reg.findPrefixOf(args(i))) break.break()
        }
        var todayStr = format.format(new Date())
        if(withParams) todayStr = format.format(format.parse(args(i)))
        val cal = Calendar.getInstance
        cal.setTime(format.parse(todayStr))
        cal.add(Calendar.DATE, -1)
        if (withParams) {
          //按参数执行，执行参数当天的
          cal.add(Calendar.DATE, 1)
        }
        val yesStr: String = format.format(cal.getTime)
        cal.add(Calendar.DATE, -1)
        val theDayBeforeYesterday: String = format.format(cal.getTime)
        cal.add(Calendar.DATE, -5)
        val _7DaysBefore: String = format.format(cal.getTime)
        doAction(spark,yesStr,todayStr,_7DaysBefore,theDayBeforeYesterday)
      }
    }
    spark.stop()
  }
}
