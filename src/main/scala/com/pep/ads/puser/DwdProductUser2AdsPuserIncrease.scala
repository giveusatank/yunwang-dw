package com.pep.ads.puser

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.DbProperties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.control.Breaks

object DwdProductUser2AdsPuserIncrease {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-DwdProductUser2AdsPuserIncrease")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val regPatten = "^[0-9]{8}$".r
    val flag = args.length > 0
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE,-1)
    var yesStr = format.format(cal.getTime)

    loop.breakable{
      for(i <- 0 until (if(args.length > 1) args.length else 1)){
        if(flag) {
          if(regPatten.findPrefixOf(args(i))==None) loop.break()
          yesStr = args(i)
        }
        doAction(spark,yesStr)
      }
      spark.stop()
    }
  }

  def doAction(spark:SparkSession, yesStr:String) = {
    writeDwdProductUser2AdsPUserIncrease(spark,yesStr)
    dwsUvIncrease2AdsPuserConversion(spark,yesStr)
    writeAdsPUserIncrease2PostgreSql(spark,yesStr)
  }

  def writeDwdProductUser2AdsPUserIncrease(spark: SparkSession, yesStr: String) = {

    spark.sql("use ads")

    val createSql =
      s"""
         |create table if not exists ads_puser_increase(
         |product_id string,
         |company string,
         |country string,
         |province string,
         |group_id string,
         |week string,
         |month string,
         |user_count string
         |) partitioned by (count_date string) stored as parquet
      """.stripMargin

    spark.sql(createSql)

    val etlSql =
      s"""
         |insert overwrite table ads_puser_increase partition(count_date='${yesStr}')
         |select temp2.product_id,temp2.company,temp2.country,temp2.province,grouping_id() as gid,
         |max(temp2.week) as week,max(temp2.month) as month,sum(temp2.user_count) as user_count from
         |(select ress.pid as product_id,ress.com as company,ress.country as country,ress.province as province,
         |count(distinct(ress.user_id)) as user_count,
         |dws.dateUtilUDF('week',unix_timestamp(ress.rtt, 'yyyyMMdd')) as week,
         |concat(substring(ress.rtt,1,4),"-",substring(ress.rtt,5,2)) as month from
         |(select ress.pid,ress.com,ress.rtt,ress.user_id,ress.country,ress.province,ress.city from
         |(select temp.user_id,temp.pid,temp.com,temp.rtt,temp.country,temp.province,temp.city,
         |row_number() over(partition by temp.user_id,temp.pid,temp.com order by temp.cou desc) as rkk from
         |(select t1.user_id,t1.product_id as pid,t1.company as com,from_unixtime(cast(t1.first_access_time as bigint) / 1000,'yyyyMMdd') as rtt,
         |t2.country,t2.province,t2.city,count(1) over(partition by t1.product_id,t1.company,t1.user_id,t2.country,t2.province,
         |t2.city) as cou from
         |(select * from dwd.dwd_product_user where from_unixtime(cast(first_access_time as bigint) / 1000,'yyyyMMdd')='${yesStr}')
         |as t1 left join dwd.dwd_user_area as t2 on
         |t1.product_id=t2.product_id and t1.company=t2.company and t1.user_id=t2.active_user) as temp )
         |as ress) as ress group by ress.pid,ress.com,ress.country,ress.province,ress.rtt) as temp2 group by
         |temp2.product_id,temp2.company,temp2.country,temp2.province,temp2.week,temp2.month
         |grouping sets(
         |(temp2.product_id),
         |(temp2.product_id,temp2.company),
         |(temp2.product_id,temp2.company,temp2.country,temp2.province),
         |(temp2.product_id,temp2.country,temp2.province)
         |) order by gid
      """.stripMargin

    spark.sql(etlSql)
  }

  /**
    * ads 每日uv pv统计
    *
    * @param spark
    * @param yestStr
    */
  def dwsUvIncrease2AdsPuserConversion(spark: SparkSession, yestStr: String): Unit = {
    spark.sql("use ads")
    val createSql =
      """
        |create table if not exists ads.ads_puser_conversion(
        |product_id string,
        |company string,
        |province string,
        |bus_reg string,
        |new_reg string,
        |tou_reg string,
        |new_device_cu string,
        |tou_device_cu string,
        |new_reg_ratio string,
        |tou_active_reg_ratio string,
        |gid string,
        |week string
        |)
        |partitioned by (count_date string)
        |stored as textfile
      """.stripMargin

    spark.sql(createSql)
    val insertSql =
      s"""
         |insert overwrite table ads.ads_puser_conversion partition(count_date)
         |select
         |    tt.product_id,
         |    tt.company,
         |    tt.province,
         |    sum(tt.bus_reg),
         |    sum(tt.new_reg),
         |    sum(tt.bus_reg)-sum(tt.new_reg),
         |    sum(tt.new_device_cu),
         |    sum(tt.tou_device_cu),
         |    round(sum(tt.new_reg)/sum(tt.new_device_cu),4) as new_reg_ratio,
         |    round((sum(tt.bus_reg)-sum(tt.new_reg))/sum(tt.tou_device_cu),4) as tou_active_reg_ratio,
         |    grouping_id() as gid,
         |    dws.dateUtilUDF('week',unix_timestamp('$yestStr', 'yyyyMMdd')) as week ,
         |    '$yestStr'
         |from (
         |    select
         |    t.product_id,
         |    t.company,
         |    t.province,
         |    t.cu as bus_reg,
         |    c.new_reg as new_reg,
         |    e.new_device_cu as new_device_cu,
         |    d.tou_device_cu as tou_device_cu
         |  from (
         |    select count(user_id) as cu,product_id,company,province from (
         |    select u1.user_id,u1.product_id,u1.company,u2.province from dwd.dwd_product_user u1
         |    left join dwd.dwd_user_area u2 on u1.product_id=u2.product_id and u1.company=u2.company and u1.user_id=u2.active_user where -- 当天注册用户
         |    from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr'
         |    group by u1.user_id,u1.product_id,u1.company,u2.province,u1.user_id) group by product_id,company,province
         |  ) t left join (
         |    select b.product_id,count(DISTINCT (b.user_id)) as new_reg,b.company,a.province from (-- 新用户注册数
         |    select aa.product_id,aa.company,aa.active_user,aa.province from dws.dws_uv_total aa join ( -- 采集注册用户中设备当日首次出现为新用户ID
         |      select device_id,product_id,company  from dws.dws_uv_increase where nvl(active_user,'')!='' and count_date='$yestStr' group by device_id,product_id,company -- 新用户表中的注册用户（新用户注册、老用户注册）的设备ID
         |    ) bb on aa.device_id=bb.device_id and from_unixtime(cast(substring(aa.first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' and nvl(aa.active_user,'')!='' and aa.country='中国'-- 筛选出当天出现的设备，排除老用户注册
         |    ) a left join (
         |    select u1.user_id as user_id,u1.product_id,u1.company,u2.province from dwd.dwd_product_user u1 left join dwd.dwd_user_area u2 on u1.product_id=u2.product_id and u1.company=u2.company and u1.user_id=u2.active_user where -- 当天注册用户
         |    from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' group by u1.user_id,u1.product_id,u1.company,u2.province
         |    ) b on a.active_user=b.user_id and a.product_id=b.product_id  and a.company=b.company and a.province=b.province
         |    group by b.product_id,b.company,a.province
         |  ) c on t.product_id=c.product_id and t.company=c.company and t.province=c.province
         |  left join ( -- 老游客
         |    select t.product_id,t.company,t.province,count(distinct(t.device_id)) as tou_device_cu from (
         |    select a.product_id,a.company,a.province,a.device_id,nvl(b.device_id,'0000') as nvlid from (
         |    select device_id,company,product_id,province from dws.dws_uv_daily where count_date='$yestStr' group by device_id,company,product_id,province having nvl(max(active_user),'')='' --游客+今日新用户
         |    ) a left join (
         |    select device_id,company,product_id,province from dws.dws_uv_increase where count_date='$yestStr' group by device_id,company,product_id,province having nvl(max(active_user),'')='' --今日新用户
         |    ) b on a.company=b.company and a.device_id=b.device_id and a.product_id=b.product_id and a.province=b.province
         |    ) t where t.nvlid='0000' group by t.product_id,t.company,t.province
         |  ) d on  t.product_id=d.product_id and t.company=d.company and t.province=d.province
         |  left join ( -- 今日新用户
         |    select count(distinct(device_id)) as new_device_cu,company,product_id,province from dws.dws_uv_increase where count_date='$yestStr' group by company,product_id,province  --今日新用户
         |  ) e on e.product_id=d.product_id and e.company=d.company and e.province=d.province
         |) tt
         |group by tt.product_id,tt.company,tt.province,tt.bus_reg,tt.new_reg,tt.new_device_cu,tt.tou_device_cu
         |grouping sets(
         |(tt.product_id,tt.company,tt.province),
         |(tt.product_id,tt.company),
         |(tt.product_id,tt.province),
         |(tt.product_id)
         |)
      """.stripMargin

    spark.sql(insertSql)
  }

  def writeAdsPUserIncrease2PostgreSql(spark: SparkSession, yesStr: String) = {

    val props = DbProperties.propScp
    props.setProperty("tableName1","ads_puser_increase")
    props.setProperty("tableName2","ads_puser_conversion")

    props.setProperty("write_mode","Append")

    spark.sql("use ads")

    val selectSql =
      s"""
        |select product_id,company,country,province,group_id,week,month,
        |user_count,count_date from ads_puser_increase where count_date='${yesStr}'
      """.stripMargin


    //ads_uv_conversion
    val querySql_4 =
      s"""
         |select * from ads.ads_puser_conversion where count_date='${yesStr}'
      """.stripMargin

    spark.sql(selectSql).coalesce(5).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName1"),props)

    spark.sql(querySql_4).coalesce(5).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName2"),props)
  }
}
