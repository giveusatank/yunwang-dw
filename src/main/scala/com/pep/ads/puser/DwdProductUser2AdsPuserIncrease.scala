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
         |user_count string
         |) partitioned by (count_date string) stored as parquet
      """.stripMargin

    spark.sql(createSql)

    spark.sql(s"alter table ads.ads_puser_increase drop if exists partition(count_date=${yesStr})")

    val etlSql =
      s"""
         |insert overwrite table ads_puser_increase
         |select ress.pid,ress.com,ress.country,ress.province,count(distinct(ress.user_id)),'${yesStr}' from
         |(select ress.pid,ress.com,ress.user_id,ress.country,ress.province,ress.city from
         |(select temp.user_id,temp.pid,temp.com,temp.country,temp.province,temp.city,
         |row_number() over(partition by temp.user_id,temp.pid,temp.com order by temp.cou desc) as rkk from
         |(select t1.user_id,t1.product_id as pid,t1.company as com,t2.country,t2.province,t2.city,
         |count(1) over(partition by t1.product_id,t1.company,t1.user_id,t2.country,t2.province,
         |t2.city) as cou from
         |(select * from dwd.dwd_product_user where from_unixtime(cast(row_timestamp as bigint) / 1000,'yyyyMMdd')='${yesStr}')
         |as t1 left join dwd.dwd_user_area as t2 on
         |t1.product_id=t2.product_id and t1.company=t2.company and t1.user_id=t2.active_user) as temp )
         |as ress) group by ress.pid,ress.com,ress.country,ress.province
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
        |query_type string,
        |bus_reg string,
        |new_reg string,
        |tou_reg string,
        |new_reg_ratio string,
        |tou_active_reg_ratio string,
        |week string
        |)
        |partitioned by (count_date string)
        |stored as textfile
      """.stripMargin

    spark.sql(createSql)
    val insertSql =
      s"""
         |insert into table ads.ads_puser_conversion partition(count_date)
         |select t.product_id,t.company,'全国','0',c.cu as bus_reg,t.new_reg ,(c.cu-t.new_reg) as tou_reg,round(new_reg/c.cu,2) as new_reg_ratio,round((c.cu-t.new_reg)/d.tou_cu,2) as tou_active_reg_ratio,dws.dateUtilUDF('week',unix_timestamp('$yestStr', 'yyyyMMdd')),'$yestStr' from (
         |select b.product_id,count(DISTINCT (b.user_id)) as new_reg,b.company from (-- 新用户注册数
         |select aa.product_id,aa.company,aa.active_user from dws.dws_uv_total aa join ( -- 采集注册用户中设备当日首次出现为新用户ID
         |select device_id,product_id,company from dws.dws_uv_increase where nvl(active_user,'')!='' and count_date='$yestStr' group by device_id,product_id,company -- 新用户表中的注册用户（新注册、游客注册）的设备ID
         |) bb on aa.device_id=bb.device_id and from_unixtime(cast(substring(aa.first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' and nvl(aa.active_user,'')!='' -- 筛选出当天出现的设备
         |) a join (
         |select user_id as user_id,product_id,company from dwd.dwd_product_user where -- 当天注册用户
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' group by user_id,product_id,company
         |) b on a.active_user=b.user_id and a.product_id=b.product_id  and a.company=b.company
         |group by b.product_id,b.company) t join (
         |select count(distinct(user_id)) as cu,product_id,company from dwd.dwd_product_user where
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr'  group by product_id,company
         |) c on t.product_id=c.product_id and t.company=c.company
         |join (
         |select t.product_id,t.company,count(distinct(t.device_id)) as tou_cu from (
         |select a.product_id,a.company,a.device_id,nvl(b.device_id,'0000') as nvlid from (
         |select device_id,company,product_id from dws.dws_uv_daily where count_date='$yestStr' group by device_id,company,product_id having nvl(max(active_user),'')='' --游客+今日新用户
         |) a left join (
         |select device_id,company,product_id from dws.dws_uv_increase where count_date='$yestStr' group by device_id,company,product_id having nvl(max(active_user),'')='' --今日新用户
         |) b on a.company=b.company and a.device_id=b.device_id and a.product_id=b.product_id
         |) t where t.nvlid='0000' group by t.product_id,t.company
         |) d on  t.product_id=d.product_id and t.company=d.company
      """.stripMargin

    spark.sql(insertSql)

    //插入 各省份的数据
    val insertSql1 =
      s"""
         |insert into table ads.ads_puser_conversion partition(count_date)
         |select t.product_id,t.company,t.province,'1',c.cu as bus_reg,t.new_reg ,(c.cu-t.new_reg) as tou_reg,round(new_reg/c.cu,2) as new_reg_ratio,round((c.cu-t.new_reg)/d.tou_cu,2) as tou_active_reg_ratio,dws.dateUtilUDF('week',unix_timestamp('$yestStr', 'yyyyMMdd')),'$yestStr' from (
         |select b.product_id,count(DISTINCT (b.user_id)) as new_reg,b.company,a.province from (-- 新用户注册数
         |select aa.product_id,aa.company,aa.active_user,aa.province from dws.dws_uv_total aa join ( -- 采集注册用户中设备当日首次出现为新用户ID
         |select device_id,product_id,company  from dws.dws_uv_increase where nvl(active_user,'')!='' and count_date='$yestStr' group by device_id,product_id,company -- 新用户表中的注册用户（新注册、游客注册）的设备ID
         |) bb on aa.device_id=bb.device_id and from_unixtime(cast(substring(aa.first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' and nvl(aa.active_user,'')!='' and aa.country='中国'-- 筛选出当天出现的设备
         |) a join (
         |select u1.user_id as user_id,u1.product_id,u1.company,u2.province from dwd.dwd_product_user u1 left join dwd.dwd_user_area u2 on u1.product_id=u2.product_id and u1.company=u2.company and u1.user_id=u2.active_user where -- 当天注册用户
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' group by u1.user_id,u1.product_id,u1.company,u2.province
         |) b on a.active_user=b.user_id and a.product_id=b.product_id  and a.company=b.company and a.province=b.province
         |group by b.product_id,b.company,a.province ) t join (
         |select count(user_id) as cu,product_id,company,province from (
         |select u1.user_id,u1.product_id,u1.company,u2.province from dwd.dwd_product_user u1 left join dwd.dwd_user_area u2 on u1.product_id=u2.product_id and u1.company=u2.company and u1.user_id=u2.active_user where -- 当天注册用户
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' group by u1.user_id,u1.product_id,u1.company,u2.province,u1.user_id) group by product_id,company,province
         |) c on t.product_id=c.product_id and t.company=c.company and t.province=c.province
         |join (
         |select t.product_id,t.company,t.province,count(distinct(t.device_id)) as tou_cu from (
         |select a.product_id,a.company,a.province,a.device_id,nvl(b.device_id,'0000') as nvlid from (
         |select device_id,company,product_id,province from dws.dws_uv_daily where count_date='$yestStr' group by device_id,company,product_id,province having nvl(max(active_user),'')='' --游客+今日新用户
         |) a left join (
         |select device_id,company,product_id,province from dws.dws_uv_increase where count_date='$yestStr' group by device_id,company,product_id,province having nvl(max(active_user),'')='' --今日新用户
         |) b on a.company=b.company and a.device_id=b.device_id and a.product_id=b.product_id and a.province=b.province
         |) t where t.nvlid='0000' group by t.product_id,t.company,t.province
         |) d on  t.product_id=d.product_id and t.company=d.company and t.province=d.province
         |
       """.stripMargin
    spark.sql(insertSql1)

    val insertSql2 =
      s"""
         |insert into table ads.ads_puser_conversion partition(count_date)
         |select t.product_id,'ALL','全国','0',c.cu as bus_reg,t.new_reg ,(c.cu-t.new_reg) as tou_reg,round(new_reg/c.cu,2) as new_reg_ratio,round((c.cu-t.new_reg)/d.tou_cu,2) as tou_active_reg_ratio,dws.dateUtilUDF('week',unix_timestamp('$yestStr', 'yyyyMMdd')),'$yestStr' from (
         |select b.product_id,count(DISTINCT (b.user_id)) as new_reg from (-- 新用户注册数
         |select aa.product_id,aa.active_user from dws.dws_uv_total aa join ( -- 采集注册用户中设备当日首次出现为新用户ID
         |select device_id,product_id from dws.dws_uv_increase where nvl(active_user,'')!='' and count_date='$yestStr' group by device_id,product_id-- 新用户表中的注册用户（新注册、游客注册）的设备ID
         |) bb on aa.device_id=bb.device_id and from_unixtime(cast(substring(aa.first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' and nvl(aa.active_user,'')!=''  and aa.country='中国'-- 筛选出当天出现的设备
         |) a join (
         |select user_id as user_id,product_id from dwd.dwd_product_user where -- 当天注册用户
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' group by user_id,product_id
         |) b on a.active_user=b.user_id and a.product_id=b.product_id
         |group by b.product_id) t join (
         |select count(distinct(user_id)) as cu,product_id from dwd.dwd_product_user where
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr'  group by product_id
         |) c on t.product_id=c.product_id
         |join (
         |select t.product_id,count(distinct(t.device_id)) as tou_cu from (
         |select a.product_id,a.device_id,nvl(b.device_id,'0000') as nvlid from (
         |select device_id,product_id from dws.dws_uv_daily where count_date='$yestStr' group by device_id,product_id having nvl(max(active_user),'')='' --游客+今日新用户
         |) a left join (
         |select device_id,product_id from dws.dws_uv_increase where count_date='$yestStr' group by device_id,product_id having nvl(max(active_user),'')='' --今日新用户
         |) b on  a.device_id=b.device_id and a.product_id=b.product_id
         |) t where t.nvlid='0000' group by t.product_id
         |) d on  t.product_id=d.product_id
       """.stripMargin
    spark.sql(insertSql2)

    val insertSql3 =
      s"""
         |insert into table ads.ads_puser_conversion partition(count_date)
         |select t.product_id,'ALL',t.province,'1',c.cu as bus_reg,t.new_reg ,(c.cu-t.new_reg) as tou_reg,round(new_reg/c.cu,2) as new_reg_ratio,round((c.cu-t.new_reg)/d.tou_cu,2) as tou_active_reg_ratio,dws.dateUtilUDF('week',unix_timestamp('$yestStr', 'yyyyMMdd')),'$yestStr' from (
         |select b.product_id,count(DISTINCT (b.user_id)) as new_reg,a.province from (-- 新用户注册数
         |select aa.product_id,aa.active_user,aa.province from dws.dws_uv_total aa join ( -- 采集注册用户中设备当日首次出现为新用户ID
         |select device_id,product_id  from dws.dws_uv_increase where nvl(active_user,'')!='' and count_date='$yestStr' group by device_id,product_id -- 新用户表中的注册用户（新注册、游客注册）的设备ID
         |) bb on aa.device_id=bb.device_id and from_unixtime(cast(substring(aa.first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' and nvl(aa.active_user,'')!='' and aa.country='中国'-- 筛选出当天出现的设备
         |) a join (
         |select u1.user_id as user_id,u1.product_id,u2.province from dwd.dwd_product_user u1 left join dwd.dwd_user_area u2 on u1.product_id=u2.product_id and u1.user_id=u2.active_user where -- 当天注册用户
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' group by u1.user_id,u1.product_id,u2.province
         |) b on a.active_user=b.user_id and a.product_id=b.product_id  and a.province=b.province
         |group by b.product_id,a.province ) t join (
         |select count(user_id) as cu,product_id,province from (
         |select u1.user_id,u1.product_id,u2.province from dwd.dwd_product_user u1 left join dwd.dwd_user_area u2 on u1.product_id=u2.product_id and u1.user_id=u2.active_user where -- 当天注册用户
         |from_unixtime(cast(substring(first_access_time, 1, 10) as bigint), 'yyyyMMdd')='$yestStr' group by u1.user_id,u1.product_id,u2.province,u1.user_id) group by product_id,province
         |) c on t.product_id=c.product_id and t.province=c.province
         |join (
         |select t.product_id,t.province,count(distinct(t.device_id)) as tou_cu from (
         |select a.product_id,a.province,a.device_id,nvl(b.device_id,'0000') as nvlid from (
         |select device_id,product_id,province from dws.dws_uv_daily where count_date='$yestStr' group by device_id,product_id,province having nvl(max(active_user),'')='' --游客+今日新用户
         |) a left join (
         |select device_id,product_id,province from dws.dws_uv_increase where count_date='$yestStr' group by device_id,product_id,province having nvl(max(active_user),'')='' --今日新用户
         |) b on   a.device_id=b.device_id and a.product_id=b.product_id and a.province=b.province
         |) t where t.nvlid='0000' group by t.product_id,t.province
         |) d on  t.product_id=d.product_id and t.province=d.province
       """.stripMargin
    spark.sql(insertSql3)
  }

  def writeAdsPUserIncrease2PostgreSql(spark: SparkSession, yesStr: String) = {

    val props = DbProperties.propScp
    props.setProperty("tableName1","ads_puser_increase")
    props.setProperty("tableName2","ads_puser_conversion")

    props.setProperty("write_mode","Append")

    spark.sql("use ads")

    val selectSql =
      s"""
        |select product_id,company,country,province,user_count,count_date from ads_puser_increase
        |where count_date='${yesStr}'
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
