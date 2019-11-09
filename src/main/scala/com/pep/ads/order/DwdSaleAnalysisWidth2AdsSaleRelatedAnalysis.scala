package com.pep.ads.order

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.DbProperties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

import scala.util.control.Breaks


object DwdSaleAnalysisWidth2AdsSaleRelatedAnalysis {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-DwdSaleAnalysisWidth2AdsSaleRelatedAnalysis")
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

  def doAction(spark: SparkSession, yesStr: String) = {
      writeDwdOrderRelatedWidth2AdsSaleRelatedAnalysis(spark,yesStr)
  }

  def writeDwdOrderRelatedWidth2AdsSaleRelatedAnalysis(spark: SparkSession, yesStr: String) = {

    spark.sql("use ads")

    val createSql_1 =
      """
        |create table if not exists ads_order_user_area_total(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |sale_count string,
        |user_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_11 =
      """
        |create table if not exists ads_order_user_area_zxxkc_nj_total(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |zxxkc string,
        |nj string,
        |sale_count string,
        |user_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_2 =
      """
        |create table if not exists ads_order_user_area_ym(
        |product_id string,
        |company string,
        |year_month string,
        |country string,
        |province string,
        |sale_count string,
        |user_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_22 =
      """
        |create table if not exists ads_order_user_area_zxxkc_nj_ym(
        |product_id string,
        |company string,
        |zxxkc string,
        |nj string,
        |year_month string,
        |country string,
        |province string,
        |sale_count string,
        |user_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_3 =
      """
        |create table if not exists ads_order_user_zxxkc(
        |product_id string,
        |company string,
        |zxxkc string,
        |user_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_4 =
      """
        |create table if not exists ads_order_user_nj(
        |product_id string,
        |company string,
        |nj string,
        |user_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_5 =
      """
        |create table if not exists ads_order_user_total(
        |product_id string,
        |company string,
        |user_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_6 =
      """
        |create table if not exists ads_order_user_ym(
        |product_id string,
        |company string,
        |year_month string,
        |user_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_7 =
      """
        |create table if not exists ads_order_entity_total(
        |product_id string,
        |company string,
        |authorize_way string,
        |passive_obj string,
        |zxxkc string,
        |nj string,
        |user_count string,
        |sale_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_8 =
      """
        |create table if not exists ads_order_entity_ym(
        |product_id string,
        |company string,
        |year_month string,
        |passive_obj string,
        |zxxkc string,
        |nj string,
        |user_count string,
        |sale_count string
        |) partitioned by (count_date string) stored as textfile
      """.stripMargin

    val createSql_9 =
      """
        |create table if not exists ads_order_increase(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |user_count string,
        |sale_count string
        |) partitioned by (count_date string)  stored as parquet
      """.stripMargin

    //创建销售相关的Ads层表
    spark.sql(createSql_1)
    spark.sql(createSql_11)
    spark.sql(createSql_2)
    spark.sql(createSql_22)
    spark.sql(createSql_3)
    spark.sql(createSql_4)
    spark.sql(createSql_5)
    spark.sql(createSql_6)
    spark.sql(createSql_7)
    spark.sql(createSql_8)
    spark.sql(createSql_9)
    spark.sql("drop view if exists dwd_order_temp_width")
    //创建销售宽表视图
    val createTempView =
      s"""
        |create view dwd_order_temp_width as (
        |select tep1.app_id as product_id,tep1.sale_channel_id as company,tep1.user_id as user_id,tep1.product_id as passive_obj,
        |dws.geteducode(tep1.product_id,'zxxkc') as zxxkc,dws.geteducode(tep1.product_id,'nj') as nj,
        |tep1.quantity as quantity,tep2.country as country,tep2.province as province,tep1.pay_time as pay_time,
        |tep1.authorization_way,tep1.row_status,tep1.row_timestamp from
        |(select app_id,sale_channel_id,user_id,quantity,product_id,pay_time,authorization_way,row_status,row_timestamp
        |from dwd.dwd_order_related_width) as tep1 left join
        |(select t.active_user,t.country,t.province,t.company,t.product_id from
        |(select tmp.active_user,tmp.product_id,tmp.company,tmp.country,tmp.province,
        |row_number() over(partition by tmp.active_user,tmp.product_id,tmp.company
        |order by tmp.cou desc) as rank from (select active_user,product_id,company,
        |country,province,count(1) as cou from dwd.dwd_user_area
        |group by active_user,product_id,company,country,province) as tmp ) as t where t.rank=1 ) as tep2
        |on tep1.user_id=tep2.active_user and tep1.app_id=tep2.product_id and tep1.sale_channel_id=tep2.company)
      """.stripMargin

    spark.sql(createTempView)

    spark.sql("select * from dwd_order_temp_width").show()

    //将数据ETL到Ads层
    //ads_order_entity_total

    val etlSql_1 =
      s"""
        |insert overwrite table ads_order_entity_total partition(count_date='${yesStr}')
        |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,
        |authorization_way,passive_obj,dws.geteducode(passive_obj,'zxxkc') as zxxkc,
        |if(dws.geteducode(passive_obj,'nj')='36','26',dws.geteducode(passive_obj,'nj')) as nj,count(distinct(user_id)) as user_count,
        |sum(quantity) as sale_count from dwd_order_temp_width group by product_id,company,passive_obj,authorization_way
      """.stripMargin
    spark.sql(etlSql_1)
    val etlSql_2 =
      s"""
        |insert overwrite table ads_order_entity_ym partition(count_date='${yesStr}')
        |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,
        |substring(pay_time,1,6) as year_month,passive_obj,dws.geteducode(passive_obj,'zxxkc') as zxxkc,
        |if(dws.geteducode(passive_obj,'nj')='36','26',dws.geteducode(passive_obj,'nj')) as nj,
        |count(distinct(user_id)) as user_count,sum(quantity) as sale_count
        |from dwd_order_temp_width group by product_id,company,substring(pay_time
        |,1,6),passive_obj
      """.stripMargin
    spark.sql(etlSql_2)
    val etlSql_3 =
      s"""
         |insert overwrite table ads_order_user_zxxkc partition(count_date='${yesStr}')
         |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,
         |zxxkc,count(distinct(user_id)) from dwd_order_temp_width group by product_id,company,zxxkc
      """.stripMargin
    spark.sql(etlSql_3)
    val etlSql_4 =
      s"""
         |insert overwrite table ads_order_user_nj partition(count_date='${yesStr}')
         |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,
         |if(dws.geteducode(passive_obj,'nj')='36','26',dws.geteducode(passive_obj,'nj')) as nj,
         |count(distinct(user_id)) from dwd_order_temp_width group by product_id,company,
         |if(dws.geteducode(passive_obj,'nj')='36','26',dws.geteducode(passive_obj,'nj'))
      """.stripMargin
    spark.sql(etlSql_4)
    val etlSql_5 =
      s"""
         |insert overwrite table ads_order_user_total partition(count_date='${yesStr}')
         |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,
         |count(distinct(user_id)) from dwd_order_temp_width group by product_id,company
      """.stripMargin
    spark.sql(etlSql_5)
    val etlSql_6 =
      s"""
         |insert overwrite table ads_order_user_ym partition(count_date='${yesStr}')
         |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,
         |substring(pay_time,1,6),count(distinct(user_id))
         |from dwd_order_temp_width group by product_id,company,substring(pay_time,1,6)
      """.stripMargin
    spark.sql(etlSql_6)
    val etlSql_7 =
      s"""
         |insert overwrite table ads_order_user_area_total partition(count_date='${yesStr}')
         |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,country,
         |province,sum(quantity) as sale_count,
         |count(distinct(user_id)) as user_count from dwd_order_temp_width where nvl(country,'')!='' and nvl(province,'')!=''
         |group by product_id,company,country,province
      """.stripMargin
    spark.sql(etlSql_7)
    val etlSql_77 =
      s"""
         |insert overwrite table ads_order_user_area_zxxkc_nj_total partition(count_date='${yesStr}')
         |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,
         |country,province,zxxkc,if(dws.geteducode(passive_obj,'nj')='36','26',dws.geteducode(passive_obj,'nj')) as nj,
         |sum(quantity) as sale_count,count(distinct(user_id)) as user_count
         |from dwd_order_temp_width where nvl(country,'')!='' and nvl(province,'')!=''
         |group by product_id,company,country,province,zxxkc,if(dws.geteducode(passive_obj,'nj')='36','26',dws.geteducode(passive_obj,'nj'))
      """.stripMargin
    spark.sql(etlSql_77)
    val etlSql_8 =
      s"""
         |insert overwrite table ads_order_user_area_ym partition(count_date='${yesStr}')
         |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,
         |substring(pay_time,1,6),
         |country,province,sum(quantity) as sale_count,count(distinct(user_id)) as user_count
         |from dwd_order_temp_width where nvl(country,'')!='' and nvl(province,'')!=''
         |group by product_id,company,country,province,substring(pay_time,1,6)
      """.stripMargin
    spark.sql(etlSql_8)
    val etlSql_88 =
      s"""
         |insert overwrite table ads_order_user_area_zxxkc_nj_ym partition(count_date='${yesStr}')
         |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,
         |zxxkc,if(dws.geteducode(passive_obj,'nj')='36','26',dws.geteducode(passive_obj,'nj')) as nj,substring(pay_time,1,6),
         |country,province,sum(quantity) as sale_count,count(distinct(user_id)) as user_count
         |from dwd_order_temp_width where nvl(country,'')!='' and nvl(province,'')!=''
         |group by product_id,company,zxxkc,if(dws.geteducode(passive_obj,'nj')='36','26',dws.geteducode(passive_obj,'nj')),country,province,substring(pay_time,1,6)
      """.stripMargin
    spark.sql(etlSql_88)

    val etlSql_9 =
      s"""
        |insert overwrite table ads_order_increase
        |select product_id,if((product_id='131'or product_id='51'),'110000006',company) as company,country,
        |province,count(distinct(user_id)) as user_count,sum(quantity) as sale_count,'${yesStr}'
        |from dwd_order_temp_width where from_unixtime(cast(row_timestamp as bigint) / 1000,'yyyyMMdd')='${yesStr}'
        |group by product_id,company,country,province
      """.stripMargin
    spark.sql(etlSql_9)

    //将数仓Ads层数据写入PostgreSql
    val selectSql_1 =
      s"""
        |select product_id,company,authorize_way,passive_obj,zxxkc,nj,user_count,
        |sale_count,count_date from ads.ads_order_entity_total where count_date='${yesStr}'
      """.stripMargin
    val selectDF1: Dataset[Row] = spark.sql(selectSql_1).coalesce(2)

    val selectSql_2 =
      s"""
         |select product_id,company,year_month,passive_obj,zxxkc,nj,
         |user_count,sale_count,count_date from ads.ads_order_entity_ym where count_date='${yesStr}'
      """.stripMargin
    val selectDF2:Dataset[Row] = spark.sql(selectSql_2).coalesce(2)


    val selectSql_3 =
      s"""
         |select product_id,company,country,province,user_count,sale_count,count_date
         |from ads.ads_order_user_area_total where count_date='${yesStr}'
      """.stripMargin
    val selectDF3: Dataset[Row] = spark.sql(selectSql_3).coalesce(2)

    val selectSql_33 =
      s"""
         |select product_id,company,country,province,zxxkc,nj,user_count,sale_count,count_date
         |from ads.ads_order_user_area_zxxkc_nj_total where count_date='${yesStr}'
      """.stripMargin
    val selectDF33: Dataset[Row] = spark.sql(selectSql_33).coalesce(2)

    val selectSql_4 =
      s"""
         |select product_id,company,year_month,country,province,sale_count,user_count,count_date
         |from ads.ads_order_user_area_ym where count_date='${yesStr}'
      """.stripMargin
    val selectDF4: Dataset[Row] = spark.sql(selectSql_4).coalesce(2)

    val selectSql_44 =
      s"""
         |select product_id,company,zxxkc,nj,year_month,country,province,sale_count,user_count,count_date
         |from ads.ads_order_user_area_zxxkc_nj_ym where count_date='${yesStr}'
      """.stripMargin
    val selectDF44: Dataset[Row] = spark.sql(selectSql_44).coalesce(2)

    val selectSql_5 =
      s"""
         |select product_id,company,nj,user_count,count_date
         |from ads.ads_order_user_nj where count_date='${yesStr}'
      """.stripMargin
    val selectDF5: Dataset[Row] = spark.sql(selectSql_5).coalesce(2)

    val selectSql_6 =
      s"""
         |select product_id,company,zxxkc,user_count,count_date
         |from ads.ads_order_user_zxxkc where count_date='${yesStr}'
      """.stripMargin
    val selectDF6: Dataset[Row] = spark.sql(selectSql_6).coalesce(2)

    val selectSql_7 =
      s"""
         |select product_id,company,user_count,count_date
         |from ads.ads_order_user_total where count_date='${yesStr}'
      """.stripMargin
    val selectDF7: Dataset[Row]  = spark.sql(selectSql_7).coalesce(2)

    val selectSql_8 =
      s"""
         |select product_id,company,year_month,user_count,count_date
         |from ads.ads_order_user_ym where count_date='${yesStr}'
      """.stripMargin
    val selectDF8: Dataset[Row]  = spark.sql(selectSql_8).coalesce(2)

    val selectSql_9 =
      s"""
         |select product_id,company,country,province,user_count,sale_count,count_date
         |from ads.ads_order_increase
      """.stripMargin
    val selectDF9: Dataset[Row]  = spark.sql(selectSql_9).coalesce(2)


    /*val props = new java.util.Properties()
    props.setProperty("user","pgadmin")
    props.setProperty("password","szkf2019")
    props.setProperty("url","jdbc:postgresql://172.30.0.9:5432/bi")*/
    val props = DbProperties.propScp

    selectDF1.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_entity_total",props)

    selectDF2.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_entity_ym",props)

    selectDF3.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_user_area_total",props)

    selectDF33.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_user_area_zxxkc_nj_total",props)

    selectDF4.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_user_area_ym",props)

    selectDF5.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_user_nj",props)

    selectDF6.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_user_zxxkc",props)

    selectDF7.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_user_total",props)

    selectDF8.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_user_ym",props)

    selectDF44.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_user_area_zxxkc_nj_ym",props)

    selectDF9.write.
      mode("append").
      jdbc(props.getProperty("url"),"ads_order_increase",props)
  }

}
