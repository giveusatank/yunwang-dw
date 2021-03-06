package com.pep.ads.puser

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.{Constants, DbProperties}
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.control.Breaks

/**
  * ads_puser_total：历史累计注册用户的uv（包含有地区和无地区的uv）
  */
object DwdProductUser2AdsPuserTotal {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-DwdProductUser2AdsPuserTotal")
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")//禁止广播
    conf.set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
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
    writeDwdProductUser2AdsPUserTotal(spark,yesStr)
    writeAdsPUserTotal2PostgreSql(spark,yesStr)
  }

  def writeDwdProductUser2AdsPUserTotal(spark: SparkSession, yesStr: String) = {

    spark.sql("use ads")

    val createSql =
      s"""
        |create table if not exists ads_puser_total(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |user_count string,
        |gid       string
        |) partitioned by (count_date string) stored as parquet
      """.stripMargin

    spark.sql(createSql)

    val etlSql =
      s"""
        |insert overwrite table ads_puser_total
        |select ress.pid,ress.com,ress.country,ress.province,count(distinct(ress.user_id)),
        |grouping_id(),'${yesStr}' as gid
        | from
        |(select ress.pid,ress.com,ress.user_id,ress.country,ress.province,ress.city from
        |(select temp.user_id,temp.pid,temp.com,temp.country,temp.province,temp.city,
        |row_number() over(partition by temp.user_id,temp.pid,temp.com order by temp.cou desc) as rkk from
        |(select t1.user_id,t1.product_id as pid,t1.company as com,t2.country,t2.province,t2.city,
        |count(1) over(partition by t1.product_id,t1.company,t1.user_id,t2.country,t2.province,
        |t2.city) as cou from
        |dwd.dwd_product_user as t1 left join dwd.dwd_user_area as t2 on
        |t1.product_id=t2.product_id and t1.company=t2.company and t1.user_id=t2.active_user and t2.country='中国') as temp )
        |as ress) as ress group by ress.pid,ress.com,ress.country,ress.province
        |grouping sets (
        |(ress.pid,ress.com,ress.country,ress.province),
        |(ress.pid,ress.com)
        |)
      """.stripMargin

    spark.sql(etlSql)

  }

  def writeAdsPUserTotal2PostgreSql(spark: SparkSession, yesStr: String) = {

    val props = DbProperties.propScp
    props.setProperty("tableName","ads_puser_total")
    props.setProperty("write_mode","Append")

    spark.sql("use ads")

    val selectSql =
      s"""
         |select * from ads_puser_total
         |where count_date='${yesStr}'
      """.stripMargin

    val df_1: Dataset[Row] = spark.sql(selectSql).coalesce(2)

    df_1.write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName"),props)
  }
}
