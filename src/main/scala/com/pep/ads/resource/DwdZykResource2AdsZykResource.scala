package com.pep.ads.resource

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.{Constants, DbProperties}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks.{break, breakable}

object DwdZykResource2AdsZykResource {


  def writeDwdZykResource2AdsZykResource(spark: SparkSession, yestStr: String) = {
    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_resource_zyk(
        |tb_id              string,
        |tb_state           string,
        |nj                 string,
        |zxxkc              string,
        |dzwjlx             string,
        |dzwjlx_name        string,
        |ex_zynrlx          string,
        |ex_zynrlx_name     string,
        |ex_zycj            string,
        |s_state            string,
        |count_file         string,
        |sum_size           string,
        |zyk_create_time    string,
        |zyk_publish_time   string,
        |zywz               string
        |) stored as textfile
      """.stripMargin
    spark.sql(createSql)

    val insertSql =
      s"""
         |insert overwrite table ads_resource_zyk
         |select
         |tb_id,
         |'' as tb_state,
         |dws.geteducode(tb_id,'nj') as nj,
         |dws.geteducode(tb_id,'zxxkc') as zxxkc,
         |r_ext as dzwjlx,
         |r_ext as dzwjlx_name,
         |'' as ex_zynrlx,
         |'' as ex_zynrlx_name,
         |'' as ex_zycj,
         |r_status as s_status,
         |cast(count(1) as decimal(32,0)) as count_file,
         |cast(sum(file_size) as decimal(32,0))  as sum_size,
         |from_unixtime(cast(substring(create_time, 1, 10) as bigint),'yyyyMMdd') as zyk_create_time,
         |from_unixtime(cast(substring(publish_time, 1, 10) as bigint),'yyyyMMdd') as zyk_publish_time,
         |'0' as zywz
         |from dwd.dwd_resource_zyk  where nvl(tb_id,'') !=''
         |group by tb_id,r_status,dws.geteducode(tb_id,'nj') ,cid3,r_ext,
         |from_unixtime(cast(substring(create_time, 1, 10) as bigint),'yyyyMMdd'),
         |from_unixtime(cast(substring(publish_time, 1, 10) as bigint),'yyyyMMdd')
      """.stripMargin

    spark.sql(insertSql)
    val selectSql =
      s"""
        |select * from ads_resource_zyk
      """.stripMargin
    val readDate = spark.sql(selectSql)

    val props = DbProperties.propScp

    var writeDF = readDate.coalesce(5)
    writeDF.write.format("jdbc").
      mode("overwrite").
      jdbc(props.getProperty("url"),"ads_resource_zyk",props)


  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("run-DwdZykResource2AdsZykResource").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")//禁止广播
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
    writeDwdZykResource2AdsZykResource(spark, yestStr)

  }
}
