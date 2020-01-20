package com.pep.ads.resource

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.{Constants, DbProperties}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks.{break, breakable}

object DwdJxwResource2AdsJxwResource {


  def writeDwdJxwResource2AdsJxwResource(spark: SparkSession, yestStr: String) = {
    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_resource_jxw(
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
        |jxw_create_time    string,
        |zyk_create_time    string,
        |zywz               string
        |)stored as textfile
      """.stripMargin
    spark.sql(createSql)


    //创建时间小于20191016为资源加工库元数据
    //select split(chapter_ids,'\,')[size(split(chapter_ids,'\,'))-1] as chapter_id,rid from (select explode(split(tid1_path,'\\|')) as chapter_ids,rid  from ods.ods_zyk_pep_cn_resource where tid1_path like  '%|%' limit 1);
    val insertSql =
      s"""
         |insert overwrite table ads.ads_resource_jxw
         |select
         |a.tb_id,tb_state,nj,zxxkc, dzwjlx ,dzwjlx_name,ex_zynrlx,ex_zynrlx_name,ex_zycj,s_state,
         |cast(count(1) as decimal(32,0)) count_file,
         |cast(sum(a.file_size) as decimal(32,0)) as sum_size,
         |date_format(a.s_create_time,'yyyyMMdd') as jxw_create_time,
         |if(isnull(b.create_time) or date_format(a.s_create_time,'yyyyMMdd')<'20191016' ,date_format(a.s_create_time,'yyyyMMdd'),from_unixtime(cast(substring(b.create_time, 1, 10) as bigint),'yyyyMMdd')) as zyk_create_time ,
         |if(isnull(b.create_time) or date_format(a.s_create_time,'yyyyMMdd')<'20191016',1,0) as zywz
         |FROM dwd.dwd_resource_jxw a left join dwd.dwd_resource_zyk b on a.file_md5=b.rel_file_md5 and a.source_pid=b.pid
         |GROUP BY a.tb_id,tb_state,nj,zxxkc, dzwjlx ,dzwjlx_name,ex_zynrlx,ex_zynrlx_name,ex_zycj,s_state,
         |date_format(a.s_create_time,'yyyyMMdd'),
         |if(isnull(b.create_time) or date_format(a.s_create_time,'yyyyMMdd')<'20191016' ,date_format(a.s_create_time,'yyyyMMdd'),from_unixtime(cast(substring(b.create_time, 1, 10) as bigint),'yyyyMMdd')),
         |if(isnull(b.create_time) or date_format(a.s_create_time,'yyyyMMdd')<'20191016',1,0)
      """.stripMargin

    spark.sql(insertSql)
    val selectSql =
      s"""
        |select * from ads_resource_jxw
      """.stripMargin
    val readDate = spark.sql(selectSql)

    val props = DbProperties.propScp

    var writeDF = readDate.coalesce(5)
    writeDF.write.format("jdbc").
      mode("overwrite").
      jdbc(props.getProperty("url"),"ads_resource_jxw",props)


  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("run-DwdJxwResource2AdsJxwResource").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
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
    writeDwdJxwResource2AdsJxwResource(spark, yestStr)

  }
}
