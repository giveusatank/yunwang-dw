package com.pep.dwd

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.control.Breaks

object OdsJxwTextbook2DwdTextbookJxw {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-OdsJxwTextbook2DwdTextbookJxw")
    /*
      select * from (select *, row_number() over (partition by id order by row_timestamp desc ) num from ods_jxw_platform_p_textbook ) where num=1 and row_status='1'
    */
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val regPatten = "^[0-9]{8}$".r
    val flag = args.length > 0
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -1)
    var yesStr = format.format(cal.getTime)
    cal.add(Calendar.DATE, -2)
    val _2DaysBefore: String = format.format(cal.getTime)

    loop.breakable {
      for (i <- 0 until (if (args.length > 1) args.length else 1)) {
        if (flag) {
          if (regPatten.findPrefixOf(args(i)) == None) loop.break()
          yesStr = args(i)
        }
        doAction(spark, yesStr, _2DaysBefore)
      }
      spark.stop()
    }
  }

  def doAction(spark: SparkSession, yesStr: String, _2DaysBefore: String): Unit = {

    OdsJxwTextbook2DwdTextbookJxw(spark, yesStr, _2DaysBefore)
  }

  def OdsJxwTextbook2DwdTextbookJxw(spark: SparkSession, yesStr: String, _2DaysBefore: String): Unit = {

    spark.sql("use dwd")
    val createSql =
      """
        |create table if not exists dwd_textbook_jxw(
        |id                   string,
        |name                 string,
        |sub_heading          string,
        |rkxd                 string,
        |rkxd_name            string,
        |nj                   string,
        |nj_name              string,
        |zxxkc                string,
        |zxxkc_name           string,
        |fascicule            string,
        |fascicule_name       string,
        |year                 string,
        |isbn                 string,
        |publisher            string,
        |chapter_json         string,
        |source_id            string,
        |ex_content_version   string,
        |down_times           string,
        |s_state              string,
        |s_creator            string,
        |s_creator_name       string,
        |s_create_time        string,
        |s_modifier           string,
        |s_modifier_name      string,
        |s_modify_time        string,
        |ex_books             string,
        |ex_booke             string,
        |ex_pages             string,
        |ed                   string,
        |publication_time     string,
        |folio                string,
        |series               string,
        |content              string,
        |res_size             string,
        |tb_version           string,
        |res_version          string,
        |row_timestamp        string,
        |row_status           string,
        |put_date             string,
        |num                  string
        |)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      """.stripMargin

    spark.sql(createSql)
    spark.sql("msck repair table ods.ods_jxw_platform_p_textbook")
    spark.sql("truncate table dwd.dwd_textbook_jxw")

    val selectSql =
      """
        |select * from (
        |select *, row_number() over (partition by id order by row_timestamp desc ) num from ods.ods_jxw_platform_p_textbook
        |) where num=1 and row_status in ('1','2')
      """.stripMargin
    val readRddDF:DataFrame = spark.sql(selectSql)

    var write_path = s"hdfs://emr-cluster//hive/warehouse/dwd.db/dwd_textbook_jxw/"

    val writeDF = readRddDF.repartition(20)
    writeDF.write.mode("overwrite").json(write_path)

  }

}