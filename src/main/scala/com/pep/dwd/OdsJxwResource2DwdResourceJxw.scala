package com.pep.dwd

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.control.Breaks

object OdsJxwResource2DwdResourceJxw {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-OdsJxwResource2DwdResourceJxw").set("spark.sql.shuffle.partitions","40")
    /*
      select * from (select *, row_number() over (partition by id order by row_timestamp desc ) num from ods_jxw_platform_p_resource ) where num=1 and row_status='1'

      select a.*,b.s_state as tb_state,b.zxxkc,b.nj from (
      select * from (select *, row_number() over (partition by id order by row_timestamp desc ) num from ods_jxw_platform_p_resource ) where num=1 and row_status='1'
      ) a join (
      select * from (select *, row_number() over (partition by id order by row_timestamp desc ) num from ods_jxw_platform_p_textbook ) where num=1 and row_status='1'
      ) b on a.tb_id=b.id

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

    OdsJxwResource2DwdResourceJxw(spark, yesStr, _2DaysBefore)
  }

  def OdsJxwResource2DwdResourceJxw(spark: SparkSession, yesStr: String, _2DaysBefore: String): Unit = {

    spark.sql("use dwd")
    val createSql =
      """
        |create table if not exists dwd_resource_jxw(
        |id                            string,
        |keywords                      string,
        |resume                        string,
        |title                         string,
        |year                          string,
        |ex_zycj                       string,
        |dzwjlx                        string,
        |dzwjlx_name                   string,
        |zylx                          string,
        |zylx_name                     string,
        |yhlx                          string,
        |mtgslx                        string,
        |source_id                     string,
        |source_pid                    string,
        |source_app                    string,
        |source_batch                  string,
        |source_handler                string,
        |ori_tree_code                 string,
        |ori_tree_name                 string,
        |ori_tree_pos                  string,
        |s_edu_code                    string,
        |file_path                     string,
        |file_format                   string,
        |file_size                     string,
        |file_md5                      string,
        |file_ecry_type                string,
        |s_bak_flag                    string,
        |pic_pre_sum                   string,
        |pic_thumb_state               string,
        |s_state                       string,
        |s_creator                     string,
        |s_creator_name                string,
        |s_create_time                 string,
        |s_modifier                    string,
        |s_modifier_name               string,
        |s_modify_time                 string,
        |down_numb                     string,
        |score                         string,
        |ex_linktype                   string,
        |ex_linkcolor                  string,
        |ex_linksort                   string,
        |ex1                           string,
        |ex2                           string,
        |ex3                           string,
        |ex4                           string,
        |ex5                           string,
        |ex_turnpage                   string,
        |ex_gallery                    string,
        |ex_zynrlx                     string,
        |ex_zynrlx_name                string,
        |ex_rely                       string,
        |ex_content_version            string,
        |tb_id                         string,
        |jump_page                     string,
        |res_setting                   string,
        |relation_resinfo              string,
        |measure_resinfo               string,
        |res_group                     string,
        |view_numb                     string,
        |ex_jxsx                       string,
        |ex_page                       string,
        |ex_pos_description            string,
        |ex_limit_plat                 string,
        |ex_from                       string,
        |row_timestamp                 string,
        |row_status                    string,
        |put_date                      string,
        |num                           string,
        |tb_state                      string,
        |zxxkc                         string,
        |nj                            string
        |)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      """.stripMargin

    spark.sql(createSql)
    spark.sql("msck repair table ods.ods_jxw_platform_p_resource")
    spark.sql("msck repair table ods.ods_jxw_platform_p_textbook")
    spark.sql("truncate table dwd.dwd_resource_jxw")
    val selectSql =
      """
        |insert overwrite table dwd.dwd_resource_jxw
        |select a.id                      ,
        |a.keywords                       ,
        |a.resume                         ,
        |a.title                          ,
        |a.year                           ,
        |a.ex_zycj                        ,
        |a.dzwjlx                        ,
        |a.dzwjlx_name                   ,
        |a.zylx                          ,
        |a.zylx_name                     ,
        |a.yhlx                          ,
        |a.mtgslx                        ,
        |a.source_id                     ,
        |a.source_pid                    ,
        |a.source_app                    ,
        |a.source_batch                  ,
        |a.source_handler                ,
        |a.ori_tree_code                 ,
        |a.ori_tree_name                 ,
        |a.ori_tree_pos                  ,
        |a.s_edu_code                    ,
        |a.file_path                     ,
        |a.file_format                   ,
        |a.file_size                     ,
        |a.file_md5                      ,
        |a.file_ecry_type                ,
        |a.s_bak_flag                    ,
        |a.pic_pre_sum                   ,
        |a.pic_thumb_state               ,
        |a.s_state                       ,
        |a.s_creator                     ,
        |a.s_creator_name                ,
        |a.s_create_time                 ,
        |a.s_modifier                    ,
        |a.s_modifier_name               ,
        |a.s_modify_time                 ,
        |a.down_numb                     ,
        |a.score                         ,
        |a.ex_linktype                   ,
        |a.ex_linkcolor                  ,
        |a.ex_linksort                   ,
        |a.ex1                           ,
        |a.ex2                           ,
        |a.ex3                           ,
        |a.ex4                           ,
        |a.ex5                           ,
        |a.ex_turnpage                   ,
        |a.ex_gallery                    ,
        |a.ex_zynrlx                     ,
        |a.ex_zynrlx_name                ,
        |a.ex_rely                       ,
        |a.ex_content_version            ,
        |a.tb_id                         ,
        |a.jump_page                     ,
        |a.res_setting                   ,
        |a.relation_resinfo              ,
        |a.measure_resinfo               ,
        |a.res_group                     ,
        |a.view_numb                     ,
        |a.ex_jxsx                       ,
        |a.ex_page                       ,
        |a.ex_pos_description            ,
        |a.ex_limit_plat                 ,
        |a.ex_from                       ,
        |a.row_timestamp                 ,
        |a.row_status                    ,
        |a.put_date                      ,
        |a.num                           ,b.s_state as tb_state,b.zxxkc,b.nj from (
        |select * from (select *, row_number() over (partition by id order by row_timestamp desc ) num from ods.ods_jxw_platform_p_resource ) where num=1 and row_status in ('1','2')
        |) a join (
        |select * from (select *, row_number() over (partition by id order by row_timestamp desc ) num from ods.ods_jxw_platform_p_textbook ) where num=1 and row_status in ('1','2')
        |) b on a.tb_id=b.id
      """.stripMargin
    spark.sql(selectSql)
//    val readRddDF:DataFrame = spark.sql(selectSql)
//
//    var write_path = s"hdfs://emr-cluster/hive/warehouse/dwd.db/dwd_resource_jxw/"
//
//    val writeDF = readRddDF.repartition(20)
//    writeDF.write.mode("overwrite").json(write_path)

  }

}