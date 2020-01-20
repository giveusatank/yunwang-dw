package com.pep.dwd

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.control.Breaks

object OdsZykResource2DwdZykResource {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-OdsZykResource2DwdZykResource").set("spark.sql.shuffle.partitions","40")
      /*
      select * from (select *, row_number() over (partition by rid order by row_timestamp desc ) num from ods_zyk_pep_cn_resource ) where num=1 and row_status='1'

      select * from (select *, row_number() over (partition by attach_id order by row_timestamp desc ) num from ods_zyk_pep_cn_attach ) where num=1 and row_status='1'

      select * from (select *, row_number() over (partition by file_md5 order by row_timestamp desc ) num from ods_zyk_pep_cn_file ) where num=1 and row_status='1'
      *
      *
      select * from
      (select a.*,c.file_md5 as rel_file_md5,c.file_url_view,c.file_url,c.file_size,c.file_oname,c.file_name,c.file_extension
      from zyk_resource a , zyk_attach b , zyk_file c
      where a.rid=b.rid and b.file_md5=c.file_md5
      union all
      select aa.*,bb.file_md5 as rel_file_md5,bb.file_url_view,bb.file_url,bb.file_size,bb.file_oname,bb.file_name,bb.file_extension
      from zyk_resource aa , zyk_file bb
      where aa.file_md5=bb.file_md5) t;
      * */

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val regPatten = "^[0-9]{8}$".r
    val flag = args.length > 0
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE,-1)
    var yesStr = format.format(cal.getTime)
    cal.add(Calendar.DATE, -2)
    val _2DaysBefore: String = format.format(cal.getTime)

    loop.breakable{
      for(i <- 0 until (if(args.length > 1) args.length else 1)){
        if(flag) {
          if(regPatten.findPrefixOf(args(i))==None) loop.break()
          yesStr = args(i)
        }
        doAction(spark,yesStr,_2DaysBefore)
      }
      spark.stop()
    }
  }

  def doAction(spark:SparkSession, yesStr:String,_2DaysBefore:String): Unit ={

    OdsZykResource2DwdZykResourceWidth(spark,yesStr,_2DaysBefore)
  }

  def OdsZykResource2DwdZykResourceWidth(spark: SparkSession, yesStr: String,_2DaysBefore:String): Unit = {

    spark.sql("use dwd")
    val createSql =
      """
        |create table if not exists dwd_resource_zyk(
        |rid                   string,
        |file_md5              string,
        |r_name                string,
        |r_desc                string,
        |r_key                 string,
        |r_people              string,
        |r_language            string,
        |r_thumb               string,
        |r_ext                 string,
        |cid1                  string,
        |cid2                  string,
        |cid3                  string,
        |rtype1                string,
        |rtype2                string,
        |tid1_path             string,
        |tid2_path             string,
        |c_version             string,
        |c_type                string,
        |c_date_start          string,
        |c_date_end            string,
        |c_owner               string,
        |c_scope               string,
        |c_scope_has           string,
        |c_author              string,
        |c_from                string,
        |create_time           string,
        |publish_time          string,
        |r_status              string,
        |uid                   string,
        |uname                 string,
        |downs                 string,
        |favs                  string,
        |hits                  string,
        |stars                 string,
        |attachs               string,
        |r_grade               string,
        |r_birth_year          string,
        |r_senv                string,
        |r_sver_min            string,
        |r_sver_max            string,
        |r_sdesc               string,
        |r_splat               string,
        |c_sug                 string,
        |c_authorship          string,
        |c_payfor              string,
        |c_limit_area          string,
        |c_limit_user          string,
        |c_limit_plat          string,
        |r_version             string,
        |rpy                   string,
        |tid3_path             string,
        |tid4_path             string,
        |tid5_path             string,
        |row_timestamp         string,
        |row_status            string,
        |put_date              string,
        |num                   string,
        |rel_file_md5          string,
        |file_url_view         string,
        |file_url              string,
        |file_size             string,
        |file_oname            string,
        |file_name             string,
        |file_extension        string,
        |pid                   string,
        |chapter_id            string,
        |tb_id                 string)
        |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
      """.stripMargin

    spark.sql(createSql)
    spark.sql("msck repair table ods.ods_zyk_pep_cn_resource")
    spark.sql("msck repair table ods.ods_zyk_pep_cn_attach")
    spark.sql("msck repair table ods.ods_zyk_pep_cn_file")
    spark.sql("msck repair table ods.ods_zyk_pep_cn_tree")
    spark.sql("truncate table dwd.dwd_resource_zyk")
    //sql :
    val readSql =
      """
        |select t.*,t3.chapter_id as chapter_id,if(!isnull(t3.chapter_id),dws.geteducode(t3.chapter_id,'educode'),'') as tb_id from
        |(select a.*,c.file_md5 as rel_file_md5,c.file_url_view,c.file_url,c.file_size,c.file_oname,c.file_name,c.file_extension,d.pid
        |from (select * from (select *, row_number() over (partition by rid order by row_timestamp desc ) num from ods.ods_zyk_pep_cn_resource) where num=1 and row_status in ('1','2')) a
        | join (select * from (select *, row_number() over (partition by attach_id order by row_timestamp desc ) num from ods.ods_zyk_pep_cn_attach) where num=1 and row_status in ('1','2')) b on a.rid=b.rid
        | join (select * from (select *, row_number() over (partition by file_md5 order by row_timestamp desc ) num from ods.ods_zyk_pep_cn_file) where num=1 and row_status in ('1','2')) c on b.file_md5=c.file_md5
        | left join (select * from (select *, row_number() over (partition by pid order by row_timestamp desc ) num from ods.ods_zyk_pep_cn_resource_push) where num=1 and row_status in ('1','2')) d on b.attach_id=d.attach_id
        | union all
        |select aa.*,bb.file_md5 as rel_file_md5,bb.file_url_view,bb.file_url,bb.file_size,bb.file_oname,bb.file_name,bb.file_extension,dd.pid
        |from (select * from (select *, row_number() over (partition by rid order by row_timestamp desc ) num from ods.ods_zyk_pep_cn_resource) where num=1 and row_status in ('1','2')) aa
        | join (select * from (select *, row_number() over (partition by file_md5 order by row_timestamp desc ) num from ods.ods_zyk_pep_cn_file) where num=1 and row_status in ('1','2')) bb on aa.file_md5=bb.file_md5
        | left join (select * from (select *, row_number() over (partition by pid order by row_timestamp desc ) num from ods.ods_zyk_pep_cn_resource_push) where num=1 and row_status in ('1','2')) dd on aa.rid=dd.rid
        | )  t join
        |(select split(chapter_ids,'\\,')[size(split(chapter_ids,'\\,'))-1] as chapter_id,rid from (select explode(split(tid1_path,'\\\\|')) as chapter_ids,rid  from ods.ods_zyk_pep_cn_resource )) t2
        |on t.rid=t2.rid
        |left join ods.ods_zyk_pep_cn_tree t3 on t2.chapter_id=t3.tid
      """.stripMargin

    val readRddDF:DataFrame = spark.sql(readSql)
    var write_path = s"hdfs://ns/hive/warehouse/dwd.db/dwd_resource_zyk"
    val writeDF= readRddDF.coalesce(20)
    writeDF.write.mode("overwrite").json(write_path)

  }

}