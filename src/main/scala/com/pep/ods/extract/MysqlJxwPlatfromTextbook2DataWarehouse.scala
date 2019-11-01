package com.pep.ods.extract

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *   将教材加工库上Mysql中jxw_platfrom库中的p_textbook全量业务用户表导入数仓的ods
  *   层的ods_jxw_platform_p_textbook的put_date=20190000分区中
  *   hadoop fs -rmr /pep_cloud/business/ods/ods_jxw_platform_p_textbook/put_date=20190000
  */
object MysqlJxwPlatfromTextbook2DataWarehouse {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-MysqlProductUserTotal2DataWarehouse")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val props = new java.util.Properties
    val tableName = "p_textbook"
    props.setProperty("user","root")
    props.setProperty("password","123456")
    props.setProperty("url","jdbc:mysql://192.168.186.36:3306/jxw_platform")

    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
    val predicates = Array(
      "2017-07",
      "2017-08",
      "2017-09",
      "2017-11",
      "2017-12",
      "2018-01",
      "2018-02",
      "2018-03",
      "2018-04",
      "2018-06",
      "2018-07",
      "2018-08",
      "2018-09",
      "2019-01",
      "2019-02",
      "2019-03",
      "2019-04",
      "2019-05",
      "2019-06"
    ).map( item => s"substring(s_create_time,1,7)='${item}'")

    val mysqlReadDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),tableName,predicates,props)
    val default_row_timestamp = System.currentTimeMillis()
    val default_row_status = "1"
    mysqlReadDF.createOrReplaceTempView("p_textbook_tmp")
    //将临时表写入数仓
    val etlSql =
      s"""
         |select id,
         |name,
         |sub_heading,
         |rkxd,
         |rkxd_name,
         |nj,
         |nj_name,
         |zxxkc,
         |zxxkc_name,
         |fascicule,
         |fascicule_name,
         |year,
         |isbn,
         |publisher,
         |'' as chapter_json,
         |source_id,
         |ex_content_version,
         |down_times,
         |s_state,
         |s_creator,
         |s_creator_name,
         |s_create_time,
         |s_modifier,
         |s_modifier_name,
         |s_modify_time,
         |ex_books,
         |ex_booke,
         |ex_pages,
         |ed,
         |publication_time,
         |folio,
         |series,
         |content,
         |res_size,
         |tb_version,
         |res_version,'$default_row_timestamp' as row_timestamp,'$default_row_status' as row_status from p_textbook_tmp
       """.stripMargin
    val etlDF = spark.sql(etlSql)
    var write_path = "hdfs://ns/pep_cloud/business/ods/ods_jxw_platform_p_textbook/put_date=20190000"
    val writeDF = etlDF.coalesce(20)
    writeDF.write.json(write_path)



    spark.stop()
  }
}
