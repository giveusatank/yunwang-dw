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
    props.setProperty("user","pdadmin")
    props.setProperty("password","R8$7Zo319%0tUi")
    props.setProperty("url","jdbc:mysql://rm-2zefoq89s74i5bfal.mysql.rds.aliyuncs.com:3306/yw_bus")

    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
    val predicates = Array(
      0,
      1,
      2,
      3,
      4,
      5,
      6,
      7,
      8,
      9
    ).map(x => s"mod(id,10)=${x}")

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
    var write_path = "hdfs://emr-cluster/pep_cloud/business/ods/ods_jxw_platform_p_textbook/put_date=20190000"
    val writeDF = etlDF.coalesce(20)
    writeDF.write.json(write_path)



    spark.stop()
  }
}
