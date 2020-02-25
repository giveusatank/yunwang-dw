package com.pep.ods.extract

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *   将教材加工库上Mysql中jxw_platfrom库中的p_textbook全量业务用户表导入数仓的ods
  *   层的ods_jxw_platform_p_textbook的put_date=20190000分区中
  *
  *   hadoop fs -rmr /pep_cloud/business/ods/ods_jxw_platform_p_resource/put_date=20190000
  */
object MysqlJxwPlatfromResource2DataWarehouse {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-MysqlJxwPlatfromResource2DataWarehouse")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val props = new java.util.Properties
    val tableName = "p_resource"
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



    //创建临时表

    val default_row_timestamp = System.currentTimeMillis()
    val default_row_status = "1"

    mysqlReadDF.createOrReplaceTempView("p_resource_tmp")
    //将临时表写入数仓
    val etlSql =
      s"""
         |select *,'$default_row_timestamp' as row_timestamp,'$default_row_status' as row_status from p_resource_tmp
       """.stripMargin

    val etlDF: DataFrame = spark.sql(etlSql)

    var write_path = "hdfs://emr-cluster/pep_cloud/business/ods/ods_jxw_platform_p_resource/put_date=20190000"
    val writeDF = etlDF.coalesce(20)
    writeDF.write.json(write_path)

    spark.stop()
  }
}
