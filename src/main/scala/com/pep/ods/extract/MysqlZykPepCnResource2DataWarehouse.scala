package com.pep.ods.extract

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  *   将教材加工库上Mysql中jxw_platfrom库中的p_textbook全量业务用户表导入数仓的ods
  *   层的ods_jxw_platform_p_textbook的put_date=20190000分区中
  *
  *   hadoop fs -rmr /pep_cloud/business/ods/ods_zyk_pep_cn_resource/put_date=20190000
  *   hadoop fs -rmr /pep_cloud/business/ods/ods_zyk_pep_cn_attach/put_date=20190000
  *   hadoop fs -rmr /pep_cloud/business/ods/ods_zyk_pep_cn_file/put_date=20190000
  *   hadoop fs -rmr /pep_cloud/business/ods/ods_zyk_pep_cn_tree/put_date=20190000
  */
object MysqlZykPepCnResource2DataWarehouse {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-MysqlJxwPlatfromResource2DataWarehouse")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val props = new java.util.Properties
    val zyk_resource = "zyk_resource"
    val zyk_file = "zyk_file"
    val zyk_attach = "zyk_attach"
    val zyk_tree = "zyk_tree"
    val zyk_resource_push = "zyk_resource_push"
    props.setProperty("user","pdadmin")
    props.setProperty("password","R8$7Zo319%0tUi")
    props.setProperty("url","jdbc:mysql://rm-2zefoq89s74i5bfal.mysql.rds.aliyuncs.com:3306/yw_bus")
    val default_row_timestamp = System.currentTimeMillis()
    val default_row_status = "1"

    /**
      * 导入表 zyk_resource
      */
    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
    val predicates1 = Array(
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
    ).map( item => s"mod(rid,10)=${item}")
    val zykResourceDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),zyk_resource,predicates1,props)
    //创建临时表
    zykResourceDF.createOrReplaceTempView("zyk_resource_tmp")
    //将临时表写入数仓
    val etlSql1 =
      s"""
         |select *,'$default_row_timestamp' as row_timestamp,'$default_row_status' as row_status from zyk_resource_tmp
       """.stripMargin
    val etlDF = spark.sql(etlSql1)
    val write_path1 = "hdfs://emr-cluster/pep_cloud/business/ods/ods_zyk_pep_cn_resource/put_date=20190000"
    val writeDF = etlDF.coalesce(20)
    writeDF.write.json(write_path1)


    /**
      * 导入表 zyk_file
      */
    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
    val predicates2 = Array(
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
    ).map( item => s"mod(file_size,10)=${item}")

    val zykFileDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),zyk_file,predicates2,props)
    //创建临时表
    zykFileDF.createOrReplaceTempView("zyk_file_tmp")

    //将临时表写入数仓
    val etlSql2 =
      s"""
         |select *,'$default_row_timestamp' as row_timestamp,'$default_row_status' as row_status from zyk_file_tmp
       """.stripMargin

    val etlDF2 = spark.sql(etlSql2)
    val write_path2 = "hdfs://emr-cluster/pep_cloud/business/ods/ods_zyk_pep_cn_file/put_date=20190000"
    val writeDF2 = etlDF2.coalesce(20)
    writeDF2.write.json(write_path2)


    /**
      * 导入表 zyk_attach_tmp
      */
    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
    val predicates3 = Array(
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
    ).map( item => s"mod(rid,10)=${item}")

    val zykAttachDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),zyk_attach,predicates3,props)

    //创建临时表
    zykAttachDF.createOrReplaceTempView("zyk_attach_tmp")

    //将临时表写入数仓
    val etlSql3 =
      s"""
         |select *,'$default_row_timestamp' as row_timestamp,'$default_row_status' as row_status from zyk_attach_tmp
       """.stripMargin
    val etlDF3 = spark.sql(etlSql3)
    val write_path3 = "hdfs://emr-cluster/pep_cloud/business/ods/ods_zyk_pep_cn_attach/put_date=20190000"
    val writeDF3 = etlDF3.coalesce(20)
    writeDF3.write.json(write_path3)


    /**
      * 导入表 zyk_attach_tmp
      */
    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
    val predicates4 = Array(
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
    ).map( item => s"mod(tid,10)=${item}")

    val zykTreeDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),zyk_tree,predicates4,props)

    //创建临时表
    zykTreeDF.createOrReplaceTempView("zyk_tree_tmp")

    //将临时表写入数仓
    val etlSql4 =
      s"""
         |select *,'$default_row_timestamp' as row_timestamp,'$default_row_status' as row_status from zyk_tree_tmp
       """.stripMargin
    val etlDF4 = spark.sql(etlSql4)
    val write_path4 = "hdfs://emr-cluster/pep_cloud/business/ods/ods_zyk_pep_cn_tree/put_date=20190000"
    val writeDF4 = etlDF4.coalesce(20)
    writeDF4.write.json(write_path4)

    /**
      * 导入表 ods_zyk_pep_cn_resource_push
      */
    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
    val predicates5 = Array(
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
    ).map( item => s"mod(pid,10)=${item}")

    val zykPushDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),zyk_resource_push,predicates5,props)

    //创建临时表
    zykPushDF.createOrReplaceTempView("zyk_push_tmp")

    //将临时表写入数仓
    val etlSql5 =
      s"""
         |select *,'$default_row_timestamp' as row_timestamp,'$default_row_status' as row_status from zyk_push_tmp
       """.stripMargin
    val etlDF5 = spark.sql(etlSql5)
    val write_path5 = "hdfs://emr-cluster/pep_cloud/business/ods/ods_zyk_pep_cn_resource_push/put_date=20190000"
    val writeDF5 = etlDF5.coalesce(20)
    writeDF5.write.json(write_path5)

    spark.stop()
  }
}
