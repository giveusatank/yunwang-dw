package com.pep.dim

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 将172.30.0.9上Mysql中user_db库中的p_user全量业务用户表导入数仓的ods
  * 层的ods_product_user的put_date=20190000分区中
  */
object DimProductDate2DW {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("RUN-DimProductDate2DW")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val props = new java.util.Properties
    val tableName = "a_product"
    props.setProperty("user","pdadmin")
    props.setProperty("password","R8$7Zo319%0tUi")
    props.setProperty("url","jdbc:mysql://rm-2zefoq89s74i5bfal.mysql.rds.aliyuncs.com:3306/pms-web")

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
    ).map(x => s"mod(code,10)=${x}")

    val mysqlReadDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),tableName,predicates,props)

    val createSql =
      s"""
         |create table if not exists dim.dim_product(
         |product           string,
         |product_name      string
         |)
         |STORED AS parquet
      """.stripMargin
    spark.sql(createSql)

    //创建临时表

    mysqlReadDF.createOrReplaceTempView("dim_product_tmp")
    //将临时表写入数仓
    val etlSql =
      s"""
         |select code as product,name as product_name from dim_product_tmp
       """.stripMargin

    val etlDF: DataFrame = spark.sql(etlSql)
    etlDF.show(10000)
    val writeDF = etlDF.coalesce(1)
    writeDF.write.mode("overwrite").parquet("hdfs://emr-cluster/user/hive/warehouse/dim.db/dim_product")

    spark.stop()
  }

}
