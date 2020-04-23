package com.pep.dim

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 将172.30.0.9上Mysql中user_db库中的p_user全量业务用户表导入数仓的ods
  * 层的ods_product_user的put_date=20190000分区中
  */
object DimCompanyDate2DW {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("RUN-DimCompanyDate2DW")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val props = new java.util.Properties
    val tableName = "p_customer"
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
    ).map(x => s"mod(s_state,10)=${x}")

    val mysqlReadDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"),tableName,predicates,props)

    val createSql =
      s"""
         |create table if not exists dim.dim_company(
         |company            string,
         |company_name           string
         |)
         |STORED AS parquet
      """.stripMargin
    spark.sql(createSql)

    //创建临时表

    mysqlReadDF.createOrReplaceTempView("dim_company_tmp")
    //将临时表写入数仓
    val etlSql =
      s"""
         |select DISTINCT t.company as company,t.company_name as company_name from (
         |select key as company,name as company_name from dim_company_tmp
         |union all
         |select key_new as company,name as company_name from dim_company_tmp
         |union all
         |select key_old as company,name as company_name from dim_company_tmp) t where COALESCE(t.company) !=''
       """.stripMargin

    val etlDF: DataFrame = spark.sql(etlSql)

    val writeDF = etlDF.coalesce(1)
    writeDF.show(1000)
    writeDF.write.mode("overwrite").parquet("hdfs://emr-cluster/user/hive/warehouse/dim.db/dim_company")

    spark.stop()
  }

}
