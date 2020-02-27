package com.pep.ods.extract

import java.util.Date

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

/**
  * 智慧教学平台的授权表全量导入数仓
  *
  * 172.30.0.9上Mysql中jxw库中a_user_ctree_rel表
  * 导入数仓ods层ods_jxw_platform_textbook的count_date=20190000分区下
  *
  */
object MysqlJxwWebUserCtreeRel2DataWarehouse {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-MysqlJxwWebUserCtreeRel2DataWarehouse")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val prop = new java.util.Properties()
    prop.setProperty("user","pdadmin")
    prop.setProperty("password","R8$7Zo319%0tUi")
    prop.setProperty("url","jdbc:mysql://rm-2zefoq89s74i5bfal.mysql.rds.aliyuncs.com:3306/yw_bus")
    prop.setProperty("tableName","jxw_web_a_user_ctree_rel")

    val predicateArray = Array(
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

    val jxwWebCtreeRelDS: Dataset[Row] = spark.read.jdbc(prop.getProperty("url"),prop.getProperty("tableName"),predicateArray,prop)

    jxwWebCtreeRelDS.createOrReplaceTempView("tmp_user_ctree_rel")

    //获取当前时间戳
    val currentTimeStamp = new Date().getTime

    val insertStatus = "1"

    val jxwWebCtreeRelResDS:Dataset[Row] = spark.sql(s"select *,${currentTimeStamp},${insertStatus} from tmp_user_ctree_rel")

    jxwWebCtreeRelResDS.createOrReplaceTempView("insert_user_ctree_rel")

    val createSql =
      s"""
         |create external table if not exists ods.ods_jxw_platform_user_ctree_rel(
         |id string,
         |user_id string,
         |user_name string,
         |user_seting string,
         |org_id string,
         |org_name string,
         |edu_code string,
         |rkxd string,
         |zxxkc string,
         |publisher string,
         |nj string,
         |fascicule string,
         |year string,
         |keywords string,
         |ctree_id string,
         |ctree_name string,
         |sub_heading string,
         |s_state string,
         |score string,
         |s_version string,
         |range_type string,
         |ctree_related_object string,
         |view_numb string,
         |down_numb string,
         |s_creator string,
         |s_creator_name string,
         |s_create_time string,
         |valid_time string,
         |authorization_code string,
         |authorization_type string,
         |authorization_way string,
         |end_time string,
         |reg_time string,
         |row_timestamp string,
         |row_status string
         |) partitioned by (count_date string) row format serde 'org.apache.hive.hcatalog.data.JsonSerDe'
         |stored as textfile location '/pep_cloud/business/ods/ods_jxw_platform_user_ctree_rel'
      """.stripMargin
    spark.sql("use ods")
    spark.sql(createSql)
    val insertSql =
    s"""
         |insert overwrite table ods_jxw_platform_user_ctree_rel partition (count_date='20190000')
         |select * from insert_user_ctree_rel
      """.stripMargin

    spark.sql(insertSql)

    spark.stop()
  }
}
