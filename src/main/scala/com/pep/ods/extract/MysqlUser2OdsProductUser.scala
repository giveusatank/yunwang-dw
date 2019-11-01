package com.pep.ods.extract


import com.pep.common.{Constants, MappingRuleConfig, MappingRuleInit}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * 将172.30.0.9上Mysql中user_db库中的p_user全量业务用户表导入数仓的ods
  * 层的ods_product_user的put_date=20190000分区中
  */
object MysqlUser2OdsProductUser {
  val product_user_tmp = "product_user_tmp"

  def main(args: Array[String]): Unit = {
    MappingRuleInit.init(args(0))
    val conf = new SparkConf().setAppName("RUN-MysqlUser2OdsProductUser")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val props = new java.util.Properties
    var mappingRule: MappingRuleConfig = MappingRuleInit.mappingRuleConfig
    if (mappingRule == null) {
      return
    }
    val tableName = mappingRule.getTable_name
    props.setProperty("user", mappingRule.getDb_user)
    props.setProperty("password", mappingRule.getDb_password)
    props.setProperty("url", mappingRule.getDb_url)

    //定义SparkSQL读取Mysql的线程数量，以及线程的读取数据量
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
    ).map(x => s"mod(${mappingRule.getPartitionsByKey},10)=${x}")

    val productUserDF: DataFrame = spark.read.format("jdbc").jdbc(props.getProperty("url"), tableName, predicateArray, props)
    //创建临时表
    productUserDF.createOrReplaceTempView(product_user_tmp)

    val createSql =
      s"""
         |create external table if not exists ods.ods_product_user(
         |user_id            string,
         |product_id         string,
         |company            string,
         |reg_name           string,
         |nick_name          string,
         |real_name          string,
         |phone              string,
         |email              string,
         |sex                string,
         |birthday           string,
         |address            string,
         |org_id             string,
         |user_type          string,
         |first_access_time  string,
         |last_access_time   string,
         |last_access_ip     string,
         |country            string,
         |province           string,
         |city               string,
         |location           string,
         |row_timestamp      string,
         |row_status         string
         |)
         |partitioned by (put_date string)
         |ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
         |stored as textfile location '/pep_cloud/business/ods/ods_product_user'
      """.stripMargin
    spark.sql(createSql)
    val etlSql = MappingRuleInit.generateSQL(MappingRuleInit.mappingRuleConfig, product_user_tmp)
    print(etlSql)
    spark.sql(etlSql)
    spark.stop()
  }

  def main1(args: Array[String]): Unit = {
    MappingRuleInit.init(args(0))
    print(MappingRuleInit.generateSQL(MappingRuleInit.mappingRuleConfig, "temp"))

  }
}
