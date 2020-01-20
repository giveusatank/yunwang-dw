package com.pep.common

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object DataDump2PGAll {
  //4 将Ads层UV相关数据写入PostgreSQL
  def writeAdsData2PostgreSQL(spark: SparkSession, tableName: String): Unit = {
    val props = DbProperties.propScp
    props.setProperty("write_mode", "Append")
    spark.sql("use ads")
    val tableList: List[String]  = List(
      "ads_uv_total",
      "ads_uv_increase",
      "ads_uv_incr_area_until_week_month",
      "ads_uv_daily",
      "ads_uv_area_until_week_month",
      "ads_used_session_user",
      "ads_used_session",
      "ads_textbook_zxxkc_used_total",
      "ads_textbook_user_area",
      "ads_textbook_used_total_cube",
      "ads_textbook_used_total",
      "ads_textbook_used_avg_use_day",
      "ads_textbook_used_avg_time_consume",
      "ads_textbook_per_used_total",
      "ads_textbook_nj_used_total",
      "ads_terminal_property",
      "ads_sale_analysis_width_total",
      "ads_sale_analysis_width_day",
      "ads_rj_ip_daily",
      "ads_rj_dpi_daily",
      "ads_rj_browser_daily",
      "ads_rj_access_statistic_daily",
      "ads_rj_access_count_daily",
      "ads_rj_access_browse_pages_daily",
      "ads_resource_summary",
      "ads_reg_area_until_week_month",
      "ads_puser_total",
      "ads_puser_increase",
      "ads_puser_conversion",
      "ads_order_user_zxxkc",
      "ads_order_user_ym",
      "ads_order_user_total",
      "ads_order_user_nj",
      "ads_order_user_area_zxxkc_nj_ym",
      "ads_order_user_area_zxxkc_nj_total",
      "ads_order_user_area_ym",
      "ads_order_user_area_total",
      "ads_order_increase",
      "ads_order_entity_ym",
      "ads_order_entity_total",
      "ads_order_channel_total",
      "ads_order_channel_daily",
      "ads_order_area_total",
      "ads_order_area_daily",
      "ads_active_reg_user"
    )

    val querySql_1 =
      s"""
         |select * from ${tableName}
      """.stripMargin

    spark.sql(querySql_1).coalesce(5).write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"), tableName, props)


  }

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("DataDump2PG").set("spark.sql.shuffle.partitions", Constants.ads_shuffle_partitions)
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    args.foreach(s =>{
      action(spark, s)
    })
    spark.stop()
  }

  def action(spark: SparkSession, tableName: String): Unit = {
    writeAdsData2PostgreSQL(spark,tableName)
  }
}
