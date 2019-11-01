package com.pep.dws.order

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.Constants
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks


object DwdOrderDetailWidth2DwsOrderDetailTotal {

  //将dwd层order全量表清洗到dws层order历史累计表
  def writeDwdOrderDetailWidth2DwsOrderDetailTotal(spark: SparkSession, yesToday: String) = {

    spark.sql("use dws")

    val createSql =
      """
        |create table if not exists dws_order_detail_total(
        |app_id bigint,
        |sale_channel_id bigint,
        |user_count bigint,
        |country string,
        |province string
        |) partitioned by (count_date string)
        |stored as parquet
      """.stripMargin

    spark.sql(createSql)
    spark.sql(s"alter table dws.dws_order_detail_total drop if exists partition(count_date=${yesToday})")
    /**
      * 注： 此时dwd层的ods_order_detail_width 还没有 这张表要经过数据的统一化
      */
    val insertSql =
      s"""
         |insert into table dws_order_detail_total partition (count_date)
         |select t1.app_id,
         |       t1.sale_channel_id,
         |       count( distinct(t1.user_id))                 as user_count,
         |       if(nvl(t2.country, '') != '', country, '')   as country,
         |       if(nvl(t2.province, '') != '', province, '') as province,
         |       '${yesToday}'
         |from (select app_id, sale_channel_id, user_id
         |      from dwd.dwd_order_details_width
         |      where create_time <= ${yesToday}) as t1
         |         left join
         |     (select tmp.active_user, tmp.product_id, tmp.company, tmp.country, tmp.province
         |      from (select t.active_user,
         |                   t.product_id,
         |                   t.company,
         |                   t.country,
         |                   t.province,
         |                   t.cout,
         |                   row_number() over (partition by t.active_user order by t.cout desc) as rank
         |            from (select active_user, product_id, company, country, province, count(1) as cout
         |                  from dws.dws_uv_total
         |                  group by product_id, company, country, province,
         |                           active_user) as t) as tmp
         |      where tmp.rank = 1) as t2
         |     on t2.active_user = t1.user_id
         |group by t1.app_id, t1.sale_channel_id, t2.country, t2.province
      """.stripMargin

    spark.sql(insertSql)
  }

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("DwsOrderDetailTotal").set("spark.sql.shuffle.partitions", Constants.dws_shuffle_partitions)

    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()

    val arrLength = args.length
    val withParam = if(args.length>0) true else false
    val regPattern = "^[0-9]{8}$".r
    val loop = new Breaks
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE,-20)
    var yesToday: String = format.format(cal.getTime)

    //如果没传入参数,就是Azkaban调度的,操作Yestoday数据,插入的分区也是Yestoday
    //如果传入参数,就是手动调度的,操作的是传入时间的数据,插入分区也是传入时间
    loop.breakable{
      for (i <- 0 until (if (withParam) {arrLength} else 1)) {
        //是手动传参
        if(withParam){
          //使用正则判断传入日期是否合法
          val res: Option[String] = regPattern.findPrefixOf(args(i))
          //如果输入参数非法,则跳出循环.否则将yesToday时间替换为传入的参数
          if(res==None) loop.break() else yesToday = args(i)
        }
        doAction(spark,yesToday)
      }
      spark.stop()
    }
  }

  def doAction(spark:SparkSession,yesToday:String) = {

    //当昨天的订单增量数据导入ods_order_detail_width之后,计算历史累计订单渠道、地区、UV分析
    writeDwdOrderDetailWidth2DwsOrderDetailTotal(spark, "20190601")

  }
}
