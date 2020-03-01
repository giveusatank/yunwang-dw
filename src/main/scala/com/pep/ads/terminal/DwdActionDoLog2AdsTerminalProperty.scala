package com.pep.ads.terminal

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import com.pep.common.DbProperties
import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.util.control.Breaks

/**
  * 从dwd.action_do_log 清洗到 ads.ads_terminal_property
  */
object DwdActionDoLog2AdsTerminalProperty {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RUN-DwdActionDoLog2AdsTerminalProperty")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val regPatten = "^[0-9]{8}$".r
    val flag = args.length > 0
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE,-1)
    var yesStr = format.format(cal.getTime)
    cal.add(Calendar.MONTH,-1)
    var yesMon = format.format(cal.getTime)

    loop.breakable{
      for(i <- 0 until (if(args.length > 1) args.length else 1)){
        if(flag) {
          if(regPatten.findPrefixOf(args(i))==None) loop.break()
          yesStr = args(i)
          cal.setTime(format.parse(yesStr))
          cal.add(Calendar.MONTH,-1)
          yesMon = format.format(cal.getTime)
        }
        println(yesStr)
        println(yesMon)
        doAction(spark,yesStr,yesMon)
      }
      spark.stop()
    }
  }

  def writeDwdActionDoLog2AdsTerminalProperty(spark: SparkSession, yesStr: String,yesMon: String) = {

    spark.sql("use ads")

    val createSql =
      """
        |create table if not exists ads_terminal_property(
        |product_id string,
        |company string,
        |country string,
        |province string,
        |os string,
        |dpi string,
        |brand string,
        |launch_type string,
        |isp string,
        |connect_way string,
        |grouping_id string,
        |quantity string
        |) partitioned by (count_date string) stored as parquet
      """.stripMargin
    spark.sql(createSql)

    val etlSql =
      s"""
        |insert overwrite table ads.ads_terminal_property partition(count_date='${yesStr}')
        |select temp2.product_id,temp2.company,temp2.country,temp2.province,temp2.os_c,temp2.dpi_c,temp2.brand_c,temp2.machine_c,
        |temp2.operator_c,temp2.connect_c,temp2.gid,temp2.cnt from
        |(select temp1.gid,temp1.product_id,temp1.company,temp1.country,temp1.province,temp1.dpi_c,temp1.brand_c,temp1.machine_c,temp1.os_c,
        |temp1.operator_c,temp1.connect_c,row_number() over(partition by temp1.gid,
        |temp1.product_id,temp1.company,temp1.country,temp1.province order by temp1.cou desc) as rak,temp1.cou as cnt from
        |(select product_id,company,country,province,upper(str_to_map(hardware)['dpi']) as dpi_c,
        |upper(dws.yunwangdateformat("brand",str_to_map(hardware)['m-type'])) as brand_c,
        |upper(str_to_map(hardware)['m-type']) as machine_c,
        |upper(dws.yunwangdateformat('os',(upper(if(instr(str_to_map(os)['os'],'iOS')!=0 or instr(str_to_map(os)['os'],'iPhone')!=0,'IOS',
        |if(instr(str_to_map(os)['os'],'Windows')!=0,regexp_extract(str_to_map(os)['os'],'(\\\\D*\\\\d*\\\\.\\\\d*)',0),'Android')))))) as os_c,
        |upper(if(instr(upper(str_to_map(os)['c-type']),'UNICOM')!=0,'中国联通',if(instr(upper(str_to_map(os)['c-type']),'MOBILE')!=0 or
        |instr(upper(str_to_map(os)['c-type']),'中國移動')!=0 or instr(upper(str_to_map(os)['c-type']),'CMCC')!=0,'中国移动',
        |if(instr(upper(str_to_map(os)['c-type']),'TELECOM')!=0,'中国电信',str_to_map(os)['c-type'])))) as operator_c,
        |upper(if(nvl(str_to_map(os)['net-type'],'')!='',str_to_map(os)['net-type'],
        |if(nvl(str_to_map(os)['c-net-type'],'')!='',dws.yunwangdateformat('connect',str_to_map(os)['c-net-type']),NULL))) as connect_c
        |,grouping_id() as gid,count(distinct(device_id)) as cou
        |from dwd.action_do_log where put_date='${yesStr}' and log_version='2'
        |group by product_id,company,country,province,dpi_c,brand_c,machine_c,os_c,operator_c,connect_c grouping sets((product_id,company,country,province,dpi_c),
        |(product_id,company,country,province,brand_c),
        |(product_id,company,country,province,machine_c),
        |(product_id,company,country,province,os_c),
        |(product_id,company,country,province,operator_c),
        |(product_id,company,country,province,connect_c))) as temp1 ) as temp2 where temp2.rak<=10
      """.stripMargin

    spark.sql(etlSql)
  }

  def writeAdsTerminalProperty2PostgreSql(spark: SparkSession, yesStr: String) = {

    val props = DbProperties.propScp
    props.setProperty("tableName","ads_terminal_property")
    props.setProperty("write_mode","Append")

    spark.sql("use ads")

    val selectSql =
      s"""
         |select product_id,company,country,province,os,dpi,brand,launch_type,isp,
         |connect_way,grouping_id,quantity,count_date from ads_terminal_property
         |where count_date='${yesStr}'
      """.stripMargin

    val df_1: Dataset[Row] = spark.sql(selectSql).coalesce(2)

    df_1.write.mode(props.getProperty("write_mode")).
      jdbc(props.getProperty("url"),props.getProperty("tableName"),props)
  }




  def doAction(spark:SparkSession, yesStr:String,yesMon: String) = {
    writeDwdActionDoLog2AdsTerminalProperty(spark,yesStr,yesMon)
    writeAdsTerminalProperty2PostgreSql(spark,yesStr)
  }

}
