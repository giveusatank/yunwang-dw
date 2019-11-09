package com.pep.ads

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf


/**
 * 
 */
object DwdDevice2AdsDevice 
{
    def main(args: Array[String]): Unit = 
    {
      val conf = new SparkConf().setAppName("DwdDevice2AdsDevice")//.setMaster("local[4]")
      val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
      spark.sparkContext.setCheckpointDir("/tmp/checkpoint/")
      spark.sql("show databases").show()
    /**
 
 select product_id,company,province,put_date, str_to_map(os,',',':')['os'], str_to_map(hardware,',',':')['dpi'],str_to_map(hardware,',',':')['m-type'],str_to_map(os,',',':')['c-type'],str_to_map(os,',',':')['net-type'],count(1) cnt 
 from action_do_log 
 where log_version='2' 
 group by product_id,company,province,put_date, str_to_map(os,',',':')['os'], str_to_map(hardware,',',':')['dpi'],str_to_map(hardware,',',':')['m-type'],str_to_map(os,',',':')['c-type'],str_to_map(os,',',':')['net-type'] 
 limit 200;
  
  
     * 
     */
    
    }
}