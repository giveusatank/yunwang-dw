package com.pep.ods.history

import scala.collection.Map
import scala.reflect.ClassTag
import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import scala.io.Source
import java.io.PrintWriter
import org.apache.hadoop.hdfs.server.common.Storage
import org.apache.spark.api.java.StorageLevels
import org.apache.hadoop.hdfs.StorageType
import org.apache.spark.storage.StorageLevel
import java.sql.Date

object Extractor
{
  
  /**
   * <p>抽取全集数据制定时间区间的数据</p>
   * 
   * @param : 
   * start_time 待抽取时间区间的开始 
   * end_time 待抽取时间区间的结束
   * cnt_all 目前系统中全集数据的总数
   * cnt_extract 要抽取的总数
   * 
   * 1.转原始文件
   * 表头
   * id	remote_addr	request_time	log_version	start_time	end_time	region	product_id	hardware	os	soft	active_user	active_org	active_type	passive_obj	passive_type	from_prod	from_pos	company	action_title	action_type	request	request_param	group_type	group_id	result_flag	result                  string 
select from_unixtime(cast(start_time/1000 as int),'yyyyMMdd-HH') key, CONCAT(id,'~',nvl(remote_addr,''),'~',nvl(request_time,''),'~',nvl(log_version,''),'~',nvl(cast(start_time as string) ,''),'~',nvl(cast(end_time as string),''),'~',nvl(region,''),'~',nvl(product_id,''),'~',nvl(hardware,''),'~',nvl(os,''),'~',nvl(soft,''),'~',nvl(active_user,''),'~',nvl(active_org,''),'~',nvl(cast(active_type as string),''),'~',nvl(passive_obj,''),'~',nvl(passive_type,''),'~',nvl(from_prod,''),'~',nvl(from_pos,''),'~',nvl(company,''),'~',nvl(action_title,''),'~',nvl(cast(action_type as string),''),'~',nvl(request,''),'~',nvl(request_param,''),'~',nvl(cast(group_type as string),''),'~',nvl(group_id,''),'~',nvl(cast(result_flag as string),''),'~',nvl(result,'')) value
from action_log_ot where start_time>UNIX_TIMESTAMP('2019-03-31 00:00:00') * 1000 and start_time<UNIX_TIMESTAMP('2019-05-01 00:00:00') * 1000 limit 2;
   * 2.按小时统计pv
   * elect from_unixtime(floor(start_time/1000),'yyyy-MM-dd-HH')  hour,count(1) pv  from action_log_ot group by from_unixtime(floor(start_time/1000),'yyyy-MM-dd-HH'); 
   * 
   */
  def  main(args: Array[String]): Unit = {
    if(args==null||args.length<3) 
    {
      println("No find args!!!Return.")
      return;
    }
    val start_time=args(0)
    val end_time=args(1)
    val cnt_all=args(2).toLong //全量数据总记录数
    val cnt_extract=if(args(3)!=null) args(3).toLong else 1000*10000l //默认抽1000万条
   println("======start_time:"+start_time+";end_time:"+end_time+";cnt_all:"+cnt_all+";cnt_extract:"+cnt_extract)
    val conf = new SparkConf()
    conf.setAppName("PepLogExtractor")//.setMaster("local[4]")
    val spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate()
   spark.sparkContext.setCheckpointDir("/tmp/checkpoint/")
  
    import spark.sql
    sql("show databases").show()
    sql("use dwd")
    val sql_1 =s"""
select from_unixtime(cast(start_time/1000 as int),'yyyyMMdd-HH') key, CONCAT(id,'~',nvl(remote_addr,''),'~',nvl(request_time,''),'~',nvl(log_version,''),'~',nvl(cast(start_time as string) ,''),'~',nvl(cast(end_time as string),''),'~',nvl(region,''),'~',nvl(product_id,''),'~',nvl(hardware,''),'~',nvl(os,''),'~',nvl(soft,''),'~',nvl(active_user,''),'~',nvl(active_org,''),'~',nvl(cast(active_type as string),''),'~',nvl(passive_obj,''),'~',nvl(passive_type,''),'~',nvl(from_prod,''),'~',nvl(from_pos,''),'~',nvl(company,''),'~',nvl(action_title,''),'~',nvl(cast(action_type as string),''),'~',nvl(request,''),'~',nvl(request_param,''),'~',nvl(cast(group_type as string),''),'~',nvl(group_id,''),'~',nvl(cast(result_flag as string),''),'~',nvl(result,'')) value
from action_do_log where start_time>UNIX_TIMESTAMP('$start_time 00:00:00') * 1000 and start_time<UNIX_TIMESTAMP('$end_time 00:00:00') * 1000 distribute by cast(round(rand()*903) as int)
    """
    println("======="+sql_1)
    //1.查询数据集
    
    var frame=spark.sql(sql_1)//尽可能多拆分出占用空间大的rdd，以便释放最初最大的原始rdd。
    var recordRdd=frame.rdd.map(x => {(new Random().nextInt(97).toString()+"|"+x.getAs[String]("key"), x.getAs[String]("value")) })//倾斜：把每个小时再细分到333台机器上
    frame.unpersist(false)
    frame=null
    //recordRdd=recordRdd.repartition(1000)
    //recordRdd=recordRdd.persist(StorageLevels.MEMORY_AND_DISK_SER)
    
    val hRdd1=recordRdd.groupByKey()  //倾斜：第一次分组
    recordRdd=recordRdd.unpersist(false)//默认不会释放，手动释放
    recordRdd=null
    
    var hRdd=hRdd1.map(x=>{ (x._1.split("\\|")(1),x._2)}).groupByKey(500).map(x=>{
      
     (x._1, x._2.reduce(_++_))
      
    })//倾斜：第二次分组
    hRdd=hRdd.repartition(2000)
    hRdd1.unpersist(false)
    
    //2,抽取并返回（yyyyMMdd，data）的形式，但其实没有按天分组,需要reduceByKey
    val dRdd=hRdd.map(x=>(x._1.split("-")(0),fun_extract(x,cnt_all,cnt_extract)))
      
    val dpRdd= dRdd.reduceByKey(_++_)
          
    val data=dpRdd.collect()
    //3.写文件，怎么写，一个大文件吗？最终加载肯定是按天分区
    data.foreach(x=>{
      println("=============day is :"+x._1+". sum of record is:"+x._2.size)
      val dir=new java.io.File("/data/extractor_log/action_log_ot/put_date="+x._1+"/");
      dir.mkdirs()
      println("=============dir exists:"+dir.getAbsolutePath+". dir is:"+dir.exists())
      val out = new PrintWriter("/data/extractor_log/action_log_ot/put_date="+x._1+"/"+x._1+".txt")
      x._2.foreach(s=>{
        out.println(s)
      })
        
      out.close();
      
    })
    
  }
  
  /**
   * 抽取当前小时，满足比例数的随机记录
   */
  val fun_extract=(h:(String, Iterable[String]),cnt:Long,cnt_extract:Long)=>{
        
        val rate=(h._2.size.toDouble/cnt.toDouble).formatted("%.10f").toDouble
        
        //待抽取数:每小时占已有总数比，再乘以待抽取比
        val numb_Extract=if(Math.round(cnt_extract*rate).toInt<1) 1 else Math.round(cnt_extract*rate).toInt //最少取1条
        //生成随机数索引
        val map_idx=createRdomMap(h._2.toArray,numb_Extract,System.currentTimeMillis())
        
        
        //zipWithIndex先转成（_,idx）形式，再过滤掉非索引内数据 
        val r=h._2.zipWithIndex.filter(x=>map_idx.contains(x._2)).map(_._1)
/*        println("        ")
        println("h._2.size:"+h._2.size+" and cnt is:"+cnt+".")
        println("This "+h._1+" hour's rate is:"+rate+" and numb_Extract is:"+numb_Extract+" and map_idx size is:"+map_idx.size+" and r size is:"+r.size+".")
        println("        ")*/
        r
  }

  /**
   * 根据原数组大小，生成随机数组，并转成map[idx,1]
   */
  def createRdomMap[T:ClassTag](a:Array[T],n:Int,seed:Long):Map[Int,Int] = {
    val rnd = new Random()
    val b=Array.fill(n)(rnd.nextInt(a.size))
    val aa= b.map((_,1)).toMap
    aa
  }
  
  def main1(args: Array[String]): Unit = {
    
    println((("0"+"|"+"20190205-13").split("\\|"))(1))
  }
}





