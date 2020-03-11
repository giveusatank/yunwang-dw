package com.pep.dwd.order

import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.util.control.Breaks


object OdsJxwCtreeRel2DwdOrderWidth {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("RUN-OdsJxwCtreeRel2DwdOrderWidth")
    val spark = SparkSession.builder().config(conf).enableHiveSupport().getOrCreate()
    val loop = new Breaks
    val regPatten = "^[0-9]{8}$".r
    val flag = args.length > 0
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    cal.add(Calendar.DATE, -1)
    var yesStr = format.format(cal.getTime)

    loop.breakable {
      for (i <- 0 until (if (args.length > 1) args.length else 1)) {
        if (flag) {
          if (regPatten.findPrefixOf(args(i)) == None) loop.break()
          yesStr = args(i)
        }
        doAction(spark, yesStr, getBeforeDay(yesStr))
      }
      spark.stop()
    }
  }

  def getBeforeDay(timeStr:String) = {
    val format = new SimpleDateFormat("yyyyMMdd")
    val cal = Calendar.getInstance()
    cal.setTime(format.parse(timeStr))
    cal.add(Calendar.DATE,-1)
    format.format(cal.getTime)
  }


  def doAction(spark:SparkSession, yesStr:String, beforeYesStr:String): Unit ={
    //writeOdsJxwCtreeRel2DwdOrderWidth(spark,yesStr,beforeYesStr)
  }



}
