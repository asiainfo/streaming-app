package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.eventsource.EventSource
import com.asiainfo.ocdc.streaming.tool.{DateFormatUtils, DataConvertTool}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

class MCEventSource() extends EventSource() {

  def formatSource(inputs: Array[String]): Option[MCSourceObject] = {
    // 事件ID,时间,LAC,CI,主叫IMEI,被叫IMEI,主叫IMSI,被叫IMSI
    try {
      if (inputs(6) == "000000000000000" && inputs(7) == "000000000000000") {
//        logError(" Emsi is wrong ! ")
        None
      } else {
        val eventID = inputs(0).toInt
        val time = inputs(1)
        val timeMs = DateFormatUtils.dateStr2Ms(time, "yyyyMMdd HH:mm:ss.SSS")

        val validWindowsTimeMs = conf.getInt("validWindowsTimeMs", -1)
        if(validWindowsTimeMs > -1 && (timeMs + validWindowsTimeMs < System.currentTimeMillis())){ //信令日志生成时间不在有效范围内
          None
        } else { //信令日志生成时间在有效范围内
          // FIXME lac ci need convert to 16 , test is 10
          /*val lac = inputs(2).toInt
          val ci = inputs(3).toInt*/
          val lac = DataConvertTool.convertHex(inputs(2))
          val ci = DataConvertTool.convertHex(inputs(3))

          var callingimei = inputs(4)
          if (callingimei.length > 14) callingimei = callingimei.substring(0, 14)

          var calledimei = inputs(5)
          if (calledimei.length > 14) calledimei = calledimei.substring(0, 14)

          val callingimsi = inputs(6)
          val calledimsi = inputs(7)

          val callingphone = inputs(8)
          val calledphone = inputs(9)

          val eventresult = inputs(10).toInt
          val alertstatus = inputs(11).toInt
          val assstatus = inputs(12).toInt
          val clearstatus = inputs(13).toInt
          val relstatus = inputs(14).toInt
          val xdrtype = inputs(15).toInt
          val issmsalone = inputs(16).toInt

          var imei = ""
          var imsi = ""

          if (List(3, 5, 7).contains(eventID)) {
            imei = calledimei
            imsi = calledimsi
          } else if (List(8, 9, 10, 26).contains(eventID)) {
            if (issmsalone == 1) {
              imei = callingimei
              imsi = callingimsi
            }
            else if (issmsalone == 2) {
              imei = calledimei
              imsi = calledimsi
            }
            else {
              return None
            }
          } else {
            imei = callingimei
            imsi = callingimsi
          }

          Some(new MCSourceObject(eventID, time, lac, ci, callingimei, calledimei, callingimsi, calledimsi,
            callingphone , calledphone , eventresult, alertstatus, assstatus, clearstatus, relstatus, xdrtype, issmsalone, imsi, imei))
        }

      }
    } catch {
      case e: Exception => {
//        logError(" Source columns have wrong type ! ")
        e.printStackTrace()
        None
      }
    }
  }

  override def transform(source: String): Option[MCSourceObject] = {
    val inputArray = source.split(conf.get("delim"))
    if (inputArray.length != conf.getInt("formatlength")) {
      logError(" Source format is wrong ! ")
      logError(" error data --> " + source)
      None
    } else {
      formatSource(inputArray)
    }
  }

  override def transformDF(sqlContext: SQLContext, labeledRDD: RDD[SourceObject]): DataFrame = {
    import sqlContext.implicits.rddToDataFrameHolder
    labeledRDD.map(_.asInstanceOf[MCSourceObject]).toDF()
  }

}


