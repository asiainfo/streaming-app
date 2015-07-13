package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.eventsource.EventSource
import com.asiainfo.ocdc.streaming.tool.DataConvertTool
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
//        val time = DateFormatUtils.dateStr2Ms(inputs(1), "yyyyMMdd HH:mm:ss.SSS")
        val time = inputs(1)

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

        val eventresult = inputs(8).toInt
        val alertstatus = inputs(9).toInt
        val assstatus = inputs(10).toInt
        val clearstatus = inputs(11).toInt
        val relstatus = inputs(12).toInt
        val xdrtype = inputs(13).toInt
        val issmsalone = inputs(14).toInt

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
          else None
        } else {
          imei = callingimei
          imsi = callingimsi
        }

        Some(new MCSourceObject(eventID, time, lac, ci, callingimei, calledimei, callingimsi, calledimsi,
          eventresult, alertstatus, assstatus, clearstatus, relstatus, xdrtype, issmsalone, imsi, imei))
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


