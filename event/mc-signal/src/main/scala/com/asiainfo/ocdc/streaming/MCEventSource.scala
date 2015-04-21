package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

class MCEventSource() extends EventSource() {

  def formatSource(inputs: Array[String]): Option[MCSourceObject] = {
    // 事件ID,时间,LAC,CI,主叫IMEI,被叫IMEI,主叫IMSI,被叫IMSI
    try {
      if (inputs(6) == "000000000000000" && inputs(7) == "000000000000000") {
        None
      }
      val sdf = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss")
      val eventID = inputs(0).toInt
      val time = sdf.parse(inputs(1)).getTime
      val lac = inputs(2).toInt
      val ci = inputs(3).toInt

      var imei: Long = 0
      if (StringUtils.isNumeric(inputs(4))) imei = inputs(4).toLong

      var imsi: Long = 0
      if (eventID == 3 || eventID == 5 || eventID == 7) {
        imsi = inputs(7).toLong
      } else {
        imsi = inputs(6).toLong
      }

      val eventresult = inputs(8).toInt
      val alertstatus = inputs(9).toInt
      val assstatus = inputs(10).toInt
      val clearstatus = inputs(11).toInt
      val relstatus = inputs(12).toInt
      val xdrtype = inputs(13).toInt

      Some(new MCSourceObject(eventID, time, lac, ci, imsi, imei, eventresult, alertstatus, assstatus, clearstatus, relstatus, xdrtype))
    } catch {
      case e: Exception => {
        None
      }
    }
  }

  override def transform(source: String): Option[MCSourceObject] = {
    val inputArray = source.split(conf.get("delim"))
    if (inputArray.length != conf.getInt("formatlength")) {
      None
    } else {
      formatSource(inputArray)
    }
  }

  override def beanclass: String = "com.asiainfo.ocdc.streaming.MCSourceObject"

  override def makeEvents(sqlContext: SQLContext, labeledRDD: RDD[SourceObject]) {
    import sqlContext.implicits.rddToDataFrameHolder
    if (labeledRDD.partitions.length > 0) {
      val df = labeledRDD.map(_.asInstanceOf[MCSourceObject]).toDF
      // cache data
      df.persist
      df.printSchema()

      val eventRuleIter = eventRules.iterator
      while (eventRuleIter.hasNext) {
        val eventRule = eventRuleIter.next
        eventRule.selectExp.foreach(x => print(" " + x + ""))

        // handle filter first
        val filteredData = df.filter(eventRule.filterExp)

        // handle select
        val selectedData = filteredData.selectExpr(eventRule.selectExp: _*)

        eventRule.output(selectedData)

      }

      df.unpersist()
    }
  }
}


