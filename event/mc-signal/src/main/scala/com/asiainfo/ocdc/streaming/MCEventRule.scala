package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat

import org.apache.commons.lang.StringUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Created by leo on 4/9/15.
 */
class MCEventRule extends EventRule {

  def getDelim: String = "|"

  def inputLength: Int = 4

  override def output(data: DataFrame) {
    transforEvent2Message(data).saveAsTextFile("hdfs://localhost:9000/user/leo/streaming/output")
  }

  override def transforEvent2Message(data: DataFrame): RDD[String] = {
    val selcol_size = selectExp.size
    data.map(row => {
      var message: String = ""
      for (i <- 0 to (selcol_size - 1)) {
        message += row.get(i).toString + getDelim
      }
      message
    })
  }

  override def transforMessage2Event(message: RDD[String]): RDD[Option[SourceObject]] = {
    message.map(x => {
      val inputArray = x.split(getDelim)
      if(inputArray.length != inputLength) {
        None
      } else {
        formatSource(inputArray)
      }
    })
  }

  def formatSource(inputs: Array[String]): Option[SourceObject] = {
    // 事件ID,时间,LAC,CI,主叫IMEI,被叫IMEI,主叫IMSI,被叫IMSI
    try {
      val sdf = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss")
      val eventID = inputs(0).toInt
      val time = sdf.parse(inputs(1)).getTime
      val lac = inputs(2).toInt
      val ci = inputs(3).toInt

      var imei: Long = 0
      if(StringUtils.isNumeric(inputs(4))) imei = inputs(4).toLong

      val imsi = inputs(6).toLong
      Some(new MCSourceObject(eventID, time, lac, ci, imsi, imei))
    } catch {
      case e: Exception => {
        None
      }
    }
  }

}
