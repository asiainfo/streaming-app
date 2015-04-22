package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat

import com.asiainfo.ocdc.streaming.tool.DataConvertTool
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame

/**
 * Created by leo on 4/9/15.
 */
class MCEventRule extends EventRule {

  def output_dir: String = conf.get("outputdir")

  override def output(data: DataFrame) {
    transforEvent2Message(data).saveAsTextFile(output_dir)
  }

  override def transforEvent2Message(data: DataFrame): RDD[String] = {
    val selcol_size = selectExp.size
    data.map(row => {
      println(" every column values ")
      for(i <- 0 to (row.length-1)){
        println(" column "+ i + " : " + row.get(i))
      }
      var message: String = ""
      for (i <- 0 to (selcol_size - 1)) {
        message += row.get(i).toString + getDelim
      }
      println("Output Message --> " + message)
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
      if (inputs(6) == "000000000000000" && inputs(7) == "000000000000000") {
        logError(" Emsi is wrong ! ")
        None
      }else{
        val sdf = new SimpleDateFormat("yyyymmdd hh:mm:ss")
        val eventID = inputs(0).toInt
        val time = sdf.parse(inputs(1)).getTime

        // FIXME lac ci need convert to 16 , test is 10
        /*val lac = inputs(2).toInt
        val ci = inputs(3).toInt*/
        val lac = DataConvertTool.convertHex(inputs(2))
        val ci = DataConvertTool.convertHex(inputs(3))

        val imei = inputs(4)

        var imsi = ""
        if (eventID == 3 || eventID == 5 || eventID == 7) {
          imsi = inputs(7)
        } else {
          imsi = inputs(6)
        }

        val eventresult = inputs(8).toInt
        val alertstatus = inputs(9).toInt
        val assstatus = inputs(10).toInt
        val clearstatus = inputs(11).toInt
        val relstatus = inputs(12).toInt
        val xdrtype = inputs(13).toInt

        Some(new MCSourceObject(eventID, time, lac, ci, imsi, imei, eventresult, alertstatus, assstatus, clearstatus, relstatus, xdrtype))
      }
    } catch {
      case e: Exception => {
        logError(" Source columns have wrong type ! ")
        e.printStackTrace()
        None
      }
    }
  }

}
