package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.eventrule.EventRule

/**
 * Created by leo on 4/9/15.
 */
class MCEventRule extends EventRule {

  /*override def output(data: DataFrame) {
    val output_msg = transforEvent2Message(data)
    if (output_msg.partitions.length > 0) {
      val f5 = System.currentTimeMillis()
//      EventWriter.writeData(output_msg, conf)
      logDebug(" Write HDFS cost time : " + (System.currentTimeMillis() - f5) + " millis ! ")
    }
  }*/

  /*override def transforEvent2Message(data: DataFrame): RDD[(String, String)] = {
    val selcol_size = selectExp.size
    val kafka_key = conf.getInt("kafkakeycol")
    val delim = getDelim
    data.map(row => {
      val key: String = row.get(kafka_key).toString
      var message: String = ""
      for (i <- 0 to (selcol_size - 1)) {
        message += row.get(i).toString + delim
      }
      message = message.substring(0, (message.length - delim.length))

      println("Output Message --> " + message)
      (key, message)
    })
  }*/

  /*override def transforMessage2Event(message: RDD[String]): RDD[Option[SourceObject]] = {
    message.map(x => {
      val inputArray = x.split(getDelim)
      if (inputArray.length != inputLength) {
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
      } else {
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
  }*/

}
