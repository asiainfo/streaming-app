package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.subscribe.BusinessEvent
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

/**
 * Created by leo on 5/11/15.
 */
class MCBSEvent extends BusinessEvent {

  override def joinkey: String = "imsi"

  override def output(data: RDD[Option[Row]]) {
    val output_msg = transforEvent2Message(data)
    if (output_msg.partitions.length > 0) {
      val f5 = System.currentTimeMillis()
      EventWriter.writeData(output_msg, conf)
      logDebug(" Write HDFS cost time : " + (System.currentTimeMillis() - f5) + " millis ! ")
    }
  }

  def transforEvent2Message(data: RDD[Option[Row]]): RDD[(String, String)] = {
    val selcol_size = selectExp.size
    val kafka_key = conf.getInt("kafkakeycol")
    val delim = getDelim

    data.filter(_ != None).map(row => {
      val key: String = row.get(kafka_key).toString
      var message: String = ""
      for (i <- 0 to (selcol_size - 1)) {
        message += row.get.get(i).toString + delim
      }
      message = message.substring(0, (message.length - delim.length))

      println("Output Message --> " + message)
      (key, message)
    })
  }
}
