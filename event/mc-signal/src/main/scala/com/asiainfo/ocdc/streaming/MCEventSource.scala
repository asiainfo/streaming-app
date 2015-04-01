package com.asiainfo.ocdc.streaming

import kafka.serializer.StringDecoder
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

import scala.util.Random

class MCEventSource() extends EventSource() {

  var conf: EventSourceConf = null

  override def readSource(ssc: StreamingContext): DStream[String] = {

//    val kafkaParams = Map(
//      "zookeeper.connect" -> conf.getString("source.zookeeper"),
//      "group.id" -> s"test-consumer-${Random.nextInt(10000)}",
//      "auto.offset.reset" -> "smallest")
//    val topic = conf.getString("source.topic")
//    val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
//      ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)
//    stream.map(_._2)
    ssc.textFileStream("hdfs://localhost:8020/user/tianyi/streaming/input")

  }

  def formatSource(inputs: Array[String]): Option[MCSourceObject] = {
    // 事件ID,时间,LAC,CI,主叫IMEI,被叫IMEI,主叫IMSI,被叫IMSI
    try {
      val eventID = inputs(0).toInt
      val time = inputs(1).toLong
      val lac = inputs(2).toInt
      val ci = inputs(3).toInt
      val simei = inputs(4).toInt
      val dimei = inputs(5).toInt
      val simsi = inputs(6).toInt
      val dimsi = inputs(7).toInt
      Some(MCSourceObject(eventID, time, lac, ci, simei, dimei, simsi, dimsi))
    } catch {
      case e: Exception => {
        None
      }
    }
  }

  override def transform(source: String): Option[MCSourceObject] = {
    val inputArray = source.split(conf.getString("source.format.delim"))
    if(source.length != conf.getInt("source.format.length")) {
      None
    } else {
      formatSource(inputArray)
    }
  }

  override def init(conf: EventSourceConf): Unit = {
    this.conf = conf
  }

  override def beanclass: String = MCSourceObject.getClass.getName
}


