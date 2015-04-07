package com.asiainfo.ocdc.streaming

import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by leo on 4/7/15.
 */
object EventSourceReader {
  def readSource(ssc: StreamingContext, conf: EventSourceConf): DStream[String] = {
    val sourceType = conf.get("type")

    if ("kafka".equals(sourceType)) {
      val kafkaParams = Map("zookeeper.connect" -> conf.get("zookeeper"),
        "group.id" -> conf.get("group"),
        "auto.offset.reset" -> conf.get("autooffset"))
      val stream = KafkaUtils.createStream(
        ssc, kafkaParams, Map(conf.get("topic") -> 1), StorageLevel.MEMORY_ONLY)
      stream.map(_._2)

    } else if ("hdfs".equals(sourceType)) {
      ssc.textFileStream(conf.get("path"))
    } else {
      throw new Exception("EventSourceType " + sourceType + " is not support !")
    }
  }
}
