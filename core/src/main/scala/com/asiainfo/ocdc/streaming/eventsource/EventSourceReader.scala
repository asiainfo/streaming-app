package com.asiainfo.ocdc.streaming.eventsource

import kafka.serializer.StringDecoder
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by leo on 4/7/15.
 */
object EventSourceReader extends org.apache.spark.Logging {
  def readSource(ssc: StreamingContext, conf: EventSourceConf): DStream[String] = {
    val sourceType = conf.get("type")

    if ("kafka".equals(sourceType)) {
      val zkQuorum = conf.get("zookeeper")
      val group = conf.get("group")
      val receiverNum = conf.getInt("receivernum")
      val topicMap = Map(conf.get("topic") -> receiverNum)
      logInfo("Init Kafka Stream : zookeeper->" + zkQuorum + "; groupid->" + group + "; topic->" + topicMap + " ! ")
      KafkaUtils.createStream(ssc, zkQuorum, group, topicMap).map(_._2)

    } else if ("kafka_direct".equals(sourceType)) {
      val topicsSet = conf.get("topic").split(",").toSet
      val brokers = conf.get("brokers")
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
      logInfo("Init Direct Kafka Stream : brokers->" + brokers + "; topic->" + topicsSet + " ! ")
      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
        ssc, kafkaParams, topicsSet).map(_._2)

    } else if ("hdfs".equals(sourceType)) {
      ssc.textFileStream(conf.get("path"))
    } else {
      throw new Exception("EventSourceType " + sourceType + " is not support !")
    }
  }
}
