package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming._
import org.apache.spark.streaming.dstream.DStream
import scala.xml.Node
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

class KafkaSource(ssc:StreamingContext) extends StreamingSource(ssc){

  def createStream(source:Node):DStream[Array[String]]={
    val zkQuorum = (source \ "zkQuorum").text.toString
    val topics = (source \ "topics").text.toString
    val group = (source \ "groupId").text.toString
    val consumerNum = (source \ "consumerNum").text.toInt
    val separator = (source \ "separator").text.toString
    val topicpMap = topics.split(",").map((_,1)).toMap
    val stream = (1 to consumerNum).map(_=>KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap)).reduce(_.union(_)).map(_._2)
    stream.map(_.split(separator))
  }
}
