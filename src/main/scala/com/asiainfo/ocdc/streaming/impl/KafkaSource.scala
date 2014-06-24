package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming._
import org.apache.spark.streaming.dstream.DStream
import scala.xml.Node
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.StreamingContext

class KafkaSource(ssc:StreamingContext) extends StreamingSource(ssc){

  def createStream(source:Node):DStream[Array[(String,String)]]={
    val zkQuorum = (source \ "zkQuorum").text.toString
    val topics = (source \ "topics").text.toString
    val group = (source \ "groupId").text.toString
    val consumerNum = (source \ "consumerNum").text.toInt
    val separator = (source \ "separator").text.toString
    val stream_columns = (source \ "stream_columns").text.toString.split(",")
    val topicpMap = topics.split(",").map((_,1)).toMap
    val stream = (1 to consumerNum).map(_=>KafkaUtils.createStream(ssc, zkQuorum, group, topicpMap)).reduce(_.union(_)).map(_._2)

    // 对输入流列名定义
    stream.map(x =>{
      val streamValues = x.split(separator)
      var result : AnyRef = null
      if(stream_columns.size == streamValues.length){
        println("====================== KafkaSource 输出数据 ======================= " )
        result = (0 to streamValues.length-1).map(i=>{
          print(stream_columns(i)+"####"+streamValues(i))
          (stream_columns(i),streamValues(i))
        })

      }else throw new Exception("流数据配置列名与数据格式不符！")

      result.asInstanceOf[Vector[(String,String)]].toArray
    })
  }
}
