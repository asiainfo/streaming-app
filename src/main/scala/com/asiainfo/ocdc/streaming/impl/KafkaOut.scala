package com.asiainfo.ocdc.streaming.impl

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import org.apache.spark.streaming.dstream.DStream
import scala.xml.Node
import com.asiainfo.ocdc.streaming.StreamingStep
import com.asiainfo.ocdc.streaming.tools.KafkaProducer

class KafkaOut extends StreamingStep{

  def onStep(step:Node,inStream:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]={
    val delim = ","
    var result = inStream
    val input = (step \ "Input").text.toString.split(delim)
    val topic = (step \ "topic").text.toString
    val brokers = (step \ "broker").text.toString
    val outcol = (step \ "OutCol").text.toString.split(delim)

    result = inStream.map(x=>{
      var InputMap = x
      var out = ""
      for(arg <- outcol){
        val item =  InputMap.toMap
        out += item(input.indexOf(arg)) + delim
      }
      val kafkaout =out
      val key = kafkaout.split(delim)(0)
      val producer = KafkaProducer.getProducer(brokers)
      var message =List[KeyedMessage[String, String]]()
      message = new KeyedMessage[String, String](topic,key,kafkaout)::message
      producer.send(message: _*)
      x
    })
    result
  }
}

