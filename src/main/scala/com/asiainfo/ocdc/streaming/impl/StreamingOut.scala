package com.asiainfo.ocdc.streaming.impl

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import java.util.Properties
import org.apache.spark.streaming.dstream.DStream
import scala.xml.Elem

/**
 * Created by maji3 on 14-6-11.
 */
class StreamingOut {
  // input parameter: Dstream and configuration file
  def kafka(inStream:DStream[Array[String]],xmlFile: Elem) :DStream[Array[String]] ={
    val inputFile = xmlFile
    val delim = ","
    var input,topic,brokers,kafkacol,out = ""
    var result = inStream

    inputFile match {
      case <configuration>{allSymbol @ _*}</configuration> =>
        for(symbolNode @ <step>{_*}</step> <- allSymbol){
          val style = (symbolNode \ "@out").text.toString
          style match {
            case "kafka" =>
              input = (symbolNode \ "Input").text.toString
              topic = (symbolNode \ "topic").text.toString
              brokers = (symbolNode \ "broker").text.toString
              kafkacol = (symbolNode \ "KafkaCol").text.toString
            case _ => println("skip")
          }
        }
    }
    val inputarray = input.split(delim)
    val kafkaarray = kafkacol.split(delim)

    result = inStream.map(x=>{
      for(arg <- kafkaarray){
        out += x(inputarray.indexOf(arg))
      }
      val product_no =out
      val producer = StreamingOut.getProducer(brokers)
      var message =List[KeyedMessage[String, String]]()
      message = new KeyedMessage[String, String](topic, product_no)::message
      producer.send(message: _*)
      x
    })
    result
  }

}

object StreamingOut {

  val producer = null

  def getProducer(brokers:String):Producer[String, String]={

    if( null != producer ) producer
    else {
      val props = new Properties()
      props.put("metadata.broker.list", brokers)
      props.put("serializer.class", "kafka.serializer.StringEncoder")
      val config = new ProducerConfig(props)
      new Producer[String, String](config)
    }

  }
}