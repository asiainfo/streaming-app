package com.asiainfo.ocdc.streaming.tools

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import java.util.Properties
/**
 * Created by maji3 on 14-6-12.
 */
object KafkaProducer {
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
