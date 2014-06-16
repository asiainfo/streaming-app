package com.asiainfo.ocdc.streaming.tools

import kafka.producer.{KeyedMessage, ProducerConfig, Producer}
import java.util.Properties

object KafkaProducer {
  val producer:Producer[String, String] = null

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
