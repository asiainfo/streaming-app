package com.asiainfo.ocdc.streaming.tool

import java.util.Properties

import com.asiainfo.ocdc.streaming.MainFrameConf
import kafka.producer.{KeyedMessage, ProducerConfig, Producer}

/**
 * Created by leo on 7/4/15.
 */
object KafkaSendTool {

  private val currentProducer = new ThreadLocal[Producer[String, String]] {
    override def initialValue = getProducer
  }

  private def getProducer: Producer[String, String] = {
    val props = new Properties()
    props.put("metadata.broker.list", MainFrameConf.get("brokerlist"))
    props.put("serializer.class", MainFrameConf.get("serializerclass"))
    new Producer[String, String](new ProducerConfig(props))
  }

  def sendMessage(message: List[KeyedMessage[String, String]]) {
    currentProducer.get().send(message: _*)
  }

  def close() {
    currentProducer.get().close()
  }

}
