/*
package com.asiainfo.ocdc.streaming

import java.util
import java.util.Properties
import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}

/**
 * Created by leo on 4/29/15.
 */
object MuThreadsSendKafka {

  /*private val currentJedis = new ThreadLocal[Jedis] {
    override def initialValue = new Jedis("localhost", 6379)
  }*/

  //  final def getJedis = currentJedis.get()
  val props: Properties = new Properties
  props.put("zookeeper.connect", "localhost:2181")
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("zookeeper.session.timeout.ms", "10000")
  props.put("zookeeper.sync.time.ms", "10000")
  props.put("auto.commit.interval.ms", "10000")
  /*props.put("request.timeout.ms", "60000")
  props.put("producer.type", "async")
  props.put("retry.backoff.ms", "3000")
  props.put("queue.buffering.max.ms", "5000")
  props.put("queue.buffering.max.messages", "50000")
  props.put("queue.enqueue.timeout.ms", "0")*/

  val producer = new Producer[String, String](new ProducerConfig(props))

  final def getProducer = producer

  val messageList: util.List[KeyedMessage[String, String]] = new util.ArrayList[KeyedMessage[String, String]]()
  val message: KeyedMessage[String, String] = new KeyedMessage[String,String]("mc_signal","key1","AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
  for(i <- 0 to 10000){
    messageList.add(message)
  }

  import scala.actors.Actor._

  val a1 = actor {
    val producer: Producer[String, String] = getProducer
    var work = true
    while (work) {
      receive {
        case x: Int => {
          var i = 0
          while (i < 1000) {
            println("send from actor1 time : " + i)
            producer.send(messageList)
            i = i + 1
          }
        }
        case y: String => work = false
      }
    }
  }

  val a2 = actor {
    val producer: Producer[String, String] = getProducer
    var work = true
    while (work) {
      receive {
        case x: Int => {
          var i = 0
          while (i < 1000) {
            println("send from actor2 time : " + i)
            producer.send(messageList)
            i = i + 1
          }
        }
        case y: String => work = false
      }
    }
  }


  def main(args: Array[String]) {
    a1 ! 1
    a2 ! 1
    /*for (i <- 0 to 1000000) {

    }*/
  }


}
*/
