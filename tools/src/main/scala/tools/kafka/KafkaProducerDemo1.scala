package tools.kafka

import java.io.FileInputStream
import java.util.Properties

import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import org.slf4j.LoggerFactory
import tools.DateFormatUtils

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Created by tsingfu on 15/8/5.
 */
object KafkaProducerDemo1 extends App {

  val logger = LoggerFactory.getLogger(this.getClass.getSimpleName.replace("$", ""))

  //解析配置，初始化 kafka producer
  val kafkaProducerPropsFile = args(0)
  val topic = args(1)
  val numMsgInBatch = args(2).toInt
  val numBatches = args(3).toInt

  println("= = " * 20)
  println("Args: " + args.mkString("[", ",", "]"))
  println("- - " * 20)

  val props = new Properties()
//  props.load(this.getClass.getClassLoader.getResourceAsStream(kafkaProducerPropsFile))
  props.load(new FileInputStream(kafkaProducerPropsFile))
  props.list(System.err)
  println("= = " * 20)

  val kafkaProducerConfig = new ProducerConfig(props)
  val kafkaProducer = new Producer[String, String](kafkaProducerConfig)


  // 构造消息模板
  /**
   * 被叫
   * 3,MSG_TIME,21278,1102,,8629490242767878,460005301567381,460005301567382,0,0,0,1,0,0,2
   * 主叫
   * 1,MSG_TIME,21278,1102,,8629490242767878,460005301567381,460005301567382,0,0,0,1,0,0,2
   * 漏电
   * 1,MSG_TIME,21278,1102,,8629490242767878,460005301567381,460005301567382,0,0,1,1,1,0,2
   * MSG_TIME 格式：
   */
  val msg_templates = Array[String](
    "3,MSG_TIME,21278,1102,,8629490242767878,460005301567381,460005301567382,0,0,0,1,0,0,2",
    "1,MSG_TIME,21278,1102,,8629490242767878,460005301567381,460005301567382,0,0,0,1,0,0,2",
    "1,MSG_TIME,21278,1102,,8629490242767878,460005301567381,460005301567382,0,0,1,1,1,0,2"
  )
  val random = new Random()
  def msg_template_id = random.nextInt(msg_templates.length)

  var batchId = 0
  var numMsgs = 0
  //循环中构造测试数据，发送到指定topic
  while(batchId < numBatches){

    val msgBuffer = ArrayBuffer[KeyedMessage[String, String]]()

    (1 to numMsgInBatch).foreach(i => {
      val msg_template = msg_templates(msg_template_id)
      val msg = msg_template.replace("MSG_TIME", DateFormatUtils.dateMs2Str(System.currentTimeMillis(), "yyyyMMdd HH:mm:ss.SSS"))
      println("batchId = " + batchId + ", msg_template_id = " + msg_template_id + ", msg = " + msg)

      val msgArr = msg.split(",")
      val eventID = msgArr(0).toInt
      val callingimsi = msgArr(6)
      val calledimsi = msgArr(7)
      val issmsalone = msgArr(14).toInt

      val key =
        if (List(3, 5, 7).contains(eventID)) {
          calledimsi
        } else if (List(8, 9, 10, 26).contains(eventID)) {
          if (issmsalone == 1) {
            callingimsi
          }
          else if (issmsalone == 2) {
            calledimsi
          }
          else {
            ""
          }
        } else {
          callingimsi
        }

      msgBuffer.append(new KeyedMessage[String, String](topic, key, msg))
    })
    numMsgs += msgBuffer.length
    println("msgBuffer.length = " + msgBuffer.length +", numMsgs = " + numMsgs)
    kafkaProducer.send(msgBuffer: _*)
    msgBuffer.clear()

    Thread.sleep(1 * 1000)
    batchId += 1
  }
}
