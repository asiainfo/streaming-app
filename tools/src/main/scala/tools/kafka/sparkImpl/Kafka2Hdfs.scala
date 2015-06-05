package tools.kafka.sparkImpl

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import kafka.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

import scala.xml.XML

/**
 * Created by tsingfu on 15/5/19.
 */
object Kafka2Hdfs {

  val logger = LoggerFactory.getLogger(this.getClass)
  val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

  def main(args: Array[String]): Unit ={

    //1 获取配置
    val confXmlFile = args(0)

    //    val confXmlFile = "kafkaTest/conf/kafka2hdfs-test.conf"
    val props = init_props_fromXml(confXmlFile)

    val topics = props.getProperty("topics")
    val zookeeperConnect = props.getProperty("zookeeper.connect")
    val groupId = props.getProperty("group.id")
    val numThreadsConsumer = props.getProperty("num.threads.kafkaConsumer").toInt

    val batchInterval = props.getProperty("streaming.batchDurationSec", "60").toLong
    val blockInterval = props.getProperty("streaming.blockInterval", "200").toLong

    val hadoopOutputDir = props.getProperty("hadoop.output.dir")
    val hadoopOutputPrefix = props.getProperty("hadoop.output.prefix", "")
    val hadoopOutputSuffix = props.getProperty("hadoop.output.suffix", "")

    //2 获取 ssc
    val sparkConf = new SparkConf().setAppName(this.getClass.getSimpleName)
    sparkConf.set("spark.streaming.blockInterval", blockInterval.toString)

    val ssc = new StreamingContext(sparkConf, Seconds(batchInterval))

    //3 从 kafka 读取消息
    val topicThreadMap = topics.split(",").map((_,numThreadsConsumer.toInt)).toMap
    val dStream = KafkaUtils.createStream(ssc, zookeeperConnect, groupId, topicThreadMap).map(_._2)


//    dStream.saveAsTextFiles(hadoopOutputDir)
    dStream.foreachRDD((rdd, time)=>{
//      println("#" * 60 + "time=" + time.toString() +", sdf.format=" + sdf.format(new Date(time.milliseconds)) + ", now=" + sdf.format(new Date()))
      logger.debug("time=" + time.toString() + ", sdf.format=" + sdf.format(new Date(time.milliseconds)) + ", now=" + sdf.format(new Date()))
      val hadoopOutput = hadoopOutputDir +"/"+ hadoopOutputPrefix + sdf.format(new Date(time.milliseconds)) + hadoopOutputSuffix
      rdd.saveAsTextFile(hadoopOutput)
    })

    ssc.start()
    ssc.awaitTermination()
  }

  def init_props_fromXml(confXmlFile: String): Properties ={

    val conf = XML.load(confXmlFile)
    val topics = (conf \ "kafkaConsumer" \ "topics").text.trim

    val zookeeperConnect = (conf \ "kafkaConsumer" \ "zookeeper.connect").text.trim
    val groupId = (conf \ "kafkaConsumer" \ "group.id").text.trim
    val numThreads_consumer = (conf \ "kafkaConsumer" \ "numThreads").text.trim.toInt

    val batchDuration = (conf \ "streaming.batchDurationSec").text.trim.toLong
    val streamingBlockInterval = (conf \ "streaming.blockInterval.ms").text.trim.toLong
    val hadoopOutputDir = (conf \ "hadoop.output.dir").text.trim
    val hadoopOutputPrefix = (conf \ "hadoop.output.prefix").text.trim
    val hadoopOutputSuffix = (conf \ "hadoop.output.suffix").text.trim

    val props = new Properties()
    props.put("topics", topics)
    props.put("zookeeper.connect", zookeeperConnect)
    props.put("group.id", groupId)
    props.put("num.threads.kafkaConsumer", numThreads_consumer.toString)

    props.put("streaming.batchDurationSec", batchDuration.toString)
    props.put("streaming.blockInterval", streamingBlockInterval.toString)

    props.put("hadoop.output.dir", hadoopOutputDir)
    props.put("hadoop.output.prefix", hadoopOutputPrefix)
    props.put("hadoop.output.suffix", hadoopOutputSuffix)

    for (k<-props.keySet().toArray) {
      println(k + " => " + props.get(k))
    }

    props
  }

  def createConsumerConfig(zookeeperConnect: String, groupId: String): ConsumerConfig = {
    val props = new Properties
    props.put("zookeeper.connect", zookeeperConnect)
    props.put("group.id", groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    new ConsumerConfig(props)
  }

}
