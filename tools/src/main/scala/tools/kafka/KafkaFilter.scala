package tools.kafka

import java.text.SimpleDateFormat
import java.util.{TimerTask, Timer, Date}

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory

/**
 * Created by tsingfu on 15/7/12.
 */
object KafkaFilter {

  val logger = LoggerFactory.getLogger(this.getClass)
  val sdf = new SimpleDateFormat("yyyyMMddHHmmss")

  def main(args: Array[String]) {
    val usage = s"""
                   |Usage:
                   | KafkaFilter <batchDurationSec> <filterMsg> <hdfsPath> <topics> high <zkQuorum>  <numThreads> <groupId>
                   | KafkaFilter <batchDurationSec> <filterMsg> <hdfsPath> <topics> low <brokers>
                   |  <brokers> is a list of one or more Kafka brokers
                   |  <topics> is a list of one or more kafka topics to consume from
                   |
        """.stripMargin

    if (args.length < 6) {
      System.err.println(usage)
      System.exit(1)
    } else {
      val kafkaApiType = args(4).trim.toLowerCase
      if (kafkaApiType != "high" && kafkaApiType != "low"){
        System.err.println(usage)
        System.exit(1)
      }
    }

    println("= = " * 20)
    println(args.mkString("[", " # ", "]"))
    println("= = " * 20)

    val batchDurationSec = args(0).toInt
    val filterMsg = args(1)
    val hadoopOutputDir = args(2)
    val topics = args(3)
    val kafkaApiType = args(4).trim.toLowerCase

    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaFilter")
    val ssc = new StreamingContext(sparkConf, Seconds(batchDurationSec))

    val lines = kafkaApiType match {
      case "high" =>
        val zkQuorum = args(5)
        val numThreads = args(6)
        val group = args(7)

        val topicMap = topics.split(",").map((_, numThreads.toInt)).toMap
        val messages = KafkaUtils.createStream(ssc, zkQuorum, group, topicMap)
        messages.map(_._2)
      case "low" =>

        val brokers = args(5)

        // Create direct kafka stream with brokers and topics
        val topicsSet = topics.split(",").toSet
        val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)
        val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
          ssc, kafkaParams, topicsSet)
        messages.map(_._2)
    }

    val numMsgs = ssc.sparkContext.accumulator(0, "filtered numMsgs")

    val timer = new Timer()
    val timerTask = new TimerTask {
      override def run(): Unit = {
        println("= * = " * 10 + sdf.format(new Date(System.currentTimeMillis())) + " D numMsgs = " + numMsgs)
      }
    }
    timer.schedule(timerTask, 60000, 60000)

    // Get the lines, split them into words, count the words and print
    lines.foreachRDD((rdd, time)=>{
      logger.debug("time=" + time.toString() + ", sdf.format=" + sdf.format(new Date(time.milliseconds)) + ", now=" + sdf.format(new Date()))
      val hadoopOutput = hadoopOutputDir +"/"+ sdf.format(new Date(time.milliseconds))
      rdd.filter(line=>{
        logger.debug("= = " * 10 + "E line = "+line)


        println("= = " * 20 + "E numMsgs = " + numMsgs)
        if(line.contains(filterMsg)){
          println("= = " * 20 + " add 1")
          numMsgs.add(1)
          true
        }else {
          false
        }
      }).saveAsTextFile(hadoopOutput)
    })

    // Start the computation
    ssc.start()
    ssc.awaitTermination()
    timer.cancel()
  }
}
