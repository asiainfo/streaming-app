package com.asiainfo.ocdc.streaming.tool

import java.io._
import java.net.URI
import java.util.Properties
import java.util.concurrent._

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.xml.XML

/**
 * Created by tsingfu on 15/4/22.
 */
object Kafka2Hdfs {

  val logger = LoggerFactory.getLogger("Kafka2Hdfs")

  def main(args: Array[String]): Unit ={
    /*

    //    val zookeeperConnect = "localhost:2181"
        val zookeeperConnect = "spark1:2181,spark2:2181"
        val groupId = "group2"
    //    val topic: String = "topic1-part1-repl1"
        val topic: String = "topic_stream1"
        val threadNum_consumer: Int = 3
        val queue_capacity: Int = 100000000
        val hadoop_output = "hdfs://spark1:9000/user/tsingfu/tmp/kafka2hdfs/tmp.txt"
        val batchLimit = 10
        val batchMaxMS = 30 * 1000
        val batchSleep = 5 * 1000
    */

    //    val confFile = args(0)
    val confFile = "kafkaTest/conf/kafka2hdfs-test.xml"
    val xml = XML.load(confFile)
    val zookeeperConnect = (xml \ "kafkaConsumer" \ "zookeeper.connect").text.trim

    val groupId = (xml \ "kafkaConsumer" \ "group.id").text.trim
    val threadNum_consumer = (xml \ "kafkaConsumer" \ "threadNum").text.trim.toInt
    val topic = (xml \ "kafkaConsumer" \ "topic").text.trim

    val buffer_capacity = (xml \ "buffer" \ "capacity").text.trim.toInt
    val hadoop_outputDir = (xml \ "hadoop" \ "outputDir").text.trim
    val hadoop_outputFilePrefix = (xml \ "hadoop" \ "outputFilePrefix").text.trim
    val batchLimit = (xml \ "hadoop" \ "batch.limit").text.trim.toInt
    val batchMaxMS = (xml \ "hadoop" \ "batch.max.ms").text.trim.toInt
    val bufferCheckSleep = (xml \ "hadoop" \ "buffer.check.sleep").text.trim.toInt

    val hadoopConf = new org.apache.hadoop.conf.Configuration()
    hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy","NEVER")
    hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable","true")

    //    var fs = FileSystem.get(URI.create(hadoop_outputDir), hadoopConf)

    val path = new Path(hadoop_outputFilePrefix)
    val hadoop_outputFile = hadoop_outputDir + "/" + hadoop_outputFilePrefix
    var fs = FileSystem.get(URI.create(hadoop_outputFile), hadoopConf)
    val fs2 = new ThreadLocal[FileSystem](){
      override def initialValue(): FileSystem ={
        FileSystem.get(URI.create(hadoop_outputFile), hadoopConf)
      }
    }

    if(!fs.exists(path)){
      fs.create(path,false)
      fs.close()
      fs = FileSystem.get(URI.create(hadoop_outputFile), hadoopConf)
    }

    val processor: MessageProcessor = new MessagePrinter
    val queues = new Array[BlockingQueue[String]](threadNum_consumer)
    val consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeperConnect, groupId))

    import scala.collection.JavaConversions.mapAsJavaMap
    val topicThreadMap: mutable.Map[String, Integer] = new mutable.HashMap[String, Integer]
    topicThreadMap.put(topic, threadNum_consumer)
    val kafkaStreamMap = consumerConnector.createMessageStreams(topicThreadMap)
    val streams = kafkaStreamMap.get(topic)

    val threadPool = Executors.newFixedThreadPool(threadNum_consumer * 2)
    var threadIdx = 0
    for (stream <- streams.toArray) {

      queues(threadIdx) = new LinkedBlockingQueue[String](buffer_capacity)
      val processor2: MessageProcessor = new MessageBuffer(queues(threadIdx))

      //      threadPool.execute(new ConsumerThread(stream.asInstanceOf[KafkaStream[Array[Byte],Array[Byte]]], processor))
      threadPool.execute(new ConsumerThread(stream.asInstanceOf[KafkaStream[Array[Byte],Array[Byte]]], queues(threadIdx), processor2))

      //TODO:
      //      val hadoop_outputFile = hadoop_outputDir + "/" + hadoop_outputFilePrefix +"_" + threadIdx

      threadPool.execute(new HdfsAppendThread(queues(threadIdx), hadoop_outputFile, fs2.get(), batchLimit, batchMaxMS, bufferCheckSleep))
      threadIdx += 1
    }

    threadPool.shutdown()
    threadPool.awaitTermination(1, TimeUnit.SECONDS)
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

class ConsumerThread(stream: KafkaStream[Array[Byte], Array[Byte]],
                     queue: BlockingQueue[String],
                     processor: MessageProcessor)
        extends Runnable {

  val logger = LoggerFactory.getLogger(classOf[ConsumerThread])
  def run() {
    val thread = Thread.currentThread()
    logger.info("[ConsumerThread] running thread " + thread )
    val it = stream.iterator()
    while (it.hasNext()) {
      val msg = new String(it.next().message())
      logger.debug("[ConsumerThread] " + thread + ", message = " + msg)
      processor.process(msg)
    }
    logger.warn("[ConsumerThread] [WARN] exit thread " + thread)
  }
}

trait MessageProcessor {
  val logger = LoggerFactory.getLogger(classOf[MessageProcessor])

  def process(str: String)
}

/**
 * MessageProcessor 简单实现类， 打印消息
 */
class MessagePrinter extends MessageProcessor{
  override def process(str: String){
    System.out.println(str)
  }
}

/**
 * MessageProcessor 实现类，写消息到执行 buffer
 * @param queue
 */
class MessageBuffer(queue: BlockingQueue[String]) extends MessageProcessor{
  override def process(str: String): Unit = {
    logger.debug("queue " + queue +" , add msg : " + str)
    queue.put(str)
  }
}

/**
 * 从指定 buffer 中读取消息，追加到 hdfs 文件
 * @param queue
 * @param hadoop_output
 * @param fs
 * @param batchLimit
 * @param batchMaxMS
 * @param bufferCheckSleep
 */
class HdfsAppendThread(queue: BlockingQueue[String],
                       hadoop_output: String,
                       fs: FileSystem,
                       batchLimit: Int, batchMaxMS: Int, bufferCheckSleep: Int)
        extends Runnable {

  val logger = LoggerFactory.getLogger(classOf[HdfsAppendThread])

  override def run() {
    val thread = Thread.currentThread()
    //    println("[HdfsAppendThread] running thread " + thread)
    logger.debug("[HdfsAppendThread] running thread " + thread)
    val sb: StringBuilder = new StringBuilder
    var startMS: Long = System.currentTimeMillis
    var elapsed: Long = 0L
    var batchNum: Int = 0
    while (true) {
      val line: String = queue.poll
      //      println("[HdfsAppendThread] line = " + line + ", batchNum = " + batchNum)
      logger.debug("[HdfsAppendThread] line = " + line + ", batchNum = " + batchNum)
      var firstFlag = true
      if (line != null) {
        //        if (!firstFlag) sb.append("\n")
        //        sb.append(line)
        sb.append("\n" + line)
        batchNum += 1

        firstFlag = false
        if (batchNum == batchLimit) {
          //          println("[HdfsAppendThread] ================== appending msg for reach batchLimit = " + batchLimit)
          logger.debug("[HdfsAppendThread] appending msg for reach batchLimit = " + batchLimit)
          appendStringToHdfs(sb.toString(), hadoop_output, fs)
          sb.setLength(0)
          firstFlag = true
          batchNum = 0
          startMS = System.currentTimeMillis()
        }
      } else {
        //        println("[HdfsAppendThread] [debug] line = " + line + ", batchNum = " + batchNum + ", sleep "+ bufferCheckSleep +" s")
        logger.debug("[HdfsAppendThread] [debug] line = " + line + ", batchNum = " + batchNum
                + ", sleep "+ bufferCheckSleep +" s")
        try {
          Thread.sleep(bufferCheckSleep)
        }
        catch {
          case e: InterruptedException =>
            e.printStackTrace()
        }
        elapsed = System.currentTimeMillis - startMS

        //        println("[HdfsAppendThread] [debug] elapsed = " + elapsed)
        logger.debug("[HdfsAppendThread] [debug] elapsed = " + elapsed)

        if (sb.length > 0 && elapsed > batchMaxMS) {
          //          println("[HdfsAppendThread] appending msg for reach batchMaxMS = " + batchMaxMS)
          logger.debug("[HdfsAppendThread] appending msg for reach batchMaxMS = " + batchMaxMS)
          appendStringToHdfs(sb.toString(), hadoop_output, fs)
          sb.setLength(0)
          firstFlag = true
          batchNum = 0
          startMS = System.currentTimeMillis()
        }
      }
    }
  }

  /**
   * 追加 str 到 指定 hdfs 文件
   * @param str
   * @param filename
   * @param fs
   */
  private def appendStringToHdfs(str: String, filename: String, fs: FileSystem) {
    try {
      val in: InputStream = new StringBufferInputStream(str)
      val out: OutputStream = fs.append(new Path(filename))
      org.apache.hadoop.io.IOUtils.copyBytes(in, out, 4096, true)
    }
    catch {
      case e: IOException =>
        if(fs != null) fs.close()
        e.printStackTrace()
    }
  }
}