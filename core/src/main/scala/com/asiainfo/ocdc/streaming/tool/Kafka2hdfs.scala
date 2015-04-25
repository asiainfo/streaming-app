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

  def main(args: Array[String]): Unit = {

    //1 获取配置
    val confFile = args(0)
    //    val confFile = "kafkaTest/conf/kafka2hdfs-test.xml"
    val xml = XML.load(confFile)
    val zookeeperConnect = (xml \ "kafkaConsumer" \ "zookeeper.connect").text.trim

    val groupId = (xml \ "kafkaConsumer" \ "group.id").text.trim
    val threadNum_consumer = (xml \ "kafkaConsumer" \ "threadNum").text.trim.toInt
    val topic = (xml \ "kafkaConsumer" \ "topic").text.trim

    val buffer_capacity = (xml \ "buffer" \ "capacity").text.trim.toInt
    val hadoop_outputDir = (xml \ "hadoop" \ "outputDir").text.trim
    val hadoop_prefix = (xml \ "hadoop" \ "prefix").text.trim
    val hadoop_parallelFlag = (xml \ "hadoop" \ "parallel").text.trim
    val batchLimit = (xml \ "hadoop" \ "batch.limit").text.trim.toInt
    val batchMaxMS = (xml \ "hadoop" \ "batch.max.ms").text.trim.toInt
    val bufferCheckSleepMS = (xml \ "hadoop" \ "buffer.check.sleep.ms").text.trim.toInt

    // 2 获取访问 hdfs 的 conf, fs, path
    val hadoopConf = new org.apache.hadoop.conf.Configuration()

    val path = new Path(hadoop_prefix)
    val hadoop_outputWithPrefix = hadoop_outputDir + "/" + hadoop_prefix

    val fs = FileSystem.get(URI.create(hadoop_outputDir), hadoopConf)

    // 3 初始化 buffer，存储从 kafka 读取的消息
    val queues = new Array[BlockingQueue[String]](threadNum_consumer)

    // 4 获取访问 kafka 的 connector, 多线程的 streams
    val consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeperConnect, groupId))
    import scala.collection.JavaConversions.mapAsJavaMap
    val topicThreadMap: mutable.Map[String, Integer] = new mutable.HashMap[String, Integer]
    topicThreadMap.put(topic, threadNum_consumer)
    val kafkaStreamMap = consumerConnector.createMessageStreams(topicThreadMap)
    val streams = kafkaStreamMap.get(topic)

    // 5 初始化线程池，启动 kafka 读线程， hdfs append 追加线程
    val threadPool = if (hadoop_parallelFlag == "true") {
      Executors.newFixedThreadPool(threadNum_consumer * 2)
    } else {
      Executors.newFixedThreadPool(threadNum_consumer + 1)
    }

    // 5.1 启动 kafka 读线程
    var threadIdx = 0
    for (stream <- streams.toArray) {
      queues(threadIdx) = new LinkedBlockingQueue[String](buffer_capacity)
      //      val processor: MessageProcessor = new MessagePrinter
      val processor: MessageProcessor = new MessageBuffer(queues(threadIdx), threadIdx)
      threadPool.execute(new ConsumerThread(stream.asInstanceOf[KafkaStream[Array[Byte], Array[Byte]]], queues(threadIdx), processor, threadIdx))
      threadIdx += 1
    }

    // 5.2 启动 kakfa 写线程
    if (hadoop_parallelFlag == "true") {
      logger.info("Using multi HdfsAppend threads to append messages(from multi kafka buffer queues) into multi hdfs files")
      for (i <- 0 until threadNum_consumer) {
        val hadoop_output = hadoop_outputWithPrefix + "-part" + i
        createHadoopFileIfNonExist(fs, hadoop_output, dirFlag = false)
        logger.debug("Starting HdfsAppend thread to write messages from buffer queue No. "+ i +" to hadoop_output " + hadoop_output)
        threadPool.execute(new HdfsAppendInMultiThread(queues(i), hadoop_output, batchLimit, batchMaxMS, bufferCheckSleepMS))
      }

    } else {
      logger.info("Using single HdfsAppend thread to append messages (from multi kafka buffer queues) into single hdfs file " + hadoop_outputWithPrefix)
      createHadoopFileIfNonExist(fs, hadoop_outputWithPrefix, dirFlag = false)
      threadPool.execute(new HdfsAppendInSingleThread(queues, hadoop_outputWithPrefix, batchLimit, batchMaxMS, bufferCheckSleepMS))
    }
    fs.close()

    threadPool.shutdown()
    threadPool.awaitTermination(1, TimeUnit.SECONDS)
  }

  def createHadoopFileIfNonExist(fs: FileSystem, hadoopPath: String, dirFlag: Boolean): Unit = {
    val path = new Path(hadoopPath)

    if (!fs.exists(path)) {
      val parent = path.getParent
      if (!fs.exists(parent)) {
        createHadoopFileIfNonExist(fs, parent.getName, dirFlag = true)
      }

      if (dirFlag) {
        fs.mkdirs(path)
        logger.info("dir " + hadoopPath + " created")
      } else {
        fs.create(path, false)
        logger.info("file " + hadoopPath + " created")
      }

    } else {
      if (dirFlag) {
        logger.warn("dir " + hadoopPath + " exists already")
      } else {
        logger.warn("file " + hadoopPath + " exists already")
      }
    }
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
                     processor: MessageProcessor,
                     queueId: Int)
        extends Runnable {

  val logger = LoggerFactory.getLogger(classOf[ConsumerThread])

  def run() {
    val thread = Thread.currentThread()
    logger.info("Running thread " + thread +" to buffer msgs in queue Id = " + queueId)
    val it = stream.iterator()
    while (it.hasNext()) {
      val msg = new String(it.next().message())
      //      logger.debug("msg = " + msg)
      processor.process(msg)
    }
    logger.warn("Exit thread " + thread)
  }
}

trait MessageProcessor {
  val logger = LoggerFactory.getLogger(classOf[MessageProcessor])

  def process(str: String)
}

/**
 * MessageProcessor 简单实现类， 打印消息
 */
class MessagePrinter extends MessageProcessor {
  override def process(str: String) {
    System.out.println(str)
  }
}

/**
 * MessageProcessor 实现类，写消息到执行 buffer
 * @param queue
 */
class MessageBuffer(queue: BlockingQueue[String], queueId: Int) extends MessageProcessor {
  override def process(str: String): Unit = {
    logger.debug("buffer queue id = " + queueId + ", add msg = " + str)
    queue.put(str)
  }
}

/**
 * 从指定 buffer 中读取消息，多线程，追加到 多个 hdfs 文件
 * @param queue
 * @param hadoop_output
 * @param batchLimit
 * @param batchMaxMS
 * @param bufferCheckSleepMS
 */
class HdfsAppendInMultiThread(queue: BlockingQueue[String],
                              hadoop_output: String,
                              batchLimit: Int, batchMaxMS: Int, bufferCheckSleepMS: Int)
        extends Runnable {

  val logger = LoggerFactory.getLogger(classOf[HdfsAppendInMultiThread])

  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")

  val threadLocalFs = new ThreadLocal[FileSystem]() {
    override def initialValue(): FileSystem = {
      FileSystem.get(URI.create(hadoop_output), hadoopConf)
    }
  }

  def fs: FileSystem = if (threadLocalFs.get() == null) {
    threadLocalFs.set(FileSystem.get(URI.create(hadoop_output), hadoopConf)).asInstanceOf[FileSystem]
  } else {
    threadLocalFs.get()
  }

  override def run() {
    val thread = Thread.currentThread()
    logger.debug("Running thread " + thread)
    val sb: StringBuilder = new StringBuilder
    var startMS: Long = System.currentTimeMillis
    var elapsed: Long = 0L
    var batchNum: Int = 0
    while (true) {
      val line: String = queue.poll
      logger.debug("HdfsAppend got line from buffer queue " + queue.getClass +", line = " + line + ", batchNum = " + batchNum)
      if (line != null) {
        sb.append("\n" + line)
        batchNum += 1

        if (batchNum == batchLimit) {
          logger.info("HdfsAppend appending msg for reach batchLimit = " + batchLimit)
          appendStringToHdfs(sb.toString(), hadoop_output, fs)
          sb.setLength(0)
          batchNum = 0
          startMS = System.currentTimeMillis()
        }
        elapsed = System.currentTimeMillis() - startMS
      } else {
        // line == null
        logger.info("HdfsAppend sleeps " + bufferCheckSleepMS + " for got line = " + line + " and batchNum = " + batchNum)
        Thread.sleep(bufferCheckSleepMS)
        elapsed = System.currentTimeMillis - startMS

        if (sb.length > 0 && elapsed > batchMaxMS) {
          logger.info("HdfsAppend appending msg for reach batchMaxMS = " + batchMaxMS +", batchNum = " + batchNum)
          appendStringToHdfs(sb.toString(), hadoop_output, fs)
          sb.setLength(0)
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

    val in: InputStream = new StringBufferInputStream(str)
    val out: OutputStream = fs.append(new Path(filename))
    org.apache.hadoop.io.IOUtils.copyBytes(in, out, 4096, true)
    out.close()
  }
}

class HdfsAppendInSingleThread(queues: Array[BlockingQueue[String]],
                               hadoop_output: String,
                               batchLimit: Int, batchMaxMS: Int, bufferCheckSleepMS: Int)
        extends Runnable {

  val logger = LoggerFactory.getLogger(classOf[HdfsAppendInMultiThread])

  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")

  val threadLocalFs = new ThreadLocal[FileSystem]() {
    override def initialValue(): FileSystem = {
      FileSystem.get(URI.create(hadoop_output), hadoopConf)
    }
  }

  override def run() {
    val thread = Thread.currentThread()
    logger.debug("Running HdfsAppend Single thread " + thread)
    val sb: StringBuilder = new StringBuilder
    var startMS: Long = System.currentTimeMillis
    var elapsed: Long = 0L
    var batchNum: Int = 0
    val queueSize = queues.length
    var queueIdx = 0

    while (true) {
      var nextQueueFlag = "false"
      logger.info("HdfsAppend scan queue No. " + queueIdx + " of " + queueSize)

      val queue = queues(queueIdx)
      while (nextQueueFlag == "false") {
        val line: String = queue.poll
        logger.debug("HdfsAppend thread got line = " + line + ", batchNum = " + batchNum)

        if (line != null) {
          sb.append("\n" + line)
          batchNum += 1

          if (batchNum == batchLimit) {
            logger.info("HdfsAppend thread appending msgs for reach batchLimit = " + batchLimit)
            appendStringToHdfs(sb.toString(), hadoop_output, threadLocalFs.get())
            sb.setLength(0)
            batchNum = 0
            startMS = System.currentTimeMillis()

          }
          elapsed = System.currentTimeMillis() - startMS

        } else {
          // line == null
          val queueCheckSleep = bufferCheckSleepMS / queueSize
          logger.info("HdfsAppend sleeps " + queueCheckSleep + " ms for got line = null from queue No. " + queueIdx + " of " + queueSize + ", switch to next buffer queue No. " + (queueIdx + 1))
          nextQueueFlag = "true"
          Thread.sleep(queueCheckSleep)
          elapsed = System.currentTimeMillis - startMS

          queueIdx += 1
          if (queueIdx == queueSize) queueIdx = 0

          logger.debug("HdfsAppend thread does not append msg for " + elapsed + " ms")
        }

        //超过指定时间，如果有数据，执行一次 append
        if (sb.length > 0 && elapsed > batchMaxMS) {
          logger.info("HdfsAppend thread appending msg for reach batchMaxMS = " + batchMaxMS + ", batchNum = " + batchNum)
          appendStringToHdfs(sb.toString(), hadoop_output, threadLocalFs.get())
          sb.setLength(0)
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
    val in = new StringBufferInputStream(str)
    val out = fs.append(new Path(filename))
    org.apache.hadoop.io.IOUtils.copyBytes(in, out, 4096, true)
    out.close()
  }
}