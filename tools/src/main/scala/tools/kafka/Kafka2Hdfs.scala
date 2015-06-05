package tools.kafka

import java.io._
import java.net.URI
import java.util.Properties
import java.util.concurrent._

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.xml.XML

/**
 * Created by tsingfu on 15/4/22.
 */
object Kafka2Hdfs {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit = {

    //1 获取配置
    val confFile = args(0)
    //    val confFile = "kafkaTest/conf/kafka2hdfs2-test.conf"
    val props = init_props_fromXml(confFile)

    val topics = props.getProperty("topics")
    val topicArray = props.getProperty("topics").split(",").map(_.trim)
    val numsThreads_producer = topicArray.length

    val zookeeperConnect = props.getProperty("zookeeper.connect")
    val groupId = props.getProperty("group.id")
    val numThreadsConsumer = props.getProperty("num.threads.kafkaConsumer").toInt

    val bufferMultiFlag = props.getProperty("buffer.multi.flag")
    val bufferCapacity = props.getProperty("buffer.capacity").toInt
    val bufferSwitchThreshold = props.getProperty("buffer.switch.threshold").toInt
    val bufferCheckSleepMs = props.getProperty("buffer.check.sleep.ms").toInt

    val hadoopOutputDir = props.getProperty("hadoop.output.dir")
    val hadoopPrefix = props.getProperty("hadoop.prefix")
    val hadoopOutputMultiFlag = props.getProperty("hadoop.output.multi.flag")
    val batchLimit = props.getProperty("hadoop.batch.limit").toInt
    val batchMaxMs = props.getProperty("hadoop.batch.max.ms").toInt

    // 2 获取访问 hdfs 的 conf, fs, path
    val hadoopConf = new org.apache.hadoop.conf.Configuration()

    hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
    hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")

    val fs = FileSystem.get(URI.create(hadoopOutputDir), hadoopConf)
    val path = new Path(hadoopPrefix)

    val hadoop_outputWithPrefix = hadoopOutputDir + "/" + hadoopPrefix

    // 3 初始化 buffer，存储从 kafka 读取的消息
    // TODO: 暂时只支持2种情况，情况1：1个 kafkaConsumer 写 1个 buffer，情况2：多个kafkaConsumer写1个 buffer
    val buffers = if (bufferMultiFlag == "true") {
      new Array[BlockingQueue[String]](numsThreads_producer)
    } else {
      new Array[BlockingQueue[String]](1)
    }

    for (bufferIdx <- 0 until buffers.length) {
      buffers(bufferIdx) = new ArrayBlockingQueue[String](bufferCapacity)
    }

    // 4 初始化线程池，启动 kafka consumer 读线程， hdfs append 追加线程
    // TODO: 暂时只支持2种情况，情况1：1个hdfsAppend 写1个buffer，情况2：1个 hdfsAppend写多个buffer
    val numThreads_hdfsAppend =
      if (bufferMultiFlag == "true" && hadoopOutputMultiFlag == "true") {
        numsThreads_producer
      } else 1
    val numThreadsInThreadPool = numsThreads_producer * numThreadsConsumer + numThreads_hdfsAppend
    logger.info("newFixedThreadPool size = " + numThreadsInThreadPool +" with numsThreads_producer = "
            + numsThreads_producer +", numThreadsConsumer = " + numThreadsConsumer +", numThreads_hdfsAppend = " + numThreads_hdfsAppend)
    val threadPool = Executors.newFixedThreadPool(numsThreads_producer * numThreadsConsumer + numThreads_hdfsAppend)


    // 5.1 启动 hhdfsAppend 写线程
    if (bufferMultiFlag == "true" && hadoopOutputMultiFlag == "true") {
      logger.info("Using multi HdfsAppend threads to append messages(from multi kafka buffer buffers) into multi hdfs files")
      for (threadId <- 0 until numThreads_hdfsAppend) {
        val bufferId = threadId
        val hadoop_output = hadoop_outputWithPrefix + "-part" + threadId
        createHadoopFileIfNonExist(hadoopConf, hadoop_output, dirFlag = false)
        logger.debug("Starting HdfsAppend thread to append messages from buffer with bufferId = " + threadId + " to hadoop_output " + hadoop_output)
        threadPool.execute(new Buffer2HdfsThread(buffers(bufferId), threadId, hadoop_output, batchLimit, batchMaxMs, bufferCheckSleepMs))
      }
    } else {
      logger.info("Using single HdfsAppend thread to append messages into single hdfs output " + hadoop_outputWithPrefix)
      createHadoopFileIfNonExist(hadoopConf, hadoop_outputWithPrefix, dirFlag = false)
      threadPool.execute(new Buffers2HdfsThread(buffers, hadoop_outputWithPrefix, batchLimit, batchMaxMs, bufferCheckSleepMs, bufferSwitchThreshold))
    }

    // 5.2 获取访问 kafka 的 consumerConnector, 多线程访问 多个 topic，多线程访问每个 topic的多个分区
    val consumerConnector = Consumer.createJavaConsumerConnector(createConsumerConfig(zookeeperConnect, groupId))
    import scala.collection.JavaConversions.mapAsJavaMap
    val topicThreadMap: mutable.Map[String, Integer] = new mutable.HashMap[String, Integer]
    for (producerId <- 0 until numsThreads_producer){
      val bufferId = producerId
      val topic = topicArray(producerId)

      topicThreadMap.put(topic, numThreadsConsumer)
      val kafkaStreamMap = consumerConnector.createMessageStreams(topicThreadMap)
      val streams = kafkaStreamMap.get(topic)

      // 5.3 启动 kafka consumer 读线程, 写到指定 BlockingQueue
      for (streamWithIdx <- streams.toArray.zipWithIndex) {
        val (stream, partThreadId) = streamWithIdx
        //      val processor: MessageProcessor = new MessagePrinter
        val processor: MessageProcessor = new MessageBuffer(buffers(bufferId), bufferId)
        threadPool.execute(new ConsumerThread(stream.asInstanceOf[KafkaStream[Array[Byte], Array[Byte]]], buffers(bufferId), processor, bufferId))
      }
    }

    /*
        // 5.3 启动 hhdfsAppend 写线程
        if (bufferMultiFlag == "true" && hadoopOutputMultiFlag == "true") {
          logger.info("Using multi HdfsAppend threads to append messages(from multi kafka buffer buffers) into multi hdfs files")
          for (threadId <- 0 until numThreads_hdfsAppend) {
            val bufferId = threadId
            val hadoop_output = hadoop_outputWithPrefix + "-part" + threadId
            createHadoopFileIfNonExist(fs, hadoop_output, dirFlag = false)
            logger.debug("Starting HdfsAppend thread to append messages from buffer with bufferId = " + threadId + " to hadoop_output " + hadoop_output)
            threadPool.execute(new Buffer2HdfsThread(buffers(bufferId), threadId, hadoop_output, batchLimit, batchMaxMs, bufferCheckSleepMs))
          }
        } else {
          logger.info("Using single HdfsAppend thread to append messages into single hdfs output " + hadoop_outputWithPrefix)
          createHadoopFileIfNonExist(fs, hadoop_outputWithPrefix, dirFlag = false)
          threadPool.execute(new Buffers2HdfsThread(buffers, hadoop_outputWithPrefix, batchLimit, batchMaxMs, bufferCheckSleepMs, bufferSwitchThreshold))
        }
    */

    fs.close()

    threadPool.awaitTermination(2, TimeUnit.SECONDS)
    threadPool.shutdown()
  }

  def init_props(confPropFile: String): Properties ={
    val props = new Properties()
    props.load(new FileInputStream(confPropFile))
    props
  }

  def init_props_fromXml(confXmlFile: String): Properties ={

    val conf = XML.load(confXmlFile)
    val topics = (conf \ "kafkaConsumer" \ "topics").text.trim

    val zookeeperConnect = (conf \ "kafkaConsumer" \ "zookeeper.connect").text.trim
    val groupId = (conf \ "kafkaConsumer" \ "group.id").text.trim
    val numThreads_consumer = (conf \ "kafkaConsumer" \ "threadNum").text.trim.toInt

    val bufferMultiFlag = (conf \ "buffer" \ "multi.flag").text.trim
    val bufferCapacity = (conf \ "buffer" \ "capacity").text.trim.toInt
    val bufferSwitchThreshold = (conf \ "buffer" \ "switch.threshold").text.trim.toInt
    val bufferCheckSleepMs = (conf \ "buffer" \ "check.sleep.ms").text.trim.toInt

    val hadoopOutputDir = (conf \ "hadoop" \ "outputDir").text.trim
    val hadoopPrefix = (conf \ "hadoop" \ "prefix").text.trim
    val hadoopOutputMultiFlag = (conf \ "hadoop" \ "outputMultiFlag").text.trim
    val batchLimit = (conf \ "hadoop" \ "batch.limit").text.trim.toInt
    val batchMaxMs = (conf \ "hadoop" \ "batch.max.ms").text.trim.toInt


    val props = new Properties()
    props.put("topics", topics)
    props.put("zookeeper.connect", zookeeperConnect)
    props.put("group.id", groupId)
    props.put("num.threads.kafkaConsumer", numThreads_consumer.toString)

    props.put("buffer.multi.flag", bufferMultiFlag)
    props.put("buffer.capacity", bufferCapacity.toString)
    props.put("buffer.check.sleep.ms", bufferCheckSleepMs.toString)
    props.put("buffer.switch.threshold", bufferSwitchThreshold.toString)

    props.put("hadoop.output.dir", hadoopOutputDir)
    props.put("hadoop.output.multi.flag", hadoopOutputMultiFlag)
    props.put("hadoop.prefix", hadoopPrefix)
    props.put("hadoop.batch.limit", batchLimit.toString)
    props.put("hadoop.batch.max.ms", batchMaxMs.toString)

    for (k<-props.keySet().toArray) {
      println(k + " => " + props.get(k))
    }

    props
  }

  def createHadoopFileIfNonExist(hadoopConf: Configuration, hadoopPath: String, dirFlag: Boolean): Unit = {
//    val hadoopConf = new Configuration()
    val path = new Path(hadoopPath)
    val fs = path.getFileSystem(hadoopConf)

    if (!fs.exists(path)) {
      val parent = path.getParent
      if (!fs.exists(parent)) {
        createHadoopFileIfNonExist(hadoopConf, parent.getName, dirFlag = true)
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

    fs.close()
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
                     buffer: BlockingQueue[String],
                     processor: MessageProcessor,
                     bufferId: Int)
        extends Runnable {

  val logger = LoggerFactory.getLogger(classOf[ConsumerThread])

  def run() {
    val thread = Thread.currentThread()
    logger.info("Running thread " + thread + ", buffer msgs to queue with bufferId = " + bufferId)
    val it = stream.iterator()
    while (it.hasNext()) {
      val msg = new String(it.next().message())
      logger.debug("Recieved msg from kafkaStream : " + msg)
      processor.process(msg)
    }
    logger.warn("Exit thread " + thread +" with bufferId = " + bufferId)
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
 * @param buffer
 */
class MessageBuffer(buffer: BlockingQueue[String], bufferId: Int) extends MessageProcessor {
  override def process(str: String): Unit = {
    logger.debug("buffer with bufferId = " + bufferId + ", add msg = " + str)
    buffer.put(str)
  }
}

/**
 * 从指定 blockingQueue 中读取消息，追加到指定 hdfs 文件
 * @param buffer
 * @param hadoop_output
 * @param batchLimit
 * @param batchMaxMS
 * @param bufferCheckSleepMS
 */
class Buffer2HdfsThread(buffer: BlockingQueue[String],
                        bufferId: Int,
                        hadoop_output: String,
                        batchLimit: Int, batchMaxMS: Int, bufferCheckSleepMS: Int)
        extends Runnable {

  val logger = LoggerFactory.getLogger(classOf[Buffer2HdfsThread])

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
    val startMs = System.currentTimeMillis

    val sb: StringBuilder = new StringBuilder
    var lastHdfsAppendMs: Long = System.currentTimeMillis
    var lastHdfsUnAppendMs: Long = 0L
    var numMsgsInBatch: Int = 0

    while (true) {
      logger.debug("HdfsAppend scan msgs from buffer with bufferId = " + bufferId + ", numMsgsInBatch = " + numMsgsInBatch)

      val line: String = buffer.poll

      if (line != null) {
        sb.append("\n" + line)
        numMsgsInBatch += 1

        if (numMsgsInBatch == batchLimit) {
          logger.info("HdfsAppend appends msg for reach batchLimit = " + batchLimit+", numMsgsInBatch="+numMsgsInBatch)
          appendStringToHdfs(sb.toString(), hadoop_output, fs)
          sb.setLength(0)
          numMsgsInBatch = 0
          lastHdfsAppendMs = System.currentTimeMillis()
        }
        lastHdfsUnAppendMs = System.currentTimeMillis() - lastHdfsAppendMs
      } else {
        // line == null
        logger.info("HdfsAppend sleeps " + bufferCheckSleepMS + " ms for got line = " + line + " ,numMsgsInBatch = " + numMsgsInBatch)
        Thread.sleep(bufferCheckSleepMS)
        lastHdfsUnAppendMs = System.currentTimeMillis - lastHdfsAppendMs

        if (sb.length > 0 && lastHdfsUnAppendMs > batchMaxMS) {
          logger.info("HdfsAppend appending msg for reach batchMaxMS = " + batchMaxMS + ", numMsgsInBatch = " + numMsgsInBatch)
          appendStringToHdfs(sb.toString(), hadoop_output, fs)
          sb.setLength(0)
          numMsgsInBatch = 0
          lastHdfsAppendMs = System.currentTimeMillis()
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

class Buffers2HdfsThread(buffers: Array[BlockingQueue[String]],
                         hadoop_output: String,
                         batchLimit: Int, batchMaxMs: Int, bufferCheckSleepMs: Int, bufferSwitchThreshold: Int)
        extends Runnable {

  val logger = LoggerFactory.getLogger(classOf[Buffers2HdfsThread])

  val hadoopConf = new org.apache.hadoop.conf.Configuration()
  hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER")
  hadoopConf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true")

  val threadLocalFs = new ThreadLocal[FileSystem]() {
    override def initialValue(): FileSystem = {
      FileSystem.get(URI.create(hadoop_output), hadoopConf)
    }
  }

  val queueSize = buffers.length
  val bufferCheckSleepMsPerBuffer = bufferCheckSleepMs / queueSize

  def nextBufferIdx(curIdx: Int, numBuffers:Int) :Int = {
    val nextIdx = curIdx + 1
    if (nextIdx == numBuffers) 0 else nextIdx
  }

  override def run() {
    val thread = Thread.currentThread()
    logger.debug("Running HdfsAppend Single thread " + thread)

    val sb: StringBuilder = new StringBuilder
    var lastHdfsAppendMs: Long = System.currentTimeMillis
    var lastHdfsUnAppendMs: Long = 0L

    var numMsgsInBatch: Int = 0
    var numBatchs: Int =0

    var queueIdx = 0

    while (true) {
      var bufferSwitchFlag = "false"
      logger.info("HdfsAppend scans buffer with bufferIdx " + queueIdx + " of " + queueSize)

      val buffer = buffers(queueIdx)
      while (bufferSwitchFlag == "false") {
        //        val line: String = buffer.poll
        val line: String = buffer.poll(bufferCheckSleepMsPerBuffer, TimeUnit.MILLISECONDS)

        logger.debug("HdfsAppend thread got line = " + line + ", numMsgsInBatch = " + numMsgsInBatch)

        if (line != null) {
          sb.append("\n" + line)
          numMsgsInBatch += 1

          if (numMsgsInBatch == batchLimit) {
            logger.info("HdfsAppend thread appending msgs for reach batchLimit = " + batchLimit)
            appendStringToHdfs(sb.toString(), hadoop_output, threadLocalFs.get())
            sb.setLength(0)
            numMsgsInBatch = 0

            lastHdfsAppendMs = System.currentTimeMillis()

            numBatchs += 1

            if (numBatchs == bufferSwitchThreshold ){
              numBatchs = 0
              bufferSwitchFlag = "true"
              queueIdx = nextBufferIdx(queueIdx, queueSize)
              logger.info("HdfsAppend thread switch to buffer with bufferIdx = " + queueIdx + " for numBatches reach bufferSwitchThreshold = " + bufferSwitchThreshold)
            }

          }
          lastHdfsUnAppendMs = System.currentTimeMillis() - lastHdfsAppendMs

        } else {
          // line == null
          logger.info("HdfsAppend thread switch buffer for got line = null after wait " + bufferCheckSleepMsPerBuffer + " ms")
          bufferSwitchFlag = "true"
          //          Thread.sleep(bufferCheckSleepMsPerBuffer)
          lastHdfsUnAppendMs = System.currentTimeMillis - lastHdfsAppendMs

          queueIdx = nextBufferIdx(queueIdx, queueSize)

          logger.debug("HdfsAppend thread does not append msg for " + lastHdfsUnAppendMs + " ms")
        }

        //超过指定时间，如果有数据，执行一次 append
        if (sb.length > 0 && lastHdfsUnAppendMs > batchMaxMs) {
          logger.info("HdfsAppend thread appending msg for reach batchMaxMS = " + batchMaxMs + ", numMsgsInBatch = " + numMsgsInBatch)
          appendStringToHdfs(sb.toString(), hadoop_output, threadLocalFs.get())
          sb.setLength(0)
          numMsgsInBatch = 0
          lastHdfsAppendMs = System.currentTimeMillis()

          numBatchs += 1
          if (numBatchs == bufferSwitchThreshold ){
            numBatchs = 0
            bufferSwitchFlag = "true"
            queueIdx = nextBufferIdx(queueIdx, queueSize)
          }
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
