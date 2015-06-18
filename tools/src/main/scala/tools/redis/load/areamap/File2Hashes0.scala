package tools.redis.load.areamap

import java.text.SimpleDateFormat
import java.util.concurrent.{Executors, FutureTask, TimeUnit}
import java.util.{HashMap => JHashMap, _}

import org.slf4j.LoggerFactory
import tools.redis.RedisUtils
import tools.redis.load.{FutureTaskResult, LoadStatus, LoadStatusUpdateThread, MonitorTask}

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by tsingfu on 15/6/7.
 */
object File2Hashes0 {

  val logger = LoggerFactory.getLogger(this.getClass)
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //进度信息监控
  val loadStatus = new LoadStatus()
  val taskMap = scala.collection.mutable.HashMap[Long, FutureTask[FutureTaskResult]]()
  val timer = new Timer() //用于调度reporter任务，定期输出进度信息

  def main(args: Array[String]): Unit ={
    val confXmlFile = args(0)
    jdbc2Hashes(confXmlFile)
  }

  def jdbc2Hashes(confXmlFile: String): Unit ={

    //解析配置
    val props = init_props_fromXml(confXmlFile)

    val redisServers = props.getProperty("redis.servers")
    val redisDatabase = props.getProperty("redis.database").trim.toInt
    val redisTimeout = props.getProperty("redis.timeout").trim.toInt
    val redisPasswd = props.getProperty("redis.password")
    val redisPassword = if(redisPasswd==null||redisPasswd == "") null else redisPasswd

    val jedisPoolMaxTotal = props.getProperty("jedisPool.maxTotal").trim.toInt
    val jedisPoolMaxIdle = props.getProperty("jedisPool.maxIdle").trim.toInt
    val jedisPoolMinIdle = props.getProperty("jedisPool.minIdle").trim.toInt

    val from = props.getProperty("load.from").trim
    assert(from=="file", "WARN: support only file From now")

    val filename = props.getProperty("load.filename").trim
    val fileEncode = props.getProperty("load.fileEncode").trim
    val columnSeperator = props.getProperty("load.columnSeperator").trim

    val hashNamePrefix = props.getProperty("load.hashNamePrefix").trim
    val hashIdxes = props.getProperty("load.hashIdxes").trim.split(",").map(_.trim.toInt)
    val hashSeperator = props.getProperty("load.hashSeperator").trim

    val fieldName = props.getProperty("load.fieldName").trim
    val valueIdx = props.getProperty("load.valueIdx").trim.toInt

    val valueMapEnabled = props.getProperty("load.valueMapEnabled").trim.toBoolean
    val valueMap = props.getProperty("load.valueMap").trim
    val conversion10to16Idxes = props.getProperty("load.conversion10to16.columnNames").trim.split(",").map(_.trim.toInt)

    val batchLimit = props.getProperty("load.batchLimit").trim.toInt
    val batchLimitForRedis = props.getProperty("load.batchLimit.redis").trim.toInt

    val numThreads = props.getProperty("load.numThreads", "1").trim.toInt
    val loadMethod = props.getProperty("load.method", "hset").trim

    val overwrite = props.getProperty("load.overwrite").trim.toBoolean
    val appendSeperator = props.getProperty("load.appendSeperator").trim

    val reportEnabled = props.getProperty("load.report.enabled", "false").trim.toBoolean
    val reportDelaySeconds = props.getProperty("load.report.delay.seconds", "10").trim.toLong
    val reportIntervalSeconds = props.getProperty("load.report.interval.seconds", "60").trim.toLong


    //记录开始时间
    loadStatus.startTimeMs = System.currentTimeMillis()
    logger.info("startTimeMs = " + loadStatus.startTimeMs + "")


    //初始化 jedisPool, jedis, pipeline
    val jedisPools = redisServers.split(",").map(server=>{
      val hostPort = server.split(":").map(_.trim)
      val host = hostPort(0)
      val port = hostPort(1).toInt
      println("host = " + host +", port ="+port)
      RedisUtils.init_jedisPool(host, port, redisTimeout, redisDatabase, redisPassword,
        jedisPoolMaxTotal, jedisPoolMaxIdle, jedisPoolMinIdle)
    })
    val numPools = jedisPools.length
    val jedises = jedisPools.map(_.getResource)
    val pipelines = jedises.map(_.pipelined)


    //初始化线程池
    val threadPool = Executors.newFixedThreadPool(numThreads)
    val threadPool2 = Executors.newFixedThreadPool(1)


    //获取要加载的记录数
    loadStatus.numTotal = scala.io.Source.fromFile(filename, fileEncode).getLines().length


    //遍历数据准备，初始化构造线程任务处理批量数据的信息
    //格式： Array[String]
    //      String格式: hashNameValue, fieldValue1, fieldValue2 ,... fieldValuen
    var numInBatch = 0
    var batchArrayBuffer: ArrayBuffer[String] = null
    def jedisPoolId = (loadStatus.numBatches % numPools).toInt

    //遍历数据之前，先启动进度监控线程和进度信息更新线程
    //如果启用定期输出进度信息功能，启动reporter任务
    if(reportEnabled){
      timer.schedule(new MonitorTask(loadStatus, reportIntervalSeconds),
        reportDelaySeconds * 1000, reportIntervalSeconds * 1000)
    }

    //启动进度信息更新线程
    val loadStatusUpdateTask = new LoadStatusUpdateThread(loadStatus, taskMap)
    threadPool2.submit(loadStatusUpdateTask)


    //遍历数据，提交加载任务，之后等待加载完成

    for (line <- scala.io.Source.fromFile(filename, fileEncode).getLines()) {
      loadStatus.numScanned += 1

      if(numInBatch == 0){
        batchArrayBuffer = new ArrayBuffer[String]()
      }
      batchArrayBuffer.append(line)
      numInBatch += 1

      if(numInBatch == batchLimit){
        logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
        val task = new Load2HashesThread0(batchArrayBuffer.toArray, columnSeperator,
          hashNamePrefix, hashIdxes,hashSeperator, conversion10to16Idxes,
          fieldName, valueIdx, valueMapEnabled, valueMap,
          jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
          FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
        val futureTask = new FutureTask[FutureTaskResult](task)

        threadPool.submit(futureTask)
        taskMap.put(loadStatus.numBatches, futureTask)

        loadStatus.numBatches += 1
        numInBatch = 0
      }
    }


    //遍历完数据后，提交没有达到batchLimit的batch任务
    if(numInBatch > 0){
      logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
      val task = new Load2HashesThread0(batchArrayBuffer.toArray, columnSeperator,
        hashNamePrefix, hashIdxes, hashSeperator, conversion10to16Idxes,
        fieldName, valueIdx, valueMapEnabled, valueMap,
        jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
        FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
      val futureTask = new FutureTask[FutureTaskResult](task)

      threadPool.submit(futureTask)
      taskMap.put(loadStatus.numBatches, futureTask)

      loadStatus.numBatches += 1
      numInBatch = 0
    }


    //提交完加载任务后，等待所有加载线程任务完成
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.DAYS)


    //加载线程任务都完成后，关闭监控线程，并输出最终的统计信息
    loadStatus.loadFinished = true
    threadPool2.shutdown()
    threadPool2.awaitTermination(Long.MaxValue, TimeUnit.DAYS)
    if(reportEnabled) timer.cancel()

    val runningTimeMs = System.currentTimeMillis() - loadStatus.startTimeMs
    val loadSpeedPerSec = loadStatus.numProcessed * 1.0 / runningTimeMs * 1000 //记录加载运行期间加载平均速度
    val loadSpeedPerSecLastMonitored = (loadStatus.numProcessed - loadStatus.numProcessedlastMonitored) * 1.0 / reportIntervalSeconds //记录最近一次输出进度信息的周期对应的加载平均速度
    loadStatus.numProcessedlastMonitored = loadStatus.numProcessed

    println(sdf.format(new Date()) + " [INFO] finished load, statistics: numTotal = "+loadStatus.numTotal+ ", numScanned = " + loadStatus.numScanned +", numBatches = "+loadStatus.numBatches +
            ", numProcessed = " + loadStatus.numProcessed + ", numProcessedBatches = "+loadStatus.numBatchesProcessed+
            ", runningTime = " + runningTimeMs +" ms <=> " + runningTimeMs / 1000.0  +" s <=> " + runningTimeMs / 1000.0 / 60 +" min" +
            ", loadSpeed = " + loadSpeedPerSec +", records/s => " + (loadSpeedPerSec * 60) + " records/min <=> " + (loadSpeedPerSec * 60 * 60) +" records/h" +
            ", loadSpeedLastMonitored = " + loadSpeedPerSecLastMonitored +" records/s <=> " + loadSpeedPerSecLastMonitored * 60 +" records/min  " +
            "<=> " + (loadSpeedPerSecLastMonitored * 60 * 60) +" records/h" +
            ", loadProgress percent of numProcessed = " + loadStatus.numProcessed * 1.0 / loadStatus.numTotal * 100 +"%" +
            ", loadProgress percent of numBatchesProcessed = " + loadStatus.numBatchesProcessed * 1.0 / loadStatus.numBatches * 100 +"%" )


    //释放资源
    logger.info("Release jedis Pool resources...")
    for(i <- 0 until numPools){
      jedisPools(i).returnResourceObject(jedises(i))
    }
    jedisPools.foreach(_.close())

  }

  /**
   * 从xml文件中初始化配置
   * @param confXmlFile
   * @return
   */
  def init_props_fromXml(confXmlFile: String): Properties ={

    val conf = XML.load(confXmlFile)
    val servers = (conf \ "redis" \ "servers").text.trim

    val database = (conf \ "redis" \ "database").text.trim
    val timeout = (conf \ "redis" \ "timeout").text.trim
    val passwd = (conf \ "redis" \ "password").text.trim
    val password = if (passwd == "" || passwd == null) null else passwd

    val maxTotal = (conf \ "jedisPool" \ "maxTotal").text.trim
    val maxIdle = (conf \ "jedisPool" \ "maxIdle").text.trim
    val minIdle = (conf \ "jedisPool" \ "minIdle").text.trim

    val from = (conf \ "load" \ "from").text.trim

    val filename = (conf \ "load" \ "filename").text.trim
    val fileEncode = (conf \ "load" \ "fileEncode").text.trim
    val columnSeperator = (conf \ "load" \ "columnSeperator").text.trim

    val hashNamePrefix = (conf \ "load" \ "hashNamePrefix").text.trim
    val hashIdxes = (conf \ "load" \ "hashIdxes").text.trim
    val hashSeperator = (conf \ "load" \ "hashSeperator").text.trim

    val fieldName = (conf \ "load" \ "fieldName").text.trim
    val valueIdx = (conf \ "load" \ "valueIdx").text.trim
    //    val valueSeperator = (conf \ "load" \ "valueSeperator").text.trim


    val batchLimit = (conf \ "load" \ "batchLimit").text.trim
    val batchLimitForRedis = (conf \ "load" \ "batchLimit.redis").text.trim

    val numThreads = (conf \ "load" \ "numThreads").text.trim
    val loadMethod = (conf \ "load" \ "method").text.trim

    val overwrite = (conf \ "load" \ "overwrite").text.trim
    val appendSeperator = (conf \ "load" \ "appendSeperator").text.trim

    val reportEnabled = (conf \ "load" \ "report.enabled").text.trim
    val reportDelaySeconds = (conf \ "load" \ "report.delay.seconds").text.trim
    val reportIntervalSeconds = (conf \ "load" \ "report.interval.seconds").text.trim

    val props = new Properties()
    props.put("redis.servers", servers)
    props.put("redis.database", database)
    props.put("redis.timeout", timeout)

    if(password != null || password == "") props.put("redis.password", password)

    props.put("jedisPool.maxTotal", maxTotal)
    props.put("jedisPool.maxIdle", maxIdle)
    props.put("jedisPool.minIdle", minIdle)

    props.put("load.from", from)
    props.put("load.filename", filename)
    props.put("load.fileEncode", fileEncode)
    props.put("load.columnSeperator", columnSeperator)

    props.put("load.hashNamePrefix", hashNamePrefix)
    props.put("load.hashIdxes", hashIdxes)
    props.put("load.hashSeperator", hashSeperator)

    props.put("load.fieldName", fieldName)
    props.put("load.valueIdx", valueIdx)
    //    props.put("load.valueSeperator", valueSeperator)

    props.put("load.batchLimit", batchLimit)
    props.put("load.batchLimit.redis", batchLimitForRedis)
    props.put("load.numThreads", numThreads)
    props.put("load.method", loadMethod)

    props.put("load.overwrite", overwrite)
    props.put("load.appendSeperator", appendSeperator)

    props.put("load.report.enabled", reportEnabled)
    props.put("load.report.delay.seconds", reportDelaySeconds)
    props.put("load.report.interval.seconds", reportIntervalSeconds)

    println("="*80)
    //TODO: 解决properties配置项输出乱序问题
    props.list(System.out)//问题，没有顺序

    println("="*80)

    props
  }

}




