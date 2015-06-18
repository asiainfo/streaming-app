package tools.redis.load.areamap

import java.sql.ResultSet
import java.text.SimpleDateFormat
import java.util.concurrent.{Executors, FutureTask, TimeUnit}
import java.util.{HashMap => JHashMap, _}

import org.slf4j.LoggerFactory
import tools.jdbc.JdbcUtils
import tools.redis.RedisUtils
import tools.redis.load.{FutureTaskResult, LoadStatus, LoadStatusUpdateThread, MonitorTask}

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by tsingfu on 15/6/7.
 */
object Jdbc2Hashes {

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
    val dummyEndMarker = "dummyEndMarker" //处理scala string1.split 陷阱问题

    val props = init_props_fromXml(confXmlFile)

    val redisServers = props.getProperty("redis.servers")
    val redisDatabase = props.getProperty("redis.database").trim.toInt
    val redisTimeout = props.getProperty("redis.timeout").trim.toInt
    val redisPasswd = props.getProperty("redis.password")
    val redisPassword = if(redisPasswd==null||redisPasswd == "") null else redisPasswd

    val jedisPoolMaxTotal = props.getProperty("jedisPool.maxTotal").trim.toInt
    val jedisPoolMaxIdle = props.getProperty("jedisPool.maxIdle").trim.toInt
    val jedisPoolMinIdle = props.getProperty("jedisPool.minIdle").trim.toInt

    val jdbcPoolMaxActive = props.getProperty("jdbcPool.maxActive").trim.toInt
    val jdbcPoolInitialSize = props.getProperty("jdbcPool.initialSize").trim.toInt
    val jdbcPoolMaxIdle = props.getProperty("jdbcPool.maxIdle").trim.toInt
    val jdbcPoolMinIdle = props.getProperty("jdbcPool.minIdle").trim.toInt

    val from = props.getProperty("load.from").trim

    val jdbcDriver = props.getProperty("load.driver").trim
    val jdbcUrl = props.getProperty("load.url").trim
    val jdbcUsername = props.getProperty("load.username").trim
    val jdbcPassword = props.getProperty("load.password").trim
    val jdbcTable = props.getProperty("load.table").trim
    val jdbcIsBigTable = props.getProperty("load.isBigTable").trim.toBoolean

    val hashNamePrefix = props.getProperty("load.hashNamePrefix").trim
    val hashColumnNames = props.getProperty("load.hashColumnNames").trim.split(",").map(_.trim)
    val hashSeperator = props.getProperty("load.hashSeperator").trim

    val fieldNames = props.getProperty("load.fieldNames").trim.split(",").map(_.trim)
    val valueColumnNames = props.getProperty("load.valueColumnNames").trim.split(",").map(_.trim)

    val valueMapEnabledColumnNames = props.getProperty("load.valueMapEnabled.columnNames").split(",").map(_.trim)
    val valueMapEnabledWhereSperator = props.getProperty("load.valueMapEnabled.where.seperator").trim
    val valueMapEnabledWhereValueSperator = props.getProperty("load.valueMapEnabled.where.valueSeperator").trim
    val valueMapEnabledWhere = props.getProperty("load.valueMapEnabled.where")
            .split(valueMapEnabledWhereSperator).map(_.split(valueMapEnabledWhereValueSperator))
    val valueMaps = props.getProperty("load.valueMaps").trim.split(",").map(_.trim)

    println("= = " * 20)
    println("valueMapEnabledWhere=" + props.getProperty("load.valueMapEnabled.where").split(valueMapEnabledWhereSperator).mkString("[", ",","]"))
    println("valueMapEnabledWhere=" + props.getProperty("load.valueMapEnabled.where").split(valueMapEnabledWhereSperator).foreach(values=>{
      println(values.split(valueMapEnabledWhereValueSperator).mkString("[", "-", "]"))
    }))
    println("valueMapEnabledWhere.length = " + valueMapEnabledWhere.length)
    println("= = " * 20)


    val conversion10to16ColumnNames = props.getProperty("load.conversion10to16.columnNames").trim.split(",").map(_.trim)

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


    //初始化jdbc连接池，获取conn和stmt
    val ds = JdbcUtils.init_dataSource(jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword,
      jdbcPoolMaxActive, jdbcPoolInitialSize, jdbcPoolMaxIdle, jdbcPoolMinIdle)

    val conn = ds.getConnection
    val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)


    //获取要加载的记录数
    val countSql = "select count(1) from " + jdbcTable
    logger.info("SQL : " + countSql)
    val countRs = stmt.executeQuery(countSql)
    while(countRs.next()){
      loadStatus.numTotal = countRs.getLong(1)
    }
    logger.info(jdbcTable + " numTotal = " + loadStatus.numTotal)
    countRs.close()


    //遍历数据准备，初始化构造线程任务处理批量数据的信息
    //格式： Array[String]
    //      String格式: hashNameValue, fieldValue1, fieldValue2 ,... fieldValuen
    val hashColumnNamesLength = hashColumnNames.length
    val valueColumnNamesLength = valueColumnNames.length
    val columnSeperator = "Jdbc2HashesSeperator"
    var numInBatch = 0
    var batchArrayBuffer: ArrayBuffer[String] = null
    def jedisPoolId = (loadStatus.numBatches % numPools).toInt

    val conversion10to16ColumnIdxes = ArrayBuffer[Int]()
    for(colIdx<-hashColumnNames.zipWithIndex){
      val (col, idx) = colIdx
      if (conversion10to16ColumnNames.contains(col)) conversion10to16ColumnIdxes.append(idx)
    }

    val valueMapEnabledColumnIdxes = ArrayBuffer[Int]()
    for(colIdx<-valueColumnNames.zipWithIndex){
      val (col, idx) = colIdx
      if (valueMapEnabledColumnNames.contains(col)) valueMapEnabledColumnIdxes.append(hashColumnNamesLength + idx)
    }

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
    //解决大表遍历内存问题
    // 针对mysql大表setFetchSize无效问题的处理
    // 参考
    // Ref: [Bug #18148	setFetchSize() does not adhear to jdbc interface contract](http://bugs.mysql.com/bug.php?id=18148)
    // Ref: [JDBC基础-setFetchSize方法](http://blog.csdn.net/hx756262429/article/details/8196845)
    // Ref: [关于oracle与mysql官方jdbc的一些区别](http://blog.csdn.net/seven_3306/article/details/9303979)
    // For Mysql: 正常情况下MySQL的JDBC是不支持setFetchSize()方法设置的，总是一次性全部抓取到内存中，大表遍历时会遇到内存溢出问题
    //    解决方法：
    //    方法1，分页抓取以降低内存开销，问题，程序复杂，查询效率低
    //    方法2，JDBC自动以流的方式进行数据抓取，statement.setFetchSize(Integer.MIN_VALUE);
    //    方法3，尝试在JDBC的URL上加上参数"useCursorFetch=true"，可以按setFetchSize指定的数量批量抓取数据，
    //          问题：支持MySQL 5.0 以上的Server/Connector。推荐以此方式解决这个问题
    //          现场测试没有生效
    //
    if(jdbcIsBigTable){
      if(jdbcDriver.equals("com.mysql.jdbc.Driver")){
        if (!jdbcUrl.contains("useCursorFetch=true")){
          logger.info(jdbcDriver + " with default fetchSize = " + stmt.getFetchSize + ", not found useCursorFetch=true in mysql url, so set fetchSize to Integer.MIN_VALUE")
          stmt.setFetchSize(Integer.MIN_VALUE)
          stmt.setFetchDirection(ResultSet.FETCH_FORWARD)
        } else {
          logger.info(jdbcDriver + " with default fetchSize = " + stmt.getFetchSize + ", found useCursorFetch=true in mysql url, so set fetchSize to " + batchLimit)
          stmt.setFetchSize(batchLimit)
        }
      } else {
        logger.info(jdbcDriver + " with default fetchSize = " + stmt.getFetchSize + ",  set fetchSize to " + batchLimit)
        stmt.setFetchSize(batchLimit)
      }
    }

    val sql = "select " + hashColumnNames.mkString(",") + "," +
            valueColumnNames.mkString(",") +
            " from " + jdbcTable
    logger.info("SQL : "+sql)

    val rs = stmt.executeQuery(sql)


    while(rs.next()){
      loadStatus.numScanned += 1

      val hashNameValues = (for (i <- 1 to hashColumnNamesLength) yield rs.getString(i)).mkString(columnSeperator)
      val fieldValues = (for (i <- 1 to hashColumnNamesLength) yield rs.getString(hashColumnNamesLength + i)).mkString(columnSeperator) + columnSeperator + dummyEndMarker


      if(numInBatch == 0){
        batchArrayBuffer = new ArrayBuffer[String]()
      }
      batchArrayBuffer.append(hashNameValues + columnSeperator + fieldValues)
      numInBatch += 1

      if(numInBatch == batchLimit){
        logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
        val task = new Load2HashesThread(batchArrayBuffer.toArray, columnSeperator,
          hashNamePrefix, (0 until hashColumnNamesLength).toArray, hashSeperator, conversion10to16ColumnIdxes.toArray,
          fieldNames,
          (hashColumnNamesLength until (hashColumnNamesLength + valueColumnNamesLength)).toArray,
          valueMapEnabledColumnIdxes.toArray, valueMapEnabledWhere, valueMaps,
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
      val task = new Load2HashesThread(batchArrayBuffer.toArray, columnSeperator,
        hashNamePrefix, (0 until hashColumnNamesLength).toArray, hashSeperator, conversion10to16ColumnIdxes.toArray,
        fieldNames,
        (hashColumnNamesLength until (hashColumnNamesLength + valueColumnNamesLength)).toArray,
        valueMapEnabledColumnIdxes.toArray, valueMapEnabledWhere, valueMaps,
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

    JdbcUtils.closeQuiet(rs, stmt, conn)
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


    val jdbcPoolMaxActive = (conf \ "jdbcPool" \ "maxActive").text.trim
    val jdbcPoolInitialSize =  (conf \ "jdbcPool" \ "initialSize").text.trim
    val jdbcPoolMaxIdle = (conf \ "jdbcPool" \ "maxIdle").text.trim
    val jdbcPoolMinIdle = (conf \ "jdbcPool" \ "minIdle").text.trim


    val from = (conf \ "load" \ "from").text.trim

    val jdbcDriver = (conf \ "load" \ "driver").text.trim
    val jdbcUrl = (conf \ "load" \ "url").text.trim
    val jdbcUsername = (conf \ "load" \ "username").text.trim
    val jdbcPassword = (conf \ "load" \ "password").text.trim
    val jdbcTable = (conf \ "load" \ "table").text.trim
    val jdbcIsBigTable =  (conf \ "load" \ "isBigTable").text.trim

    val hashNamePrefix = (conf \ "load" \ "hashNamePrefix").text.trim
    val hashColumnNames = (conf \ "load" \ "hashColumnNames").text.trim
    val hashSeperator = (conf \ "load" \ "hashSeperator").text.trim

    val conversion10to16ColumnNames = (conf \ "load" \ "conversion10to16.columnNames").text

    val fieldNames = (conf \ "load" \ "fieldNames").text.trim
    val valueColumnNames = (conf \ "load" \ "valueColumnNames").text.trim
    val valueMapEnabledColumnNames = (conf \ "load" \ "valueMapEnabled.columnNames").text.trim
    val valueMapEnabledWhere = (conf \ "load" \ "valueMapEnabled.where").text.trim
    val valueMapEnabledWhereSeperator = (conf \ "load" \ "valueMapEnabled.where.seperator").text.trim
    val valueMapEnabledWhereValueSeperator = (conf \ "load" \ "valueMapEnabled.where.valueSeperator").text.trim
    val valueMaps = (conf \ "load" \ "valueMaps").text.trim

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

    props.put("jdbcPool.maxActive", jdbcPoolMaxActive)
    props.put("jdbcPool.initialSize", jdbcPoolInitialSize)
    props.put("jdbcPool.maxIdle", jdbcPoolMaxIdle)
    props.put("jdbcPool.minIdle", jdbcPoolMinIdle)

    props.put("load.driver", jdbcDriver)
    props.put("load.url", jdbcUrl)
    props.put("load.username", jdbcUsername)
    props.put("load.password", jdbcPassword)
    props.put("load.table", jdbcTable)
    props.put("load.isBigTable", jdbcIsBigTable)

    props.put("load.from", from)

    props.put("load.hashNamePrefix", hashNamePrefix)
    props.put("load.hashColumnNames", hashColumnNames)
    props.put("load.hashSeperator", hashSeperator)
    props.put("load.conversion10to16.columnNames", conversion10to16ColumnNames)

    props.put("load.fieldNames", fieldNames)
    props.put("load.valueColumnNames", valueColumnNames)
    props.put("load.valueMapEnabled.columnNames", valueMapEnabledColumnNames)
    props.put("load.valueMapEnabled.where", valueMapEnabledWhere)
    props.put("load.valueMapEnabled.where.seperator", valueMapEnabledWhereSeperator)
    props.put("load.valueMapEnabled.where.valueSeperator", valueMapEnabledWhereValueSeperator)
    props.put("load.valueMaps", valueMaps)

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




