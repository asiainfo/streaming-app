package tools.redis.load

import java.sql.{Connection, ResultSet, Statement}
import java.text.SimpleDateFormat
import java.util.concurrent.{Executors, FutureTask, TimeUnit}
import java.util.{Date, Properties, Timer}

import org.slf4j.LoggerFactory
import tools.jdbc.JdbcUtils
import tools.redis.RedisUtils

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by tsingfu on 15/6/16.
 */
object Sync2Redis {

  val logger = LoggerFactory.getLogger(this.getClass)
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

  //进度信息监控
  val loadStatus = new LoadStatus()
  val taskMap = scala.collection.mutable.HashMap[Long, FutureTask[FutureTaskResult]]()
  val timer = new Timer() //用于调度reporter任务，定期输出进度信息

  def main(args: Array[String]): Unit ={
    if (args.length != 1) {
      println("Error: args.length = " + args.length + "\n" + "You should specify a confXmlFile")
      System.exit(-1)
    }
    val confXmlFile = args(0)
    jdbc2Hashes(confXmlFile)
  }

  def jdbc2Hashes(confXmlFile: String): Unit ={

    //解析配置
    val props = init_props_fromXml(confXmlFile)

    val redisServers = props.getProperty("redis.servers")
    val redisDatabase = props.getProperty("redis.database").trim.toInt
    val redisTimeout = props.getProperty("redis.timeout").trim.toInt
    val redisPassword = props.getProperty("redis.password") match {
      case "" => null
      case x => x
    }

    val jedisPoolMaxTotal = props.getProperty("jedisPool.maxTotal").trim.toInt
    val jedisPoolMaxIdle = props.getProperty("jedisPool.maxIdle").trim.toInt
    val jedisPoolMinIdle = props.getProperty("jedisPool.minIdle").trim.toInt

    val from = props.getProperty("load.from").trim
    val redisType = props.getProperty("load.redis.type").trim


    val syncIncrementEnabled = props.getProperty("load.sync.incrementEnabled").trim.toBoolean

    var syncInsertFlag: String = null
    var syncUpdateFlag: String = null
    var syncDeleteFlag: String = null
    var syncInsertEnabled: Boolean = false
    var syncUpdateEnabled: Boolean = false
    var syncDeleteEnabled: Boolean = false
    var syncColumnName: String = null
    var syncIdx: Int = 0

    if(syncIncrementEnabled){
      syncInsertFlag = props.getProperty("load.sync.insertFlag").trim
      syncUpdateFlag = props.getProperty("load.sync.updateFlag").trim
      syncDeleteFlag = props.getProperty("load.sync.deleteFlag").trim

      syncInsertEnabled = props.getProperty("load.sync.insertEnabled").trim.toBoolean
      syncUpdateEnabled = props.getProperty("load.sync.insertEnabled").trim.toBoolean
      syncDeleteEnabled = props.getProperty("load.sync.insertEnabled").trim.toBoolean
    }


    val batchLimit = props.getProperty("load.batchLimit").trim.toInt
    val batchLimitForRedis = props.getProperty("load.batchLimit.redis").trim.toInt

    val numThreads = props.getProperty("load.numThreads", "1").trim.toInt
    val loadMethod = props.getProperty("load.method", "hset").trim

    val overwrite = props.getProperty("load.overwrite").trim.toBoolean
    val appendSeperator = props.getProperty("load.appendSeperator").trim

    val reportEnabled = props.getProperty("load.report.enabled").trim.toBoolean
    val reportDelaySeconds = (props.getProperty("load.report.delay.seconds") match{
      case null => "5"
      case "" => "5"
      case x => x
    }).toLong
    val reportIntervalSeconds = (props.getProperty("load.report.interval.seconds") match {
      case null => "60"
      case "" => "60"
      case x => x
    }).toLong


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

    def jedisPoolId = (loadStatus.numBatches % numPools).toInt


    //初始化线程池
    val threadPool = Executors.newFixedThreadPool(numThreads)
    val threadPool2 = Executors.newFixedThreadPool(1)


    //遍历数据之前，先启动进度监控线程和进度信息更新线程
    //如果启用定期输出进度信息功能，启动reporter任务
    if(reportEnabled){
      timer.schedule(new MonitorTask(loadStatus, reportIntervalSeconds),
        reportDelaySeconds * 1000, reportIntervalSeconds * 1000)
    }

    //启动进度信息更新线程
    val loadStatusUpdateTask = new LoadStatusUpdateThread(loadStatus, taskMap)
    threadPool2.submit(loadStatusUpdateTask)


    //定义db相关的变量
    var ds: org.apache.tomcat.jdbc.pool.DataSource = null
    var conn: Connection = null
    var stmt: Statement = null
    var rs: ResultSet = null


    //初始化遍历变量
    var numInBatch = 0
    var batchArrayBuffer: ArrayBuffer[String] = ArrayBuffer[String]() //For insert or update
    var numInBatchForDelete = 0
    var batchArrayBufferForDelete: ArrayBuffer[String] = ArrayBuffer[String]() //For delete


    from match {
      case "db" =>

        val jdbcPoolMaxActive = props.getProperty("jdbcPool.maxActive").trim.toInt
        val jdbcPoolInitialSize = props.getProperty("jdbcPool.initialSize").trim.toInt
        val jdbcPoolMaxIdle = props.getProperty("jdbcPool.maxIdle").trim.toInt
        val jdbcPoolMinIdle = props.getProperty("jdbcPool.minIdle").trim.toInt

        val jdbcDriver = props.getProperty("load.driver").trim
        val jdbcUrl = props.getProperty("load.url").trim
        val jdbcUsername = props.getProperty("load.username").trim
        val jdbcPassword = props.getProperty("load.password").trim
        val jdbcTable = props.getProperty("load.table").trim
        val jdbcIsBigTable = props.getProperty("load.isBigTable").trim.toBoolean

        if(syncIncrementEnabled) syncColumnName=props.getProperty("load.sync.columnName").trim

        //初始化jdbc连接池，获取conn和stmt
        ds = JdbcUtils.init_dataSource(jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword,
          jdbcPoolMaxActive, jdbcPoolInitialSize, jdbcPoolMaxIdle, jdbcPoolMinIdle)

        conn = ds.getConnection
        stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)


        //获取要加载的记录数
        val countSql = "select count(1) from " + jdbcTable
        logger.info("SQL : " + countSql)
        val countRs = stmt.executeQuery(countSql)
        while(countRs.next()){
          loadStatus.numTotal = countRs.getLong(1)
        }
        logger.info(jdbcTable + " numTotal = " + loadStatus.numTotal)
        countRs.close()


        //遍历数据，提交加载任务，之后等待加载完成
        //解决大表遍历内存问题
        // 针对mysql大表setFetchSize无效问题的处理
        // 参考
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


        redisType match {
          case "multihashes" =>

            val hashNamePrefix = props.getProperty("load.hashNamePrefix").trim
            val hashColumnNames = props.getProperty("load.hashColumnNames").trim.split(",").map(_.trim)
            val hashSeperator = props.getProperty("load.hashSeperator").trim

            val valueColumnNames = props.getProperty("load.valueColumnNames").trim.split(",").map(_.trim)
            val fieldNames = props.getProperty("load.fieldNames").trim.split(",").map(_.trim)


            //遍历数据准备，初始化构造线程任务处理批量数据的信息
            //格式： Array[String]
            //      String格式: hashNameValue, fieldValue1, fieldValue2 ,... fieldValuen
            val hashColumnNamesLength = hashColumnNames.length
            val valueColumnNamesLength = valueColumnNames.length
            val columnSeperator = "Db2MultiHashesSeperator"


            val sql = if(syncIncrementEnabled) {
              "select " + hashColumnNames.mkString(",") + "," +
                      valueColumnNames.mkString(",") +
                      ", "+syncColumnName +
                      " from " + jdbcTable
            } else{
              "select " + hashColumnNames.mkString(",") + "," +
                      valueColumnNames.mkString(",") +
                      " from " + jdbcTable
            }

            logger.info("SQL : "+sql)

            rs = stmt.executeQuery(sql)

            val syncColumnIdx = hashColumnNamesLength + valueColumnNamesLength + 1 //sync标识字段的位置索引

            while(rs.next()){
              loadStatus.numScanned += 1

              val hashNameValue = (for (i <- 1 to hashColumnNamesLength) yield rs.getString(i)).mkString(hashSeperator)
              val fieldValues = for (i <- hashColumnNamesLength + 1 to hashColumnNamesLength + valueColumnNamesLength) yield rs.getString(i)

              if(syncIncrementEnabled){
                val syncFlag = rs.getString(syncColumnIdx)
                if(syncFlag == syncDeleteFlag && syncDeleteEnabled){
                  batchArrayBufferForDelete.append(hashNameValue + columnSeperator + fieldNames.mkString(columnSeperator))
                  numInBatchForDelete += 1
                } else if (syncFlag == syncInsertFlag && syncInsertEnabled){
                  batchArrayBuffer.append(hashNameValue + columnSeperator + fieldValues.mkString(columnSeperator))
                  numInBatch += 1
                } else if (syncFlag == syncUpdateFlag && syncUpdateEnabled){
                  batchArrayBuffer.append(hashNameValue + columnSeperator + fieldValues.mkString(columnSeperator))
                  numInBatch += 1
                } else {
                  logger.info("found unknown syncFlag = " + syncFlag +" for line = " + hashNamePrefix+hashNameValue)
                }
              } else {
                batchArrayBuffer.append(hashNameValue + columnSeperator + fieldValues.mkString(columnSeperator))
                numInBatch += 1
              }

              if(numInBatch == batchLimit){
                logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                val task = new Load2HashesThread(batchArrayBuffer.toArray, columnSeperator,
                  hashNamePrefix, Array(0),hashSeperator, fieldNames, (1 to valueColumnNamesLength).toArray,
                  jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
                  FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                val futureTask = new FutureTask[FutureTaskResult](task)

                threadPool.submit(futureTask)
                taskMap.put(loadStatus.numBatches, futureTask)

                loadStatus.numBatches += 1
                numInBatch = 0

                batchArrayBuffer = new ArrayBuffer[String]()
              }

              if(syncIncrementEnabled){
                if(numInBatchForDelete == batchLimit){
                  logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                  val task = new Sync2HashesHdelThread(batchArrayBufferForDelete.toArray, columnSeperator,
                    hashNamePrefix, Array(0),hashSeperator, fieldNames,
                    jedisPools(jedisPoolId),loadMethod, batchLimitForRedis,
                    FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                  val futureTask = new FutureTask[FutureTaskResult](task)

                  threadPool.submit(futureTask)
                  taskMap.put(loadStatus.numBatches, futureTask)

                  loadStatus.numBatches += 1
                  numInBatchForDelete = 0

                  batchArrayBufferForDelete = new ArrayBuffer[String]()
                }
              }
            }


            //遍历完数据后，提交没有达到batchLimit的batch任务
            if(numInBatch > 0){
              logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
              val task = new Load2HashesThread(batchArrayBuffer.toArray, columnSeperator,
                hashNamePrefix, Array(0),hashSeperator, fieldNames, (1 to valueColumnNamesLength).toArray,
                jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
                FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
              val futureTask = new FutureTask[FutureTaskResult](task)

              threadPool.submit(futureTask)
              taskMap.put(loadStatus.numBatches, futureTask)

              loadStatus.numBatches += 1
              numInBatch = 0
            }

            if(syncIncrementEnabled){
              if(numInBatchForDelete > 0){
                logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                val task = new Sync2HashesHdelThread(batchArrayBufferForDelete.toArray, columnSeperator,
                  hashNamePrefix, Array(0),hashSeperator, fieldNames,
                  jedisPools(jedisPoolId),loadMethod, batchLimitForRedis,
                  FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                val futureTask = new FutureTask[FutureTaskResult](task)

                threadPool.submit(futureTask)
                taskMap.put(loadStatus.numBatches, futureTask)

                loadStatus.numBatches += 1
                numInBatchForDelete = 0
              }
            }


          case "onehash" =>
            val hashName = props.getProperty("load.hashName").trim
            val fieldColumnNames = props.getProperty("load.fieldColumnNames").trim.split(",").map(_.trim)
            val fieldSeperator = props.getProperty("load.fieldSeperator").trim

            val valueColumnNames = props.getProperty("load.valueColumnNames").trim.split(",").map(_.trim)
            val valueSeperator = props.getProperty("load.valueSeperator").trim

            //遍历数据准备，初始化构造线程任务处理批量数据的信息
            //格式： Array[String]
            //      String格式: field1, value1
            val fieldColumnNamesLength = fieldColumnNames.length
            val valueColumnNamesLength = valueColumnNames.length
            val columnSeperator = "Db2OneHashSeperator"

            val sql = if(syncIncrementEnabled) {
              "select " +fieldColumnNames.mkString(",") + "," +
                      valueColumnNames.mkString(",") +
                      ", "+syncColumnName +
                      " from " + jdbcTable
            } else {
              "select " +fieldColumnNames.mkString(",") + "," +
                      valueColumnNames.mkString(",") +
                      " from " + jdbcTable
            }

            rs = stmt.executeQuery(sql)

            val syncColumnIdx = fieldColumnNamesLength + valueColumnNamesLength + 1 //sync标识字段的位置索引

            while(rs.next()){
              loadStatus.numScanned += 1

              val field = (for (i <- 1 to fieldColumnNamesLength) yield rs.getString(i)).mkString(fieldSeperator)
              val value = (for (i <- fieldColumnNamesLength + 1 to fieldColumnNamesLength + valueColumnNamesLength) yield rs.getString(i)).mkString(valueSeperator)

              if(syncIncrementEnabled){
                val syncFlag = rs.getString(syncColumnIdx)
                if(syncFlag == syncDeleteFlag && syncDeleteEnabled){
                  batchArrayBufferForDelete.append(field + columnSeperator + value)
                  numInBatchForDelete += 1
                } else if (syncFlag == syncInsertFlag && syncInsertEnabled){
                  batchArrayBuffer.append(field + columnSeperator + value)
                  numInBatch += 1
                } else if (syncFlag == syncUpdateFlag && syncUpdateEnabled){
                  batchArrayBuffer.append(field + columnSeperator + value)
                  numInBatch += 1
                } else {
                  logger.info("found unknown syncFlag = " + syncFlag +" for hash = " + hashName +" " + field)
                }
              } else {
                batchArrayBuffer.append(field + columnSeperator + value)
                numInBatch += 1
              }


              if(numInBatch == batchLimit){
                logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                val task = new Load2OneHashThread(batchArrayBuffer.toArray, columnSeperator,
                  hashName,
                  Array(0), fieldSeperator,
                  Array(1), valueSeperator,
                  jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
                  FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                val futureTask = new FutureTask[FutureTaskResult](task)

                threadPool.submit(futureTask)
                taskMap.put(loadStatus.numBatches, futureTask)

                loadStatus.numBatches += 1
                numInBatch = 0

                batchArrayBuffer = new ArrayBuffer[String]()
              }

              if(syncIncrementEnabled){
                if(numInBatchForDelete == batchLimit){
                  logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                  val task = new Sync2OneHashHdelThread(batchArrayBufferForDelete.toArray, columnSeperator,
                    hashName,
                    Array(0), fieldSeperator,
                    jedisPools(jedisPoolId),loadMethod, batchLimitForRedis,
                    FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                  val futureTask = new FutureTask[FutureTaskResult](task)

                  threadPool.submit(futureTask)
                  taskMap.put(loadStatus.numBatches, futureTask)

                  loadStatus.numBatches += 1
                  numInBatchForDelete = 0

                  batchArrayBufferForDelete = new ArrayBuffer[String]()
                }
              }

            }


            //遍历完数据后，提交没有达到batchLimit的batch任务
            if(numInBatch > 0){
              logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
              val task = new Load2OneHashThread(batchArrayBuffer.toArray, columnSeperator,
                hashName,
                Array(0), fieldSeperator,
                Array(1), valueSeperator,
                jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
                FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
              val futureTask = new FutureTask[FutureTaskResult](task)

              threadPool.submit(futureTask)
              taskMap.put(loadStatus.numBatches, futureTask)

              loadStatus.numBatches += 1
              numInBatch = 0
            }

            if(syncIncrementEnabled){
              if(numInBatchForDelete > 0){
                logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                val task = new Sync2OneHashHdelThread(batchArrayBufferForDelete.toArray, columnSeperator,
                  hashName,
                  Array(0), fieldSeperator,
                  jedisPools(jedisPoolId),loadMethod, batchLimitForRedis,
                  FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                val futureTask = new FutureTask[FutureTaskResult](task)

                threadPool.submit(futureTask)
                taskMap.put(loadStatus.numBatches, futureTask)

                loadStatus.numBatches += 1
                numInBatchForDelete = 0
              }
            }
        }
      case "file" =>
        val filename = props.getProperty("load.filename").trim
        val fileEncode = props.getProperty("load.fileEncode").trim
        val columnSeperator = props.getProperty("load.columnSeperator").trim

        if(syncIncrementEnabled) syncIdx=props.getProperty("load.sync.idx").trim.toInt

        //获取要加载的记录数
        loadStatus.numTotal = scala.io.Source.fromFile(filename, fileEncode).getLines().length


        redisType match {
          case "multihashes" =>

            val hashNamePrefix = props.getProperty("load.hashNamePrefix").trim
            val hashIdxes = props.getProperty("load.hashIdxes").trim.split(",").map(_.trim.toInt)
            val hashSeperator = props.getProperty("load.hashSeperator").trim

            val valueIdxes = props.getProperty("load.valueIdxes").trim.split(",").map(_.trim.toInt)
            val fieldNames = props.getProperty("load.fieldNames").trim.split(",").map(_.trim)


            //遍历数据，，提交加载任务，之后等待加载完成
            for (line <- scala.io.Source.fromFile(filename, fileEncode).getLines()) {
              loadStatus.numScanned += 1

              if(syncIncrementEnabled){
                val lineArray = line.split(columnSeperator)
                val syncFlag = lineArray(syncIdx)

                if(syncFlag == syncDeleteFlag && syncDeleteEnabled){
                  batchArrayBufferForDelete.append(line)
                  numInBatchForDelete += 1
                } else if (syncFlag == syncInsertFlag && syncInsertEnabled){
                  batchArrayBuffer.append(line)
                  numInBatch += 1
                } else if (syncFlag == syncUpdateFlag && syncUpdateEnabled){
                  batchArrayBuffer.append(line)
                  numInBatch += 1
                } else {
                  logger.info("found unknown syncFlag = " + syncFlag +" for line = " + line)
                }
              } else {
                batchArrayBuffer.append(line)
                numInBatch += 1
              }


              if(numInBatch == batchLimit){
                logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                val task = new Load2HashesThread(batchArrayBuffer.toArray, columnSeperator,
                  hashNamePrefix, hashIdxes, hashSeperator,
                  fieldNames, valueIdxes,
                  jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
                  FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                val futureTask = new FutureTask[FutureTaskResult](task)

                threadPool.submit(futureTask)
                taskMap.put(loadStatus.numBatches, futureTask)

                loadStatus.numBatches += 1
                numInBatch = 0

                batchArrayBuffer = new ArrayBuffer[String]()
              }

              if(syncIncrementEnabled){
                if(numInBatchForDelete == batchLimit){
                  logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                  val task = new Sync2HashesHdelThread(batchArrayBufferForDelete.toArray, columnSeperator,
                    hashNamePrefix, hashIdxes, hashSeperator,
                    fieldNames,
                    jedisPools(jedisPoolId),loadMethod, batchLimitForRedis,
                    FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                  val futureTask = new FutureTask[FutureTaskResult](task)

                  threadPool.submit(futureTask)
                  taskMap.put(loadStatus.numBatches, futureTask)

                  loadStatus.numBatches += 1
                  numInBatchForDelete = 0

                  batchArrayBufferForDelete = new ArrayBuffer[String]()
                }
              }

            }


            //遍历完数据后，提交没有达到batchLimit的batch任务
            if(numInBatch > 0){
              logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
              val task = new Load2HashesThread(batchArrayBuffer.toArray, columnSeperator,
                hashNamePrefix, hashIdxes, hashSeperator,
                fieldNames, valueIdxes,
                jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
                FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
              val futureTask = new FutureTask[FutureTaskResult](task)

              threadPool.submit(futureTask)
              taskMap.put(loadStatus.numBatches, futureTask)

              loadStatus.numBatches += 1
              numInBatch = 0
            }

            if(syncIncrementEnabled){
              if(numInBatchForDelete > 0){
                logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                val task = new Sync2HashesHdelThread(batchArrayBufferForDelete.toArray, columnSeperator,
                  hashNamePrefix, hashIdxes, hashSeperator,
                  fieldNames,
                  jedisPools(jedisPoolId),loadMethod, batchLimitForRedis,
                  FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                val futureTask = new FutureTask[FutureTaskResult](task)

                threadPool.submit(futureTask)
                taskMap.put(loadStatus.numBatches, futureTask)

                loadStatus.numBatches += 1
                numInBatchForDelete = 0

              }
            }


          case "onehash" =>
            val hashName = props.getProperty("load.hashName").trim
            val fieldIdxes = props.getProperty("load.fieldIdxes").trim.split(",").map(_.trim.toInt)
            val fieldSeperator = props.getProperty("load.fieldSeperator").trim
            val valueIdxes = props.getProperty("load.valueIdxes").trim.split(",").map(_.trim.toInt)
            val valueSeperator = props.getProperty("load.valueSeperator").trim



            //遍历数据，，提交加载任务，之后等待加载完成
            for (line <- scala.io.Source.fromFile(filename, fileEncode).getLines()) {
              loadStatus.numScanned += 1

              if(syncIncrementEnabled){
                val lineArray = line.split(columnSeperator)
                val syncFlag = lineArray(syncIdx)

                if(syncFlag == syncDeleteFlag && syncDeleteEnabled){
                  batchArrayBufferForDelete.append(line)
                  numInBatchForDelete += 1
                } else if (syncFlag == syncInsertFlag && syncInsertEnabled){
                  batchArrayBuffer.append(line)
                  numInBatch += 1
                } else if (syncFlag == syncUpdateFlag && syncUpdateEnabled){
                  batchArrayBuffer.append(line)
                  numInBatch += 1
                } else {
                  logger.info("found unknown syncFlag = " + syncFlag +" for line = " + line)
                }
              } else {
                batchArrayBuffer.append(line)
                numInBatch += 1
              }

              if(numInBatch == batchLimit){
                logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                val task = new Load2OneHashThread(batchArrayBuffer.toArray, columnSeperator,
                  hashName,
                  fieldIdxes, fieldSeperator,
                  valueIdxes, valueSeperator,
                  jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
                  FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                val futureTask = new FutureTask[FutureTaskResult](task)

                threadPool.submit(futureTask)
                taskMap.put(loadStatus.numBatches, futureTask)

                loadStatus.numBatches += 1
                numInBatch = 0

                batchArrayBuffer = new ArrayBuffer[String]()
              }

              if(syncIncrementEnabled){
                if(numInBatchForDelete == batchLimit){
                  logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                  val task = new Sync2OneHashHdelThread(batchArrayBufferForDelete.toArray, columnSeperator,
                    hashName,
                    fieldIdxes, fieldSeperator,
                    jedisPools(jedisPoolId),loadMethod, batchLimitForRedis,
                    FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                  val futureTask = new FutureTask[FutureTaskResult](task)

                  threadPool.submit(futureTask)
                  taskMap.put(loadStatus.numBatches, futureTask)

                  loadStatus.numBatches += 1
                  numInBatch = 0

                  batchArrayBufferForDelete = new ArrayBuffer[String]()
                }
              }

            }


            //遍历完数据后，提交没有达到batchLimit的batch任务
            if(numInBatch > 0){
              logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
              val task = new Load2OneHashThread(batchArrayBuffer.toArray, columnSeperator,
                hashName,
                fieldIdxes, fieldSeperator,
                valueIdxes, valueSeperator,
                jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator,
                FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
              val futureTask = new FutureTask[FutureTaskResult](task)

              threadPool.submit(futureTask)
              taskMap.put(loadStatus.numBatches, futureTask)

              loadStatus.numBatches += 1
              numInBatch = 0
            }

            if(syncIncrementEnabled){
              if(numInBatchForDelete > 0){
                logger.info("submit a new thread with [numScanned = " + loadStatus.numScanned + ", numBatches = " + loadStatus.numBatches + ", numInBatch = " + numInBatch +"]" )
                val task = new Sync2OneHashHdelThread(batchArrayBufferForDelete.toArray, columnSeperator,
                  hashName,
                  fieldIdxes, fieldSeperator,
                  jedisPools(jedisPoolId),loadMethod, batchLimitForRedis,
                  FutureTaskResult(loadStatus.numBatches, numInBatch, 0))
                val futureTask = new FutureTask[FutureTaskResult](task)

                threadPool.submit(futureTask)
                taskMap.put(loadStatus.numBatches, futureTask)

                loadStatus.numBatches += 1
                numInBatch = 0

              }
            }
        }
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

    if (from == "db") JdbcUtils.closeQuiet(rs, stmt, conn)
  }

  /**
   * 从xml文件中初始化配置
   * @param confXmlFile
   * @return
   */
  def init_props_fromXml(confXmlFile: String): Properties ={

    val props = new Properties()

    val conf = XML.load(confXmlFile)

    val servers = (conf \ "redis" \ "servers").text.trim
    val database = (conf \ "redis" \ "database").text.trim
    val timeout = (conf \ "redis" \ "timeout").text.trim
    val passwd = (conf \ "redis" \ "password").text.trim
    val password = if (passwd == "" || passwd == null) null else passwd

    props.put("redis.servers", servers)
    props.put("redis.database", database)
    props.put("redis.timeout", timeout)

    if(password != null || password == "") props.put("redis.password", password)


    val maxTotal = (conf \ "jedisPool" \ "maxTotal").text.trim
    val maxIdle = (conf \ "jedisPool" \ "maxIdle").text.trim
    val minIdle = (conf \ "jedisPool" \ "minIdle").text.trim

    props.put("jedisPool.maxTotal", maxTotal)
    props.put("jedisPool.maxIdle", maxIdle)
    props.put("jedisPool.minIdle", minIdle)


    val from = (conf \ "load" \ "from").text.trim
    val redisType = (conf \ "load" \ "redis.type").text.trim
    props.put("load.from", from)
    props.put("load.redis.type", redisType)


    val syncIncrementEnabled = (conf \ "load" \ "sync.incrementEnabled").text.trim
    props.put("load.sync.incrementEnabled", syncIncrementEnabled)

    syncIncrementEnabled match {
      case "false" =>
      case "true" =>
        val syncInsertFlag = (conf \ "load" \ "sync.insertFlag").text.trim
        val syncUpdateFlag = (conf \ "load" \ "sync.updateFlag").text.trim
        val syncDeleteFlag = (conf \ "load" \ "sync.deleteFlag").text.trim

        val syncInsertEnabled = (conf \ "load" \ "sync.insertEnabled").text.trim
        val syncUpdateEnabled = (conf \ "load" \ "sync.updateEnabled").text.trim
        val syncDeleteEnabled = (conf \ "load" \ "sync.deleteEnabled").text.trim

        props.put("load.sync.insertFlag", syncInsertFlag)
        props.put("load.sync.updateFlag", syncUpdateFlag)
        props.put("load.sync.deleteFlag", syncDeleteFlag)

        props.put("load.sync.insertEnabled", syncInsertEnabled)
        props.put("load.sync.updateEnabled", syncUpdateEnabled)
        props.put("load.sync.deleteEnabled", syncDeleteEnabled)

        from match {
          case "db" =>
            val syncColumnName = (conf \ "load" \ "sync.columnName").text.trim
            props.put("load.sync.columnName", syncColumnName)
          case "file" =>
            val syncIdx = (conf \ "load" \ "sync.idx").text.trim
            props.put("load.sync.idx", syncIdx)

        }
    }

    from match {
      case "db" =>
        val jdbcPoolMaxActive = (conf \ "jdbcPool" \ "maxActive").text.trim
        val jdbcPoolInitialSize =  (conf \ "jdbcPool" \ "initialSize").text.trim
        val jdbcPoolMaxIdle = (conf \ "jdbcPool" \ "maxIdle").text.trim
        val jdbcPoolMinIdle = (conf \ "jdbcPool" \ "minIdle").text.trim

        props.put("jdbcPool.maxActive", jdbcPoolMaxActive)
        props.put("jdbcPool.initialSize", jdbcPoolInitialSize)
        props.put("jdbcPool.maxIdle", jdbcPoolMaxIdle)
        props.put("jdbcPool.minIdle", jdbcPoolMinIdle)


        val jdbcDriver = (conf \ "load" \ "driver").text.trim
        val jdbcUrl = (conf \ "load" \ "url").text.trim
        val jdbcUsername = (conf \ "load" \ "username").text.trim
        val jdbcPassword = (conf \ "load" \ "password").text.trim
        val jdbcTable = (conf \ "load" \ "table").text.trim
        val jdbcIsBigTable =  (conf \ "load" \ "isBigTable").text.trim

        props.put("load.driver", jdbcDriver)
        props.put("load.url", jdbcUrl)
        props.put("load.username", jdbcUsername)
        props.put("load.password", jdbcPassword)
        props.put("load.table", jdbcTable)
        props.put("load.isBigTable", jdbcIsBigTable)


        redisType match {
          case "multihashes" =>

            val hashNamePrefix = (conf \ "load" \ "hashNamePrefix").text.trim
            val hashColumnNames = (conf \ "load" \ "hashColumnNames").text.trim
            val hashSeperator = (conf \ "load" \ "hashSeperator").text.trim
            val valueColumnNames = (conf \ "load" \ "valueColumnNames").text.trim
            val valueSeperator = (conf \ "load" \ "valueSeperator").text.trim
            val fieldNames = (conf \ "load" \ "fieldNames").text.trim

            props.put("load.hashNamePrefix", hashNamePrefix)
            props.put("load.hashColumnNames", hashColumnNames)
            props.put("load.hashSeperator", hashSeperator)
            props.put("load.valueColumnNames", valueColumnNames)
            props.put("load.valueSeperator", valueSeperator)
            props.put("load.fieldNames", fieldNames)

          case "onehash" =>
            val hashName = (conf \ "load" \ "hashName").text.trim
            val fieldColumnNames = (conf \ "load" \ "fieldColumnNames").text.trim
            val fieldSeperator = (conf \ "load" \ "fieldSeperator").text.trim
            val valueColumnNames = (conf \ "load" \ "valueColumnNames").text.trim
            val valueSeperator = (conf \ "load" \ "valueSeperator").text.trim

            props.put("load.hashName", hashName)
            props.put("load.fieldColumnNames", fieldColumnNames)
            props.put("load.fieldSeperator", fieldSeperator)
            props.put("load.valueColumnNames", valueColumnNames)
            props.put("load.valueSeperator", valueSeperator)
        }

      case "file" =>
        val filename = (conf \ "load" \ "filename").text.trim
        val fileEncode = (conf \ "load" \ "fileEncode").text.trim
        val columnSeperator = (conf \ "load" \ "columnSeperator").text.trim

        props.put("load.filename", filename)
        props.put("load.fileEncode", fileEncode)
        props.put("load.columnSeperator", columnSeperator)

        redisType match {
          case "multihashes" =>

            val hashNamePrefix = (conf \ "load" \ "hashNamePrefix").text.trim
            val hashIdxes = (conf \ "load" \ "hashIdxes").text.trim
            val hashSeperator = (conf \ "load" \ "hashSeperator").text.trim
            val valueIdxes = (conf \ "load" \ "valueIdxes").text.trim
            val fieldNames = (conf \ "load" \ "fieldNames").text.trim

            props.put("load.hashNamePrefix", hashNamePrefix)
            props.put("load.hashIdxes", hashIdxes)
            props.put("load.hashSeperator", hashSeperator)
            props.put("load.valueIdxes", valueIdxes)
            props.put("load.fieldNames", fieldNames)

          case "onehash" =>
            val hashName = (conf \ "load" \ "hashName").text.trim
            val fieldIdxes = (conf \ "load" \ "fieldIdxes").text.trim
            val fieldSeperator = (conf \ "load" \ "fieldSeperator").text.trim
            val valueIdxes = (conf \ "load" \ "valueIdxes").text.trim
            val valueSeperator = (conf \ "load" \ "valueSeperator").text.trim

            props.put("load.hashName", hashName)
            props.put("load.fieldIdxes", fieldIdxes)
            props.put("load.fieldSeperator", fieldSeperator)
            props.put("load.valueIdxes", valueIdxes)
            props.put("load.valueSeperator", valueSeperator)
        }

    }


    val batchLimit = (conf \ "load" \ "batchLimit").text.trim
    val batchLimitForRedis = (conf \ "load" \ "batchLimit.redis").text.trim
    val numThreads = (conf \ "load" \ "numThreads").text.trim

    val loadMethod = (conf \ "load" \ "method").text.trim
    val overwrite = (conf \ "load" \ "overwrite").text.trim
    val appendSeperator = (conf \ "load" \ "appendSeperator").text.trim

    val reportEnabled = (conf \ "load" \ "report.enabled").text.trim
    val reportDelaySeconds = (conf \ "load" \ "report.delay.seconds").text.trim
    val reportIntervalSeconds = (conf \ "load" \ "report.interval.seconds").text.trim


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
