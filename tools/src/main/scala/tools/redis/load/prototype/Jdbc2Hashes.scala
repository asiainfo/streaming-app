package tools.redis.load.prototype

import java.sql.ResultSet
import java.util.Properties
import java.util.concurrent.{Executors, TimeUnit}

import org.slf4j.LoggerFactory
import tools.jdbc.JdbcUtils
import tools.redis.RedisUtils

import scala.collection.mutable.ArrayBuffer
import scala.xml.XML

/**
 * Created by tsingfu on 15/6/7.
 */
object Jdbc2Hashes {

  val logger = LoggerFactory.getLogger(this.getClass)

  def main(args: Array[String]): Unit ={

    val confXmlFile = args(0)
    jdbc2Hashes(confXmlFile)

  }

  def jdbc2Hashes(confXmlFile: String): Unit ={
    val props = init_props_fromXml(confXmlFile)

    val redisServers = props.getProperty("redis.servers")
    val redisDatabase = props.getProperty("redis.database").trim.toInt
    val redisTimeout = props.getProperty("redis.timeout").trim.toInt
    val redisPasswd = props.getProperty("redis.password")
    val redisPassword = if(redisPasswd==null||redisPasswd == "") null else redisPasswd

    val jedisPoolMaxToal = props.getProperty("jedisPool.maxTotal").trim.toInt
    val jedisPoolMaxIdle = props.getProperty("jedisPool.maxIdle").trim.toInt
    val jedisPoolMinIdle = props.getProperty("jedisPool.minIdle").trim.toInt

    val jdbcPoolMaxActive = props.getProperty("jdbcPool.maxActive").trim.toInt
    val jdbcPoolInitialSize = props.getProperty("jdbcPool.initialSize").trim.toInt
    val jdbcPoolMaxIdle = props.getProperty("jdbcPool.maxIdle").trim.toInt
    val jdbcPoolMinIdle = props.getProperty("jdbcPool.minIdle").trim.toInt

    val from = props.getProperty("load.from").trim

    //    val filename = props.getProperty("load.filename").trim
    //    val fileEncode = props.getProperty("load.fileEncode").trim
    //    val columnSeperator = props.getProperty("load.columnSeperator").trim
    val jdbcDriver = props.getProperty("load.driver").trim
    val jdbcUrl = props.getProperty("load.url").trim
    val jdbcUsername = props.getProperty("load.username").trim
    val jdbcPassword = props.getProperty("load.password").trim
    val jdbcTable = props.getProperty("load.table").trim

    val hashNamePrefix = props.getProperty("load.hashNamePrefix").trim
    val hashColumnNames = props.getProperty("load.hashColumnNames").trim.split(",").map(_.trim)
    val hashSeperator = props.getProperty("load.hashSeperator").trim

    val valueColumnNames = props.getProperty("load.valueColumnNames").trim.split(",").map(_.trim)
    val fieldNames = props.getProperty("load.fieldNames").trim.split(",").map(_.trim)

    val batchLimit = props.getProperty("load.batchLimit").trim.toInt
    val batchLimitForRedis = props.getProperty("load.batchLimit.redis").trim.toInt

    val numThreads = props.getProperty("load.numThreads").trim.toInt
    val loadMethod = props.getProperty("load.method").trim

    val overwrite = props.getProperty("load.overwrite").trim.toBoolean
    val appendSeperator = props.getProperty("load.appendSeperator").trim

    // 初始化 jedisPool, jedis, pipeline
    val jedisPools = redisServers.split(",").map(server=>{
      val hostPort = server.split(":").map(_.trim)
      val host = hostPort(0)
      val port = hostPort(1).toInt
      println("host = " + host +", port ="+port)
      RedisUtils.init_jedisPool(host, port, redisTimeout, redisDatabase, redisPassword,
        jedisPoolMaxToal, jedisPoolMaxIdle, jedisPoolMinIdle)
    })
    val numPools = jedisPools.length
    val jedises = jedisPools.map(_.getResource)
    val pipelines = jedises.map(_.pipelined)

    // 初始化线程池
    val threadPool = Executors.newFixedThreadPool(numThreads)

    val ds = JdbcUtils.init_dataSource(jdbcDriver, jdbcUrl, jdbcUsername, jdbcPassword,
      jdbcPoolMaxActive, jdbcPoolInitialSize, jdbcPoolMaxIdle, jdbcPoolMinIdle)

    val conn = ds.getConnection
    val stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)

    //For bigdata
    if (jdbcUrl.contains("useCursorFetch=true")){
      stmt.setFetchSize(batchLimit)
    } else {
      logger.info("default fetchSize = " + stmt.getFetchSize + ", set fetchSize to Integer.MIN_VALUE")
      stmt.setFetchSize(Integer.MIN_VALUE)
      stmt.setFetchDirection(ResultSet.FETCH_FORWARD)
    }

    val sql = "select " + hashColumnNames.mkString(",") + "," +
            valueColumnNames.mkString(",") +
            " from " + jdbcTable
    logger.info("SQL: "+sql)

    val rs = stmt.executeQuery(sql)

    val hashColumnNamesLength = hashColumnNames.length
    val valueColumnNamesLength = valueColumnNames.length
    //构造线程需要处理的批量数据
    //格式： Array[String]
    //      String格式: hashNameValue, fieldValue1, fieldValue2 ,... fieldValuen
    val columnSeperator = "Jdbc2HashesSeperator"

    var numProcessed = 0
    var numBatches = 0
    var numInBatch = 0
    var batchArrayBuffer: ArrayBuffer[String] = null

    def jedisPoolId = numBatches % numPools

    while(rs.next()){

      val hashNameValue = (for (i <- 1 to hashColumnNamesLength) yield rs.getString(i)).mkString(hashSeperator)
      val fieldValues = for (i <- hashColumnNamesLength + 1 to hashColumnNamesLength + valueColumnNamesLength) yield rs.getString(i)

      if(numInBatch == 0){
        batchArrayBuffer = new ArrayBuffer[String]()
      }
      batchArrayBuffer.append(hashNameValue + columnSeperator + fieldValues.mkString(columnSeperator))
      numInBatch += 1

      if(numInBatch == batchLimit){
        logger.info("submit a new task with [numProcessed = " + numProcessed + ", numBatches = " + numBatches + ", numInBatch = " + numInBatch +"]" )
        threadPool.submit(new Load2HashesThread(batchArrayBuffer.toArray, columnSeperator,
          hashNamePrefix, Array(0),hashSeperator, fieldNames, (1 to valueColumnNamesLength).toArray,
          jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator
        ))
        numBatches += 1
        numInBatch = 0
      }
      numProcessed += 1
    }

    if(numInBatch > 0){
      logger.info("submit a new task with [numProcessed = " + numProcessed + ", numBatches = " + numBatches + ", numInBatch = " + numInBatch +"]" )
      threadPool.submit(new Load2HashesThread(batchArrayBuffer.toArray, columnSeperator,
        hashNamePrefix, Array(0),hashSeperator, fieldNames, (1 to valueColumnNamesLength).toArray,
        jedisPools(jedisPoolId),loadMethod, batchLimitForRedis, overwrite, appendSeperator
      ))
      numBatches += 1
      numInBatch = 0
    }

//    Thread.sleep(1 * 1000)
    threadPool.shutdown()
    threadPool.awaitTermination(Long.MaxValue, TimeUnit.DAYS)

    //释放资源
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

//    val filename = (conf \ "load" \ "filename").text.trim
//    val fileEncode = (conf \ "load" \ "fileEncode").text.trim
//    val columnSeperator = (conf \ "load" \ "columnSeperator").text.trim
    val hashNamePrefix = (conf \ "load" \ "hashNamePrefix").text.trim
    val hashColumnNames = (conf \ "load" \ "hashColumnNames").text.trim
    val hashSeperator = (conf \ "load" \ "hashSeperator").text.trim
    val valueColumnNames = (conf \ "load" \ "valueColumnNames").text.trim
    val valueSeperator = (conf \ "load" \ "valueSeperator").text.trim

    val fieldNames = (conf \ "load" \ "fieldNames").text.trim


    val batchLimit = (conf \ "load" \ "batchLimit").text.trim
    val batchLimitForRedis = (conf \ "load" \ "batchLimit.redis").text.trim

    val numThreads = (conf \ "load" \ "numThreads").text.trim
    val loadMethod = (conf \ "load" \ "method").text.trim

    val overwrite = (conf \ "load" \ "overwrite").text.trim
    val appendSeperator = (conf \ "load" \ "appendSeperator").text.trim


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

    props.put("load.from", from)
//    props.put("load.filename", filename)
//    props.put("load.fileEncode", fileEncode)
//    props.put("load.columnSeperator", columnSeperator)

    props.put("load.hashNamePrefix", hashNamePrefix)
    props.put("load.hashColumnNames", hashColumnNames)
    props.put("load.hashSeperator", hashSeperator)
    props.put("load.valueColumnNames", valueColumnNames)
    props.put("load.valueSeperator", valueSeperator)

    props.put("load.fieldNames", fieldNames)

    props.put("load.batchLimit", batchLimit)
    props.put("load.batchLimit.redis", batchLimitForRedis)
    props.put("load.numThreads", numThreads)
    props.put("load.method", loadMethod)

    props.put("load.overwrite", overwrite)
    props.put("load.appendSeperator", appendSeperator)

    println("="*80)
    props.list(System.out)
    println("="*80)

    props
  }

}
