package tools.redis

import org.scalatest.FunSuite

import scala.xml.XML

/**
 * Created by tsingfu on 15/4/29.
 */
class LoadFileIntoHashesSuite extends FunSuite {

  test("基本测试"){
    val confXmlFile = "tools/conf/tools-redis-loadfile2hashes-test.xml"
    LoadFileIntoHashes.loadfile2hashes(confXmlFile)


    val conf = XML.load(confXmlFile)
    val serverPort = (conf \ "redis" \ "hostPort").text.trim

    //    val host = (conf \ "jedis" \ "host").text.trim
    //    val port = (conf \ "jedis" \ "port").text.trim.toInt
    val serverPortArray = serverPort.split(":")
    val host = serverPortArray(0)
    val port = serverPortArray(1).toInt

    val hashNamePrefix = (conf \ "load" \ "hashNamePrefix").text.trim
    val hashSeperator = (conf \ "load" \ "hashSeperator").text.trim
    val key1 = "imsi2"
    val hashName = if(hashNamePrefix == "") key1 else hashNamePrefix +hashSeperator+ key1
    val dbNum = (conf \ "redis" \ "database").text.trim.toInt

    assert(RedisUtils.hget(serverPort, dbNum, hashName, "phoneNo")=="13633719601")
    assert(RedisUtils.hget(serverPort, dbNum, hashName, "areaId")=="济南")
  }

//
//  test("2测试 Load2HashesThread"){
//
//    val confXmlFile = args(0)
//
//    val props = LoadFile2Hashes.init_props_fromXml(confXmlFile)
//    val servers = props.getProperty("redis.servers").trim
//    val database = props.getProperty("redis.database", "0").trim.toInt
//    val timeout = props.getProperty("redis.timeout").trim.toInt
//    val password = props.getProperty("redis.password")
//
//    val maxTotal = props.getProperty("jedisPool.maxTotal").trim.toInt
//    val maxIdle = props.getProperty("jedisPool.maxIdle").trim.toInt
//    val minIdle = props.getProperty("jedisPool.minIdle").trim.toInt
//
//    val from = props.getProperty("load.from").trim
//    val filename = props.getProperty("load.filename").trim
//    val fileEncode = props.getProperty("load.fileEncode").trim
//    val columnSeperator = props.getProperty("load.columnSeperator").trim
//
//    val hashNamePrefix = props.getProperty("load.hashNamePrefix").trim
//    val hashIdxes = props.getProperty("load.hashIdxes").trim.split(",").map(_.trim.toInt)
//    val hashSeperator = props.getProperty("load.hashSeperator").trim
//
//    val valueIdxes = props.getProperty("load.valueIdxes").trim.split(",").map(_.trim.toInt)
//    val fieldNames = props.getProperty("load.fieldNames").trim.split(",").map(_.trim)
//
//    val batchLimit = props.getProperty("load.batchLimit").trim.toInt
//    val batchLimitForRedis = props.getProperty("load.batchLimit.redis").trim.toInt
//
//    val numThreads = props.getProperty("load.numThreads").trim.toInt
//    val loadMethod = props.getProperty("load.method").trim
//
//    val overwrite = props.getProperty("load.overwrite").trim.toBoolean
//    val appendSeperator = props.getProperty("load.appendSeperator").trim
//
//    val startMS = System.currentTimeMillis()
//    var runningTime: Long = -1
//
//    // 初始化 jedisPool, jedis, pipeline
//    val jedisPools = servers.split(",").map(server=>{
//      val hostPort = server.split(":").map(_.trim)
//      val host = hostPort(0)
//      val port = hostPort(1).toInt
//      println("host = " + host +", port ="+port)
//      RedisUtils.init_jedisPool(host, port, timeout, database, password, maxTotal, maxIdle, minIdle)
//    })
//    val numPools = jedisPools.length
//    val jedises = jedisPools.map(_.getResource)
//    val pipelines = jedises.map(_.pipelined)
//
//    //获取文件记录数
//    val fileRecordsNum = scala.io.Source.fromFile(filename, fileEncode).getLines().length
//
//    var numProcessed = 0
//    var numInBatch = 0
//    var numBatches = 0
//    var batchArrayBuffer: ArrayBuffer[String] = null
//
//    // 初始化线程池
//    val threadPool: ExecutorService = Executors.newFixedThreadPool(numThreads)
//
//    def jedisPoolId = numBatches % numPools
//  }
}
