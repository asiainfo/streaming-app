package tools.redis

import org.slf4j.LoggerFactory

import scala.xml.XML

/**
 * Created by tsingfu on 15/4/28.
 */
object LoadFileIntoSingleHash {

  val logger = LoggerFactory.getLogger("tools.redis.LoadFileIntoSingleHash")

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println("WARN: args.length = " + args.length + "\n" + "You should specify a confXmlFile")
      System.exit(-1)
    }

    val confXmlFile = args(0)

    loadfile2singlehash(confXmlFile)
  }

  def loadfile2singlehash(confXmlFile: String): Unit ={
    val conf = XML.load(confXmlFile)
    val serverPort = (conf \ "redis" \ "hostPort").text.trim

    //    val host = (conf \ "jedis" \ "host").text.trim
    //    val port = (conf \ "jedis" \ "port").text.trim.toInt
    val serverPortArray = serverPort.split(":")
    val host = serverPortArray(0)
    val port = serverPortArray(1).toInt

    val dbNum = (conf \ "redis" \ "database").text.trim.toInt
    val timeout = (conf \ "redis" \ "timeout").text.trim.toInt
    val passwd = (conf \ "redis" \ "password").text.trim
    val password = if (passwd == "" || passwd == null) null else passwd

    val maxIdel = (conf \ "jedisPool" \ "maxIdel").text.trim.toInt
    val maxTotal = (conf \ "jedisPool" \ "maxTotal").text.trim.toInt
    val minIdel = (conf \ "jedisPool" \ "minIdel").text.trim.toInt

    val from = (conf \ "load" \ "from").text.trim
    val filename = (conf \ "load" \ "filename").text.trim
    val fileEncode = (conf \ "load" \ "fileEncode").text.trim
    val fieldSeperator = (conf \ "load" \ "fieldSeperator").text.trim

    val hashName = (conf \ "load" \ "hashName").text.trim
    val keyIdxes = (conf \ "load" \ "keyIdxes").text.split(",").map(_.trim.toInt)
    val keySeperator = (conf \ "load" \ "keySeperator").text.trim
    val valueIdxes = (conf \ "load" \ "valueIdxes").text.split(",").map(_.trim.toInt)
    val valueSeperator = (conf \ "load" \ "valueSeperator").text.trim

    val batchLimit = (conf \ "load" \ "batchLimit").text.trim.toInt

    val startMS = System.currentTimeMillis()

    // 初始化 jedisPoolConfig
    val jedisPoolConfig = RedisUtils.init_jedisPoolConfig(maxIdel, maxTotal, minIdel)

    // 获取 jedisPool
    val jedisPool = RedisUtils.get_jedisPool(jedisPoolConfig, host, port, timeout, password, dbNum)

    var jedis = jedisPool.getResource
    var pipeline = jedis.pipelined()

    //加载文件
    var positionIdx = 0
    var batchNum = 0
    for (line <- scala.io.Source.fromFile(filename, fileEncode).getLines()) {
      val lineArray = line.split(fieldSeperator).map(_.trim)

      val key = (for(idx <- keyIdxes) yield lineArray(idx)).mkString(keySeperator)
      val value = (for(idx <- valueIdxes) yield lineArray(idx)).mkString(valueSeperator)

      pipeline.hset(hashName, key, value)
      batchNum += 1
      positionIdx += 1
      if(batchNum == batchLimit){
        pipeline.sync()
        batchNum = 0
        //2.5.0 later, need not returnResource, JedisException: Could not return the resource to the pool
        //      jedisPool.returnResourceObject(jedis)

        jedis = jedisPool.getResource
        pipeline = jedis.pipelined()
      }
    }
    pipeline.sync()

    jedisPool.close()

    val elapsedMS = System.currentTimeMillis() - startMS
    println("[info] load file " + filename + " finished. " +
            "records = "+positionIdx+
            ", elspsed = " + elapsedMS + " ms, " + elapsedMS / 1000.0 + " s, " + elapsedMS / 1000.0 / 60.0 + " min" +
            ", speed = " + positionIdx / (elapsedMS / 1000.0) +" records/s" )
  }
}
