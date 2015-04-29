package tools.redis

import org.slf4j.LoggerFactory

import scala.xml.XML

/**
 * Created by tsingfu on 15/4/27.
 */
object LoadFileIntoHashes {

  val logger = LoggerFactory.getLogger("LoadFile2Redis")

  def main(args: Array[String]): Unit = {

    if (args.length != 1) {
      println( """
                 | 使用方法说明:
                 | 适用情景：文本文件中每行记录可以作为1个hash/Map结构，加载到 redis 中每条记录使用 1个 hash 结构存储；
                 | 配置选项说明：导入时在 xml 配置文件中指定 redis, jedisPool, load的相关信息.
                 | <load>部分
                 |        <from>file</from> 指定从哪加载数据，因为目前只支持 file，可以缺省不配置 (TODO: 支持从db)
                 |        <filename>tools/src/test/resources/tools-redis-loadfile-test1.data</filename> 要加载文件的路径
                 |        <fileEncode>UTF-8</fileEncode> 加载文件的编码
                 |        <fieldSeperator>,</fieldSeperator> 每行记录字段分隔符
                 |
                 |        <hashNamePrefix>userinfo:imsi</hashNamePrefix> 加载时可以设置 redis中 hash名的前缀
                 |        <hashIdxes>0</hashIdxes> 设置加载时使用每行记录的哪个字段作为 hash名，可以指定多个
                 |        <hashSeperator>:</hashSeperator> 如果设置 hash名取多个字段的取的连接，可以设置分隔符
                 |
                 |        <keyNames>phoneNo, areaId</keyNames> 设置加载字段的属性name，作为每个 hash 中属性的属性名
                 |        <valueIdxes>1,2</valueIdxes>设置加载时加载哪些字段
                 |        <batchLimit>10000</batchLimit> 加载时使用 pipeline，设置pipeline 每个执行批次最大记录数
               """.stripMargin
      )
    }

    val confXmlFile = args(0)
    loadfile2hashes(confXmlFile)

  }

  def loadfile2hashes(confXmlFile: String): Unit ={
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

    val hashIdexes = (conf \ "load" \ "hashIdxes").text.split(",").map(_.trim.toInt)
    val hashSeperator = (conf \ "load" \ "hashSeperator").text.trim
    val hashNamePrefix = (conf \ "load" \ "hashNamePrefix").text.trim

    val keyNames = (conf \ "load" \ "keyNames").text.split(",").map(_.trim)
    val valueIdxes = (conf \ "load" \ "valueIdxes").text.split(",").map(_.trim.toInt)

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
      val hashName = (
              if (hashNamePrefix == "") ""
              else {
                hashNamePrefix + hashSeperator
              }
              ) + (for (idx <- hashIdexes) yield lineArray(idx)).mkString(hashSeperator)

      /*
            val hashKVs = fieldNames.zip(fieldIdxes).map(kv => {
              val (k, i) = kv
              logger.debug("hash Key = " + k + ", valueIdx = " + i)
              (k, lineArray(i))
            }).toMap
            RedisUtils.hset(jedis, hashName, hashKVs)
      */

      val hashKVs = keyNames.zip(valueIdxes).map(kv => {
        val (k, i) = kv
        pipeline.hset(hashName, k, lineArray(i))
      })

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
