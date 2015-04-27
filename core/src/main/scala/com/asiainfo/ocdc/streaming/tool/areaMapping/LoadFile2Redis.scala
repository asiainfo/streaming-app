package com.asiainfo.ocdc.streaming.tool.areaMapping

import com.asiainfo.ocdc.streaming.{CacheManager, CacheFactory, MainFrameConf}

import redis.clients.jedis.Jedis

/**
 * Created by tsingfu on 15/4/13.
 */
object LoadFile2Redis {

  var cacheMgr: CacheManager = _

  def load(filename: String, serverPort: String, key: String): Unit = {

    cacheMgr = init_redis(serverPort)

    for (line <- scala.io.Source.fromFile(filename).getLines()) {
      val array = line.split(",")
      val lac_cell = array(2) + ":" + array(3)
      cacheMgr.setCommonCacheValue(key, lac_cell, "SchoolA")
    }
  }

  def init_redis(serverPort: String): CacheManager = {
    //test with CodisCacherManager
    MainFrameConf.set("DefaultCacheManager", "CodisCacheManager")
    MainFrameConf.set("CodisProxy", serverPort)
    MainFrameConf.set("JedisMEM", "10000")
    MainFrameConf.set("JedisMaxActive", "100")
    MainFrameConf.set("JedisMaxActive", "15")
    CacheFactory.getManager
  }

  def hget(serverPort: String, hashName: String, key:String): String ={
    val serverPortArray = serverPort.split(":")
    val host = serverPortArray(0)
    val port = serverPortArray(1).toInt

    val jedis = new Jedis(host, port)

    jedis.hget(hashName, key)
  }

  def load2(filename: String, serverPort: String, key: String): Unit = {

    val serverPortArray = serverPort.split(":")
    val host = serverPortArray(0)
    val port = serverPortArray(1).toInt

    val jedis = new Jedis(host, port)

    for (line <- scala.io.Source.fromFile(filename).getLines()) {
      val array = line.split(",")
      val lac_cell = array(2) + ":" + array(3)
      jedis.hset(key, lac_cell, "SchoolA")
    }

    jedis.close()
  }

  /**
   * 将基站与区域映射加载到指定 redis 中
   * 提供的文件支持2种格式：
   * 格式1：formatType=1, lacIdx, cellIdx, XXXareaFlag
   * 格式2：formatType=2, lacIdx, cellIdx, areaName
   *
   * @param filename 映射文件路径
   * @param serverPort redis/codis地址
   * @param key 加载到redis时，可以指定hash名
   */
  def load(filename: String, serverPort: String, key: String,
           formatType: Int,
           lacColIdx: Int = 0,
           cellColIdx: Int = 1,
           areaColIdx: Int = 2,
           areaName: String = null
                  ): Unit = {

    if (formatType == 1) {
      assert(areaName != null, "specified invalid value for parameter areaName while formatType=" + formatType)
    }
    val serverPortArray = serverPort.split(":")
    val host = serverPortArray(0)
    val port = serverPortArray(1).toInt

    val jedis = new Jedis(host, port)

    for (line <- scala.io.Source.fromFile(filename).getLines()) {
      val array = line.split(",")
      val lac_cell = array(lacColIdx) + ":" + array(cellColIdx)

      val areaFlag = array(areaColIdx)
      val newAreaName = if (formatType == 1) {
        if (areaFlag == "1" || areaFlag == "true") areaName else "non-" + areaName
      } else {
        if (areaFlag == "") null else areaFlag
      }
      if (newAreaName != null) {
        var fieldValue = jedis.hget(key, lac_cell)
        println("[debug] oldValue = " + fieldValue)
        val areaArray = if (fieldValue != null) fieldValue.split(",") else Array[String]()
        if (fieldValue == null) {
          fieldValue = newAreaName
          jedis.hset(key, lac_cell, fieldValue)
        } else {
          if (!areaArray.contains(newAreaName)) {
            fieldValue += "," + newAreaName
            jedis.hset(key, lac_cell, fieldValue)
          }
        }
      }
    }

    jedis.close()
  }


  def main(args: Array[String]): Unit = {

    if (args.length < 7) {
      println(
        """Invalid usage!
          |Usage: java -cp ... com.asiainfo.ocdc.streaming.tool.areaMapping.LoadFile2Redis
          |filePath redis-serverPort hashKey
          |formatType lacColIdx cellColIdx areaColIdx [areaName(when formatType=1)]
        """.stripMargin)
      System.exit(-1)
    }

    val filename = args(0)
    val serverPort = args(1)
    val hashKey = args(2)
    val formatType = args(3)
    assert(formatType == "1" || formatType == "2", "formatType valid values area [1,2]")
    if (formatType == "1") {
      assert(args.length == 8)
    } else {
      assert(args.length == 7)
    }
    val lacColIdx = args(4)
    val cellColIdx = args(5)
    val areaColIdx = args(6)
    val areaName = if (formatType == "1") args(7) else null

    assert(serverPort.contains(":"), "invalid format of redis serverPort, eg: redis-host-ip:6379")

    //load(filename, serverPort, hashKey)
    //    load2(filename, serverPort, hashKey)
    load(filename, serverPort, hashKey,
         formatType.toInt, lacColIdx.toInt, cellColIdx.toInt, areaColIdx.toInt, areaName)
  }

}
