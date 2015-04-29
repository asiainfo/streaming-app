package tools.redis

import redis.clients.jedis.Jedis

/**
 * Created by tsingfu on 15/4/27.
 */
object LoadAreaMap {

  def main(args: Array[String]): Unit = {

    if (args.length < 9) {
      println("args.length = " + args.length + "\n" +
              """
                | Usage: java -cp ... tools.redis.LoadAreaMap
                | filePath redis-serverPort dbNum hashName
                | formatType colSeperator lacColIdx cellColIdx areaColIdx [areaName(when formatType=1)]
                |
              """.stripMargin)
      System.exit(-1)
    }

    val filename = args(0)
    val serverPort = args(1)
    val dbNum = args(2).toInt
    val hashName = args(3)
    val formatType = args(4)
    val colSeperator = args(5)
    assert(formatType == "1" || formatType == "2", "formatType valid values area [1,2]")

    val lacColIdx = args(6)
    val cellColIdx = args(7)
    val areaColIdx = args(8)
    val areaName = if (formatType == "1") args(9) else null

    assert(serverPort.contains(":"), "invalid format of redis serverPort, eg: redis-host-ip:6379")

    loadAreaMap(filename, serverPort, dbNum, hashName,
      formatType.toInt, colSeperator, lacColIdx.toInt, cellColIdx.toInt, areaColIdx.toInt, areaName)
  }

  /**
   * 将基站与区域映射加载到指定 redis 中
   * 提供的文件支持2种格式：
   * 格式1：formatType=1, lacIdx, cellIdx, XXXareaFlag
   * 格式2：formatType=2, lacIdx, cellIdx, areaName
   *
   * @param filename 映射文件路径
   * @param serverPort redis/codis地址
   * @param hashName 加载到redis时，可以指定hash名
   */
  def loadAreaMap(filename: String, serverPort: String, dbNum: Int, hashName: String,
                  formatType: Int,
                  colSep: String = ",",
                  lacColIdx: Int = 0,
                  cellColIdx: Int = 1,
                  areaColIdx: Int = 2,
                  areaName: String = null
                         ): Unit = {

    val startMS = System.currentTimeMillis()

    if (formatType == 1) {
      assert(areaName != null, "specified invalid value for parameter areaName while formatType=" + formatType)
    }
    val serverPortArray = serverPort.split(":")
    val host = serverPortArray(0)
    val port = serverPortArray(1).toInt

    val password = if (serverPortArray.length == 3) serverPortArray(2) else null

    for (line <- scala.io.Source.fromFile(filename, "UTF-8").getLines()) {
      val array = line.split(colSep)
      val lac_cell = array(lacColIdx) + ":" + array(cellColIdx)

      val areaFlag = array(areaColIdx)
      val newAreaName = if (formatType == 1) {
        if (areaFlag == "1" || areaFlag == "true") areaName else "non-" + areaName
      } else {
        if (areaFlag == "") null else areaFlag
      }

      val jedis = new Jedis(host, port)
      jedis.select(dbNum)
      if (password != null) jedis.auth(password)

      if (newAreaName != null) {
        var fieldValue = jedis.hget(hashName, lac_cell)
        val areaArray = if (fieldValue != null) fieldValue.split(",") else Array[String]()
        if (fieldValue == null) {
          fieldValue = newAreaName
          jedis.hset(hashName, lac_cell, fieldValue)
        } else {
          if (!areaArray.contains(newAreaName)) {
            fieldValue += "," + newAreaName
            jedis.hset(hashName, lac_cell, fieldValue)
          }
        }
      }
      jedis.close()
    }
    val elapsedMS = System.currentTimeMillis() - startMS
    println("[info] load file " + filename + " finished. " +
            "elspsed = " + elapsedMS + " ms, " + elapsedMS / 1000.0 + " s, " + elapsedMS / 1000.0 / 60.0 + " min")
  }
}
