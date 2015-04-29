package tools.redis

import org.scalatest.FunSuite

/**
 * Created by tsingfu on 15/4/28.
 */
class LoadAreaMapSuite extends FunSuite {

  test("测试 格式1 的区域文件 导入"){
    //将 tools/src/resources/WLAN-areaMap.csv 导入 reids，导入时 第2列，第3列拼接在一起作为 hashName 的属性名，第3列作为属性值

    val filename="tools/src/test/resources/tools-redis-loadareamap1.log"
    val serverPort = "redis1:6379"
    val dbNum = 0
    val hashName = "areaMap1"
    val formatType = 1
    val colSep = ","
    val lacColIdx = 1
    val cellColIdx = 2
    val areaColIdx = 3
    val areaName = "WLAN"
    LoadAreaMap.loadAreaMap(filename, serverPort, dbNum, hashName, formatType, colSep, lacColIdx, cellColIdx, areaColIdx, areaName)

    assert(RedisUtils.hget(serverPort, dbNum, hashName, "D301:0C06")=="WLAN")
    assert(RedisUtils.hget(serverPort, dbNum, hashName, "548D:2B5D")=="non-WLAN")
  }

  test("测试 格式2 的区域文件 导入"){
    //将 tools/src/resources/WLAN-areaMap.csv 导入 reids，导入时 第2列，第3列拼接在一起作为 hashName 的属性名，第3列作为属性值

    val filename="tools/src/test/resources/tools-redis-loadareamap2.log"
    val serverPort = "redis1:6379"
    val dbNum = 0
    val hashName = "areaMap1"
    val formatType = 2
    val colSep = ","
    val lacColIdx = 0
    val cellColIdx = 1
    val areaColIdx = 2
    val areaName = null
    LoadAreaMap.loadAreaMap(filename, serverPort, dbNum, hashName, formatType, colSep, lacColIdx, cellColIdx, areaColIdx, areaName)

    assert(RedisUtils.hget(serverPort, dbNum, hashName, "D301:0C06")=="WLAN")
    assert(RedisUtils.hget(serverPort, dbNum, hashName, "548D:2B5D")=="non-WLAN")
  }

}
