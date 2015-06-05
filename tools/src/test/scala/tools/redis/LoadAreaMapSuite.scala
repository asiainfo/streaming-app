package tools.redis

import org.scalatest.FunSuite

/**
 * Created by tsingfu on 15/4/28.
 */
class LoadAreaMapSuite extends FunSuite {

  test("测试 格式1 的区域文件 导入"){
    //将 tools/src/test/resources/tools-redis-loadareamap1.log 导入 reids 指定hashName 的hash中
    // 导入时 field 取第5列(lac)，第6列(cell)拼接值(分号分隔)，value 取值根据第3列的值，为1或true，设置指定值(如 'WLAN', 其他 'non-WLAN')
    // 注意 ColIdx 是以0为基础的索引

    val filename="tools/src/test/resources/tools-redis-loadareamap1.log"
    val serverPort = "redis1:6379"
    val dbNum = 0
    val hashName = "areaMap1"
    val formatType = 1
    val colSep = ","
    val lacColIdx = 4
    val cellColIdx = 5
    val areaColIdx = 3
    val areaName = "WLAN"
    LoadAreaMap.loadAreaMap(filename, serverPort, dbNum, hashName, formatType, colSep, lacColIdx, cellColIdx, areaColIdx, areaName)

    assert(RedisUtils.hget(serverPort, dbNum, hashName, "D301:0C06")=="WLAN")
  }

  test("测试 格式2 的区域文件 导入"){
    //将 tools/src/test/resources/tools-redis-loadareamap2.log 导入 reids 指定hashName的hash中
    // 导入时 field 取第1列(lac)，第2列(cell)拼接值，value 取第3列值

    val filename="tools/src/test/resources/tools-redis-loadareamap2.log"
    val serverPort = "redis1:6379"
    val dbNum = 0
    val hashName = "areaMap2"
    val formatType = 2
    val colSep = ","
    val lacColIdx = 0
    val cellColIdx = 1
    val areaColIdx = 2
    val areaName = null
    LoadAreaMap.loadAreaMap(filename, serverPort, dbNum, hashName, formatType, colSep, lacColIdx, cellColIdx, areaColIdx, areaName)

    assert(RedisUtils.hget(serverPort, dbNum, hashName, "D301:0C06")=="学校,学校2")
    assert(RedisUtils.hget(serverPort, dbNum, hashName, "548D:2B5D")=="school,airport")
  }

}
