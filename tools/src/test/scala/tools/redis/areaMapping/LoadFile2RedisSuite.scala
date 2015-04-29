package tools.redis.areaMapping

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.slf4j.LoggerFactory

/**
 * Created by tsingfu on 15/4/13.
 */
class LoadFile2RedisSuite extends FunSuite with BeforeAndAfter {

  val logger = LoggerFactory.getLogger("tools.redis.areaMapping.LoadFile2RedisSuite")

  test("test load area mapping into redis with file formatType 1") {
    // 文件格式: lac,ci,flag(说明：是否在某区域的标识 1在，0不在)
    val fileName = "tools/src/test/resources/tools-redis-loadareamap1.log"
    val serverPort = "redis1:6379"
    val mapKey = "areaMap1"

    LoadFile2Redis.load(fileName, serverPort, mapKey, 1, 1, 2, 3, "WLAN")
    assert(LoadFile2Redis.hget(serverPort, mapKey, "D301:0C06")=="WLAN")
    assert(LoadFile2Redis.hget(serverPort, mapKey, "548D:2B5D")=="non-WLAN")
  }

  test("test load area mapping into redis with file formatType 2"){
    // 文件格式: lac,ci,area(说明：区域名)
    val fileName="tools/src/test/resources/tools-redis-loadareamap2.log"
    val serverPort="redis1:6379"
    val mapKey="areaMap1"
    LoadFile2Redis.load(fileName, serverPort, mapKey, 2, 0, 1, 2)

    logger.info(LoadFile2Redis.hget(serverPort, mapKey, "25350:4882"))
    assert(LoadFile2Redis.hget(serverPort, mapKey, "25350:4882")=="学校,学校2,school,airport")

  }
}
