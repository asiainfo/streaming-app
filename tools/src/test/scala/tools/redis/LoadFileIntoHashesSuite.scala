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
}
