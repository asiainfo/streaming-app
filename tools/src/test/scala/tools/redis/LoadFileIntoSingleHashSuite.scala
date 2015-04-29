package tools.redis

import org.scalatest.FunSuite

import scala.xml.XML

/**
 * Created by tsingfu on 15/4/29.
 */
class LoadFileIntoSingleHashSuite extends FunSuite{

  test("简单测试"){
    val confXmlFile = "tools/conf/tools-redis-loadfile2singlehash-test.xml"
    LoadFileIntoSingleHash.loadfile2singlehash(confXmlFile)


    val conf = XML.load(confXmlFile)
    val serverPort = (conf \ "redis" \ "hostPort").text.trim

    //    val host = (conf \ "jedis" \ "host").text.trim
    //    val port = (conf \ "jedis" \ "port").text.trim.toInt
    val serverPortArray = serverPort.split(":")
    val host = serverPortArray(0)
    val port = serverPortArray(1).toInt

    val hashName = (conf \ "load" \ "hashName").text.trim
    val dbNum = (conf \ "redis" \ "database").text.trim.toInt

    assert(RedisUtils.hget(serverPort, dbNum, hashName, "548D:2B5D")=="0,21645")
  }
}
