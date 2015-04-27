package com.asiainfo.ocdc.streaming.tool.areaMapping

import com.asiainfo.ocdc.streaming.{CacheFactory, MainFrameConf}
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by tsingfu on 15/4/13.
 */
class LoadFile2RedisSuite extends FunSuite with BeforeAndAfter {

  //test with CodisCacherManager
  MainFrameConf.set("DefaultCacheManager", "CodisCacheManager")
  MainFrameConf.set("CodisProxy", "redis1:6379")
  MainFrameConf.set("JedisMEM", "10000")
  MainFrameConf.set("JedisMaxActive", "100")
  MainFrameConf.set("JedisMaxActive", "15")
  val cacheMgr = CacheFactory.getManager

  test("test load area mapping into redis") {
    //		val fileName="/data01/data/datadir_github/ai-projects/streaming-app-201503/testdata/mc-test.log"
    val fileName = "/data01/data/datadir_work/ai/projects/products/streaming-201503/testdata/WLAN-areaMap.csv"
    val serverPort = "redis1:6379"
    val mapKey = "areaMap1"

    //		LoadFile2Redis.load(fileName, serverPort, mapKey)
    //		LoadFile2Redis.load2(fileName, serverPort, mapKey)
    //		assert(cacheMgr.getCommonCacheMap(mapKey).size==10)
    LoadFile2Redis.load(fileName, serverPort, mapKey, 1, 1, 2, 3, "WLAN")
  }

  test("test load area mapping into redis with file formatType 2"){
    val fileName="/data01/data/datadir_github/ai-projects/streaming-app-201503/testdata/mc-test2.log"
    val serverPort="redis1:6379"
    val mapKey="areaMap1"
    LoadFile2Redis.load(fileName, serverPort, mapKey, 2, 0, 1, 2)

//    println(LoadFile2Redis.hget(serverPort, mapKey, "25350:4882"))
//    assert(LoadFile2Redis.hget(serverPort, mapKey, "25350:4882")=="学校")

  }
}
