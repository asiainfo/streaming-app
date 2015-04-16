package com.asiainfo.ocdc.streaming.tool.areaMapping

import com.asiainfo.ocdc.streaming.{CacheManager, CacheFactory, MainFrameConf}

import redis.clients.jedis.Jedis

/**
 * Created by tsingfu on 15/4/13.
 */
object LoadFile2Redis {

	var cacheMgr : CacheManager = _

	def load(filename: String, serverPort: String, key: String): Unit = {

		cacheMgr = init_redis(serverPort)

		for(line <- scala.io.Source.fromFile(filename).getLines){
//			println("[debug] line="+line)
			val array=line.split(",")
			val lac_cell=array(2)+":"+array(3)
			cacheMgr.setCommonCacheValue(key, lac_cell, "SchoolA")
		}
	}

	def init_redis(serverPort: String): CacheManager ={
		//test with CodisCacherManager
		MainFrameConf.set("DefaultCacheManager", "CodisCacheManager")
		MainFrameConf.set("CodisProxy",serverPort)
		MainFrameConf.set("JedisMEM","10000")
		MainFrameConf.set("JedisMaxActive","100")
		MainFrameConf.set("JedisMaxActive","15")
		CacheFactory.getManager

	}


	def load2(filename: String, serverPort: String, key: String): Unit = {

		val serverPortArray = serverPort.split(":")
		val host = serverPortArray(0)
		val port = serverPortArray(1).toInt

		val jedis = new Jedis(host, port)

		for(line <- scala.io.Source.fromFile(filename).getLines){
			val array=line.split(",")
			val lac_cell=array(2)+":"+array(3)
			jedis.hset(key, lac_cell, "SchoolA")
		}

		jedis.close()
	}

	def main(args:Array[String]): Unit ={

		if(args.length !=3){
			println(
				"""Invalid usage!
					|Usage: java -cp ... com.asiainfo.ocdc.streaming.tool.areaMapping.LoadFile2Redis <file> <redis-serverPort> <hashKey>
				""".stripMargin)
			System.exit(-1)
		}

		val filename:String = args(0)
		val serverPort:String = args(1)
		val hashKey:String = args(2)

		assert(serverPort.contains(":"), "invalid format of redis serverPort, eg: redis-host-ip:6379")

//		load(filename, serverPort, hashKey)
		load2(filename, serverPort, hashKey)
	}

}
