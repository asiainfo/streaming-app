package com.asiainfo.ocdc.streaming.tool

import java.net.InetAddress

import com.asiainfo.ocdc.streaming.MainFrameConf
import redis.clients.jedis.{JedisPool, JedisPoolConfig}

class CodisCacheManager extends RedisCacheManager {

  private val jedisPool: JedisPool = {

    val JedisConfig = new JedisPoolConfig()
    JedisConfig.setMaxIdle(MainFrameConf.getInt("JedisMaxIdle"))
    JedisConfig.setMaxTotal(MainFrameConf.getInt("JedisMaxTotal"))
    JedisConfig.setMinEvictableIdleTimeMillis(MainFrameConf.getInt("JedisMEM"))
    JedisConfig.setTestOnBorrow(true)

    val hp:Tuple2[String,String]= {
      val proxylist = MainFrameConf.get("CodisProxy").split(",")
      val localip = InetAddress.getLocalHost.getHostAddress
      val proxymap = proxylist.map(args => (args.split(":")(0),args.split(":")(1))).toMap
      var rhost:String = null
      var rip:String = null

      proxymap.get(localip) match {
        case Some(value) =>  rhost = localip
          rip = value
        case None => rhost = proxylist(0).split(":")(0)
          rip = proxylist(0).split(":")(1)
      }
      (rhost,rip)
    }
    new JedisPool(JedisConfig,hp._1,hp._2.toInt,MainFrameConf.getInt("JedisTimeOut"))
  }

  override def getResource = jedisPool.getResource
}


