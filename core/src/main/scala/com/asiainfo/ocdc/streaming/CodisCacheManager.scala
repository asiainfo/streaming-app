package com.asiainfo.ocdc.streaming

import java.net.InetAddress
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

class CodisCacheManager extends RedisCacheManager {

  private var jedis: Jedis = null

  private val jedisPool: JedisPool = {

    val JedisConfig = new JedisPoolConfig()
    JedisConfig.setMaxIdle(MainFrameConf.getInt("JedisMaxIdle"))
    JedisConfig.setMaxActive(MainFrameConf.getInt("JedisMaxActive"))
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
    new JedisPool(JedisConfig,hp._1,hp._2.toInt)
  }

  override def exec[U](f: Jedis => U): U = {
    try{
      jedis = jedisPool.getResource
      f(jedis)
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis).asInstanceOf[U]
    }finally {
      if (jedis != null) jedisPool.returnResource(jedis)
    }
  }

  def returnResource(pool:JedisPool, jedis:Jedis){
    if(jedis != null){
      pool.returnResource(jedis)
    }
  }

}
