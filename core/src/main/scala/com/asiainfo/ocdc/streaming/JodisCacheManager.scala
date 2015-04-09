package com.asiainfo.ocdc.streaming

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import com.wandoulabs.jodis.{JedisResourcePool, RoundRobinJedisPool}

class JodisCacheManager extends RedisCacheManager{
  private var jedis: Jedis = null

  private val jedisPool:JedisResourcePool = {

    val JedisConfig = new JedisPoolConfig()
    JedisConfig.setMaxIdle(MainFrameConf.getInt("JedisMaxIdle"))
    JedisConfig.setMaxActive(MainFrameConf.getInt("JedisMaxActive"))
    JedisConfig.setMinEvictableIdleTimeMillis(MainFrameConf.getInt("JedisMEM"))
    JedisConfig.setTestOnBorrow(true)
    new RoundRobinJedisPool(MainFrameConf.get("zk"),
                            MainFrameConf.getInt("zkSessionTimeoutMs"),
                            MainFrameConf.get("zkpath"),
                            JedisConfig)
  }

  override def exec[U](f: Jedis => U): U = {
    try{
      jedis = jedisPool.getResource
      f(jedis)
    }catch{
      case e:Exception => e.printStackTrace()
      null.asInstanceOf[U]
       // jedisPool.returnBrokenResource(jedis).asInstanceOf[U]
    }finally {
     // if (jedis != null) jedisPool.returnResource(jedis)
    }
  }

  def returnResource(pool:JedisPool, jedis:Jedis){
    if(jedis != null){
      pool.returnResource(jedis)
    }
  }
}
