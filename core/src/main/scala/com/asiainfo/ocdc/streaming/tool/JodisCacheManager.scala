package com.asiainfo.ocdc.streaming.tool

import com.asiainfo.ocdc.streaming.MainFrameConf
import com.wandoulabs.jodis.{JedisResourcePool, RoundRobinJedisPool}
import redis.clients.jedis.JedisPoolConfig

class JodisCacheManager extends RedisCacheManager{

  private val jedisPool:JedisResourcePool = {

    val JedisConfig = new JedisPoolConfig()
    JedisConfig.setMaxIdle(MainFrameConf.getInt("JedisMaxIdle"))
    JedisConfig.setMaxTotal(MainFrameConf.getInt("JedisMaxTotal"))
    JedisConfig.setMinEvictableIdleTimeMillis(MainFrameConf.getInt("JedisMEM"))
    JedisConfig.setTestOnBorrow(true)
    new RoundRobinJedisPool(MainFrameConf.get("zk"),
                            MainFrameConf.getInt("zkSessionTimeoutMs"),
                            MainFrameConf.get("zkpath"),
                            JedisConfig)
  }

  override def getResource = jedisPool.getResource
}
