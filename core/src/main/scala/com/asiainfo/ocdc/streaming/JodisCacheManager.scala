package com.asiainfo.ocdc.streaming

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import com.wandoulabs.jodis.{JedisResourcePool, RoundRobinJedisPool}

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
