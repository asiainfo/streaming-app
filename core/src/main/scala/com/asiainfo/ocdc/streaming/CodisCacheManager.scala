package com.asiainfo.ocdc.streaming

import redis.clients.jedis.Jedis

/**
 * Created by tianyi on 3/30/15.
 */

object CodisCacheManager extends CacheManager {

  private val jedis:Jedis = new Jedis("localhost")

  override def getHashCacheList(key: String): List[String] = ???

  override def getHashCacheMap(key: String): Map[String, String] = ???

  override def setHashCacheString(key: String, value: String): Unit = ???

  override def setHashCacheList(key: String, value: List[String]): Unit = ???

  override def getCommonCacheValue(cacheName: String, key: String): String = ???

  override def getHashCacheString(key: String): String = ???

  override def setHashCacheMap(key: String, value: Map[String, String]): Unit = ???

  override def getCommonCacheMap(key: String): Map[String, String] = ???

  override def getCommonCacheList(key: String): List[String] = ???
}
