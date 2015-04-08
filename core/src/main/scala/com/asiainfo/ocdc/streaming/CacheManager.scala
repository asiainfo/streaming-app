package com.asiainfo.ocdc.streaming

import scala.collection.mutable.Map

trait CacheManager {

  def getHashCacheList(key: String): List[String]

  def getHashCacheMap(key: String): Map[String, String]

  def getHashCacheString(key: String): String

  def setHashCacheList(key: String, value: List[String])

  def setHashCacheMap(key: String, value: Map[String, String])

  def setHashCacheString(key: String, value: String)

  def getCommonCacheMap(key: String): Map[String, String]

  def getCommonCacheList(key: String): List[String]

  def getCommonCacheValue(cacheName: String, key: String): String

  def setByteCacheString(key: String, value: String)

  def getByteCacheString(key: String): List[String]

  def setMultiCache(keysvalues: Map[String, Any])

  def getMultiCacheByKeys(keys: List[String]): Map[String, Any]

}
