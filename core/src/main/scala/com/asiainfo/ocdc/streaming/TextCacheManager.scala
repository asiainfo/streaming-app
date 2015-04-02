package com.asiainfo.ocdc.streaming

object TextCacheManager extends CacheManager {

  private val CommonCacheMap:Map[String, Map[String, String]] = Map("LGMAP"-> Map("laccell" -> "2015map"))
  private val CommonCacheValue:Map[String,Map[String, String]] = Map("LGValue"-> Map("laccell" -> "2015value"))
  private val CommonCacheList:Map[String,List[String]] = Map("LGList" -> List("2015list"))

  private var HashCacheMap:Map[String, Map[String, String]] = null
  private var HashCacheString:Map[String,String] = null
  private var HashCacheList:Map[String,List[String]] = null


  override def getHashCacheList(key: String): List[String] = {
    HashCacheList.getOrElse(key,null)
  }

  override def getHashCacheMap(key: String): Map[String, String] = {
    HashCacheMap.getOrElse(key,null)
  }

  override def getHashCacheString(key: String): String = {
    HashCacheString.getOrElse(key,null)
  }

  override def setHashCacheList(key: String, value: List[String]) {
    HashCacheList = Map(key -> value)
  }

  override def setHashCacheMap(key: String, value: Map[String, String]) {
    HashCacheMap = Map(key -> value)
  }

  override def setHashCacheString(key: String, value: String) {
    HashCacheString = Map(key -> value)
  }

  override def getCommonCacheMap(key: String): Map[String, String] = {
    CommonCacheMap.getOrElse(key,null)
  }

  override def getCommonCacheList(key: String): List[String] = {
    CommonCacheList.getOrElse(key,null)
  }

  override def getCommonCacheValue(cacheName: String, key: String): String = {
    CommonCacheValue.getOrElse(cacheName,null).getOrElse(key,null)
  }

}
