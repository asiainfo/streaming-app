package com.asiainfo.ocdc.streaming

import scala.io.Source
import scala.collection.mutable.Map

object TextCacheManager extends CacheManager {

  private var CommonCacheMap = Map[String, Map[String, String]]()
  private var CommonCacheValue = Map[String,Map[String, String]]()
  private var CommonCacheList = Map[String,List[String]]()

  private var HashCacheMap = Map[String, Map[String, String]]()
  private var HashCacheString = Map[String,String]()
  private var HashCacheList = Map[String,List[String]]()
  private var ByteCacheString = Map[String,String]()
  private var MultiCacheString = Map[String,String]()

  private val delim = ":"

  def init() {
    CommonCacheMapinit("core/src/main/resources/CacheMapFile")
    CommonCacheValueinit("core/src/main/resources/CacheValueFile")
    CommonCacheListinit("core/src/main/resources/CacheListFile")
  }


  def CommonCacheListinit(filename: String) {
    for (line <- Source.fromFile(filename).getLines){
      var kv = line.split(delim)
      CommonCacheList += (kv(0) -> kv(1).split(",").toList)
    }
    // TODO: optimize this code
  }

  def CommonCacheMapinit(filename: String) {
    for (line <- Source.fromFile(filename).getLines){
      var maptmp = Map[String, String]()
      val maparray = line.split(delim)
      for(i <- 1 to maparray.length-1){
        maptmp += (maparray(i).split(",")(0) -> maparray(i).split(",")(1))
      }
      CommonCacheMap += (maparray(0) -> maptmp)
    }
    // TODO: optimize this code
  }

  def CommonCacheValueinit(filename: String) {

    for (line <- Source.fromFile(filename).getLines){
      val cachevalue = line.split(delim)

      CommonCacheValue.get(cachevalue(0)) match {
        case Some(value) => value += (cachevalue(1)->cachevalue(2))
        case None =>  CommonCacheValue += (cachevalue(0) -> Map((cachevalue(1)->cachevalue(2))))
      }
    }
    // TODO: optimize this code
  }

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
    HashCacheList += (key -> value)
  }

  override def setHashCacheMap(key: String, value: Map[String, String]) {
    HashCacheMap += (key -> value)
  }

  override def setHashCacheString(key: String, value: String) {
    HashCacheString += (key -> value)
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

  def setCommonCacheMap(key: String, value: Map[String, String]) = {
    CommonCacheMap += key -> value
  }

  def setCommonCacheList(key: String, value: List[String]) = {
    CommonCacheList += key -> value
  }

  def setCommonCacheValue(cacheName: String, key: String, value: String) = {
    val temp = CommonCacheValue.getOrElse(cacheName, Map[String, String]())
    temp.put(key, value)
    CommonCacheValue += cacheName -> temp
  }

  override def setByteCacheString(key: String, value: String) {
    //TODO
  }

  override def getByteCacheString(key: String): List[String] = {
    //TODO
    null
  }

  override def setMultiCache(keysvalues: Map[String, Any]) {
    //TODO
  }

  override def getMultiCacheByKeys(keys: List[String]): Map[String, Any] = {
    null
  }

}
