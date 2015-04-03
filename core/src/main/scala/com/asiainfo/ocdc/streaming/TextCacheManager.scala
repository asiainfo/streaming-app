package com.asiainfo.ocdc.streaming

import scala.io.Source

object TextCacheManager extends CacheManager {

  private var CommonCacheMap = Map[String, Map[String, String]]()
  private var CommonCacheValue = Map[String,Map[String, String]]()
  private var CommonCacheList = Map[String,List[String]]()

  private var HashCacheMap = Map[String, Map[String, String]]()
  private var HashCacheString = Map[String,String]()
  private var HashCacheList = Map[String,List[String]]()
  private val delim = ":"

  def init(){
    CommonCacheMapinit("/home/ocdc/asiainfo/cachemap")
    CommonCacheValueinit("/home/ocdc/asiainfo/cachevalue")
    CommonCacheListinit("/home/ocdc/asiainfo/cachelist")
  }

  /*
    inputformat:
        key1:value11,value12,value13,value14,value15
        key2:value21,value22,value23,value24,value25
    outputformat:
        Map(key1 -> List(value11,value12,value13,value14,value15))
  */

  def CommonCacheListinit(filename: String) {
    for (line <- Source.fromFile(filename).getLines){
      var kv = line.split(delim)
      CommonCacheList += (kv(0) -> kv(1).split(",").toList)
    }
    // TODO: optimize this code
  }

  /*
    input format:
        KeyA:keya1,valuea1:keya2,valuea2:keya3,valuea3
        KeyB:keyb1,valueb1:keyb2,valueb2:keyb3,valueb3
    output format:
        Map(KeyA -> Map(keya1 -> valuea1, keya2 -> valuea2, keya3 -> valuea3))
  */

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

  /*
  input format:
      groupA,lacAcellA,AAAA
      groupA,lacBcellB,BBBB
      groupB,lacBcellA,AAAA
  output format:
      Map(groupA -> Map(lacAcellA -> AAAA, lacBcellB -> BBBB), groupB -> Map(lacBcellA -> AAAA))
*/

  def CommonCacheValueinit(filename: String) {
    CommonCacheValue +=
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

}
