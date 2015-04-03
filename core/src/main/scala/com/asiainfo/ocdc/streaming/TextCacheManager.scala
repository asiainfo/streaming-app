package com.asiainfo.ocdc.streaming
import scala.io.Source


object TextCacheManager extends CacheManager {

  private val CommonCacheMap:Map[String, Map[String, String]] = Map("LGMAP"-> Map("laccell" -> "2015map"))
  private val CommonCacheValue:Map[String,Map[String, String]] = Map("LGValue"-> Map("laccell" -> "2015value"))
  private var CommonCacheList = Map[String,List[String]]()

  private var HashCacheMap = Map[String, Map[String, String]]()
  private var HashCacheString = Map[String,String]()
  private var HashCacheList = Map[String,List[String]]()

  def init(){
    CommonCacheMapinit
    CommonCacheValueinit
    CommonCacheListinit("/home/ocdc/asiainfo/cachemap")
  }

 // getCommonCacheList(key: String): List[String]
 // key1, value11,value12,value13,value14,value15
 // key2, value21,value22,value23,value24,value25

  def CommonCacheListinit(filename: String) {
    for (line <- Source.fromFile(filename).getLines){
      var kv = line.split(",")
      CommonCacheList += (kv(0) -> kv.toList)
    }
  }

//  getCommonCacheMap(key: String): Map[String, String]
//  KeyA,keya1,valuea1,keya2,valuea2,keya3,valuea3
//  KeyB,keyb1,valueb1,keyb2,valueb2,keyb3,valueb3

  def CommonCacheMapinit() {

  }

  //getCommonCacheValue(cacheName: String, key: String): String
  //groupA,lacAcellA,AAAA
  //groupA,lacBcellB,BBBB

  def CommonCacheValueinit() {

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
