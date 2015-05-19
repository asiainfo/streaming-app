package com.asiainfo.ocdc.streaming.tool

import java.nio.ByteBuffer
import java.util.ArrayList

import redis.clients.jedis.Jedis

import scala.collection.convert.wrapAsJava.mapAsJavaMap
import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.Map

trait CacheManager extends org.apache.spark.Logging {

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

  def setCommonCacheValue(cacheName: String, key: String, value: String)
}


abstract class RedisCacheManager extends CacheManager {

  private val currentJedis = new ThreadLocal[Jedis] {
    override def initialValue = getResource
  }

  private val currentKryoTool = new ThreadLocal[KryoSerializerStreamAppTool] {
    override def initialValue = new KryoSerializerStreamAppTool
  }

  //  private val currentJedis = getResource
  final def openConnection = currentJedis.set(getResource)

  final def getConnection = {
    val curr_jedis = currentJedis.get()
    logInfo(" Current Jedis : " + curr_jedis)
    curr_jedis
  }

  final def getKryoTool = currentKryoTool.get()

  //final def getConnection = currentJedis

  final def closeConnection = {
    getConnection.close()
    currentJedis.remove()
  }

  protected def getResource: Jedis

  override def getHashCacheList(key: String): List[String] = {
    getConnection.lrange(key, 0, -1).toList
  }

  override def getHashCacheMap(key: String): Map[String, String] = {
    getConnection.hgetAll(key)
  }

  override def setHashCacheString(key: String, value: String): Unit = {
    getConnection.set(key, value)
  }

  override def getCommonCacheValue(cacheName: String, key: String): String = {
    getConnection.hget(cacheName, key)
  }

  override def getHashCacheString(key: String): String = {
    getConnection.get(key)
  }

  override def getCommonCacheMap(key: String): Map[String, String] = {
    getConnection.hgetAll(key)
  }

  override def getCommonCacheList(key: String): List[String] = {
    getConnection.lrange(key, 0, -1).toList
  }

  override def setHashCacheMap(key: String, value: Map[String, String]): Unit = {
    getConnection.hmset(key, mapAsJavaMap(value))
  }

  override def setHashCacheList(key: String, value: List[String]): Unit = {
    value.map { x => getConnection.rpush(key, x)}
  }

  override def setByteCacheString(key: String, value: String) {
    //TODO
  }

  override def getByteCacheString(key: String): List[String] = {
    null
  }

  override def setMultiCache(keysvalues: Map[String, Any]) {
    val t1 = System.currentTimeMillis()
    val seqlist = new ArrayList[Array[Byte]]()
    val it = keysvalues.keySet.iterator
    while (it.hasNext) {
      val elem = it.next()
      seqlist.add(elem.getBytes)
      seqlist.add(getKryoTool.serialize(keysvalues(elem)).array())
    }
    val r = getConnection.mset(seqlist.toIndexedSeq: _*)
    System.out.println("MSET " + keysvalues.size + " key cost " + (System.currentTimeMillis() - t1))
    r
  }

  override def getMultiCacheByKeys(keys: List[String]): Map[String, Any] = {
    val t1 = System.currentTimeMillis()
    val multimap = Map[String, Any]()
    val bytekeys = keys.map(x => x.getBytes).toSeq
    var i = 0
    val cachedata = getConnection.mget(bytekeys: _*)

    val t2 = System.currentTimeMillis()
    System.out.println("MGET " + keys.size + " key cost " + (t2 - t1))

    val anyvalues = cachedata.map(x => {
      if (x != null) {
        if(i==0){
          println(" data size : " + x.length)
          i = i + 1
        }
        val data = getKryoTool.deserialize[Any](ByteBuffer.wrap(x))
        data
      }
      else null
    }).toList
    for (i <- 0 to keys.length - 1) {
      multimap += (keys(i) -> anyvalues(i))
    }

    System.out.println("DESERIALIZED " + keys.size + " key cost " + (System.currentTimeMillis() - t2))

    multimap
  }

  override def setCommonCacheValue(cacheName: String, key: String, value: String) = {
    getConnection.hset(cacheName, key, value)
  }
}
