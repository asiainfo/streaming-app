package com.asiainfo.ocdc.streaming

import redis.clients.jedis.Jedis
import java.util.ArrayList
import com.asiainfo.ocdc.streaming.tool.KryoSerializerStreamAppTool
import java.nio.ByteBuffer
import scala.collection.convert.wrapAsScala._
import scala.collection.convert.wrapAsJava.mapAsJavaMap
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

	def setCommonCacheValue(cacheName: String, key: String, value: String)
}


abstract class RedisCacheManager extends CacheManager {

  def exec[U](f: Jedis => U): U

  override def getHashCacheList(key: String): List[String] = {
    exec { jedis => jedis.lrange(key,0,-1).toList }
  }

  override def getHashCacheMap(key: String): Map[String, String] = {
    exec { jedis => jedis.hgetAll(key) }
  }

  override def setHashCacheString(key: String, value: String): Unit = {
    exec { jedis => jedis.set(key,value)}
  }

  override def getCommonCacheValue(cacheName: String, key: String): String = {
    exec { jedis => jedis.hget(cacheName,key)}
  }

  override def getHashCacheString(key: String): String = {
    exec { jedis => jedis.get(key)}
  }

  override def getCommonCacheMap(key: String): Map[String, String] = {
    exec { jedis => jedis.hgetAll(key)}
  }

  override def getCommonCacheList(key: String): List[String] = {
    exec { jedis => jedis.lrange(key,0,-1).toList}
  }

  override def setHashCacheMap(key: String, value: Map[String, String]): Unit = {
    exec { jedis => jedis.hmset(key,mapAsJavaMap(value))}
  }

  override def setHashCacheList(key: String, value: List[String]): Unit = {
    exec { jedis => value.map{ x=> jedis.rpush(key,x)}}
  }

  override def setByteCacheString(key: String, value: String) {
    //TODO
  }

  override def getByteCacheString(key: String): List[String] = {
    null
  }

  override def setMultiCache(keysvalues: Map[String, Any]) {
    exec { jedis =>
      val seqlist = new ArrayList[Array[Byte]]()
      val it = keysvalues.keySet.iterator
      while (it.hasNext){
        val elem = it.next()
        seqlist.add(elem.getBytes)
        seqlist.add(KryoSerializerStreamAppTool.serialize(keysvalues(elem)).array())
      }
      jedis.mset(seqlist.toIndexedSeq: _*)
    }
  }

  override def getMultiCacheByKeys(keys: List[String]): Map[String, Any] = {
    exec { jedis =>
      val multimap = Map[String,Any]()
      val bytekeys = keys.map(x=> x.getBytes).toSeq
      val anyvalues = jedis.mget(bytekeys: _*).map(x => {
        KryoSerializerStreamAppTool.deserialize[Any](ByteBuffer.wrap(x))
      }).toList
      for(i <- 0 to keys.length -1){
        multimap += (keys(i)-> anyvalues(i))
      }
      multimap
    }
  }

	override def setCommonCacheValue(cacheName: String, key: String, value: String) = {
		exec { jedis => jedis.hset(cacheName,key,value)}
	}
}
