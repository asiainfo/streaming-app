package com.asiainfo.ocdc.streaming.tool

import java.net.InetAddress
import java.nio.ByteBuffer
import java.util.ArrayList

import com.asiainfo.ocdc.streaming.MainFrameConf
import redis.clients.jedis.{Jedis, JedisPoolConfig, JedisPool}

import scala.collection.mutable.Map
import scala.collection.convert.wrapAsJava.mapAsJavaMap
import scala.collection.convert.wrapAsScala._

/**
 * Created by leo on 5/12/15.
 */
class TestCodisCacheManager extends TestRedisCacheManager {
  private val jedisPool: JedisPool = {

    val JedisConfig = new JedisPoolConfig()
    JedisConfig.setMaxIdle(MainFrameConf.getInt("JedisMaxIdle"))
    JedisConfig.setMaxTotal(MainFrameConf.getInt("JedisMaxTotal"))
    JedisConfig.setMinEvictableIdleTimeMillis(MainFrameConf.getInt("JedisMEM"))
    JedisConfig.setTestOnBorrow(true)

    val hp:Tuple2[String,String]= {
      val proxylist = MainFrameConf.get("CodisProxy").split(",")
      val localip = InetAddress.getLocalHost.getHostAddress
      val proxymap = proxylist.map(args => (args.split(":")(0),args.split(":")(1))).toMap
      var rhost:String = null
      var rip:String = null

      proxymap.get(localip) match {
        case Some(value) =>  rhost = localip
          rip = value
        case None => rhost = proxylist(0).split(":")(0)
          rip = proxylist(0).split(":")(1)
      }
      (rhost,rip)
    }
    new JedisPool(JedisConfig,hp._1,hp._2.toInt)
  }

  override def getResource = jedisPool.getResource
}

abstract class TestRedisCacheManager extends CacheManager {

  final def getConnection = getResource

  final def getKryoTool = new KryoSerializerStreamAppTool

  //final def getConnection = currentJedis

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
    val seqlist = new ArrayList[Array[Byte]]()
    val it = keysvalues.keySet.iterator
    while (it.hasNext) {
      val elem = it.next()
      seqlist.add(elem.getBytes)
      seqlist.add(getKryoTool.serialize(keysvalues(elem)).array())
    }
    getConnection.mset(seqlist.toIndexedSeq: _*)
  }

  override def getMultiCacheByKeys(keys: List[String]): Map[String, Any] = {
    val multimap = Map[String, Any]()
    val bytekeys = keys.map(x => x.getBytes).toSeq
    val anyvalues = getConnection.mget(bytekeys: _*).map(x => {
      if (x != null) {
        val data = getKryoTool.deserialize[Any](ByteBuffer.wrap(x))
        data
      }
      else None
    }).toList
    for (i <- 0 to keys.length - 1) {

      multimap += (keys(i) -> anyvalues(i))
    }
    multimap
  }

  override def setCommonCacheValue(cacheName: String, key: String, value: String) = {
    getConnection.hset(cacheName, key, value)
  }
}
