package com.asiainfo.ocdc.streaming

import redis.clients.jedis.{JedisPool, Jedis,Pipeline,JedisPoolConfig}
import scala.collection.convert.wrapAsJava.mapAsJavaMap
import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.Map
import java.net.InetAddress


/**
 * Created by tianyi on 3/30/15.
 * Modified by maji on 4/01/15
 */

object CodisCacheManager extends CacheManager {

  private var jedisPool: JedisPool = null
  private var jedis: Jedis = null

  def getPool():JedisPool = {
    if (jedisPool == null){
      val hp = getProxy()
      val JedisConfig = new JedisPoolConfig()
      JedisConfig.setMaxIdle(MainFrameConf.getInt("JedisMaxIdle"))
      JedisConfig.setMaxActive(MainFrameConf.getInt("JedisMaxActive"))
      JedisConfig.setMinEvictableIdleTimeMillis(MainFrameConf.getInt("JedisMEM"))
      JedisConfig.setTestOnBorrow(true)
      jedisPool = new JedisPool(JedisConfig,hp._1,hp._2.toInt)
    }
    jedisPool
  }

  def returnResource(pool:JedisPool, jedis:Jedis){
    if(jedis != null){
      pool.returnResource(jedis)
    }
  }

  def getProxy(proxy: String = MainFrameConf.get("CodisProxy")): Tuple2[String,String] = {
    val proxylist = proxy.split(",")
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

  override def getHashCacheList(key: String): List[String] = {
    try{
      jedisPool = getPool()
      jedis = jedisPool.getResource
      jedis.lrange(key,0,-1).toList
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis).asInstanceOf[List[String]]
    }finally {
      returnResource(jedisPool,jedis)
    }
  }

  override def getHashCacheMap(key: String): Map[String, String] = {
    try{
      jedisPool = getPool()
      jedis = jedisPool.getResource
      jedis.hgetAll(key)
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis).asInstanceOf[Map[String,String]]
    }finally {
      returnResource(jedisPool,jedis)
    }
  }

  override def setHashCacheString(key: String, value: String): Unit = {
    try{
      jedisPool = getPool()
      jedis = jedisPool.getResource
      jedis.set(key,value)
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis)
    }finally {
      returnResource(jedisPool,jedis)
    }
  }

  override def getCommonCacheValue(cacheName: String, key: String): String = {
    try{
      jedisPool = getPool()
      jedis = jedisPool.getResource
      jedis.hget(cacheName,key)
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis).asInstanceOf[String]
    }finally {
      returnResource(jedisPool,jedis)
    }
  }

  override def getHashCacheString(key: String): String = {
    try{
      jedisPool = getPool()
      jedis = jedisPool.getResource
      jedis.get(key)
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis).asInstanceOf[String]
    }finally {
      returnResource(jedisPool,jedis)
    }
  }

  override def getCommonCacheMap(key: String): Map[String, String] = {
    try{
      jedisPool = getPool()
      jedis = jedisPool.getResource
      jedis.hgetAll(key)
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis).asInstanceOf[Map[String,String]]
    }finally {
      returnResource(jedisPool,jedis)
    }
  }

  override def getCommonCacheList(key: String): List[String] = {
    try{
      jedisPool = getPool()
      jedis = jedisPool.getResource
      jedis.lrange(key,0,-1).toList
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis).asInstanceOf[List[String]]
    }finally {
      returnResource(jedisPool,jedis)
    }
  }

  override def setHashCacheMap(key: String, value: Map[String, String]): Unit = {
    try{
      jedisPool = getPool()
      jedis = jedisPool.getResource
      jedis.hmset(key,mapAsJavaMap(value))
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis)
    }finally {
      returnResource(jedisPool,jedis)
    }
  }

  override def setHashCacheList(key: String, value: List[String]): Unit = {
    /*
       val pl = jedis.pipelined()
       value.map{ x=> pl.rpush(key,x)}
       pl.sync()
    */
    try{
      jedisPool = getPool()
      jedis = jedisPool.getResource
      value.map{ x=>
        jedis.rpush(key,x)
      }
    }catch{
      case e:Exception => e.printStackTrace()
        jedisPool.returnBrokenResource(jedis)
    }finally {
      returnResource(jedisPool,jedis)
    }
  }
}
