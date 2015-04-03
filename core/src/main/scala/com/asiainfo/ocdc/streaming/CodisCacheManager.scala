package com.asiainfo.ocdc.streaming

import redis.clients.jedis.{JedisPool, Jedis,Pipeline,JedisPoolConfig}
import scala.collection.convert.wrapAsJava.mapAsJavaMap
import scala.collection.convert.wrapAsScala._
import scala.collection.mutable.Map


/**
 * Created by tianyi on 3/30/15.
 * Modified by maji on 4/01/15
 */

object CodisCacheManager extends CacheManager {

  private var jedisPool: JedisPool = null
  private var jedis: Jedis = null

  def getPool():JedisPool = {
    if (jedisPool == null){
      val host = "192.168.84.142"
      val port = 6381
      val JedisConfig = new JedisPoolConfig()
      JedisConfig.setMaxIdle(300)
      JedisConfig.setMaxActive(1000)
      JedisConfig.setMinEvictableIdleTimeMillis(600000)
      JedisConfig.setTestOnBorrow(true)
      jedisPool = new JedisPool(JedisConfig,host,port)
    }
    jedisPool
  }

  def returnResource(pool:JedisPool, jedis:Jedis){
    if(jedis != null){
      pool.returnResource(jedis)
    }
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
