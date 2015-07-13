package com.asiainfo.ocdc.streaming.tool

import java.util
import java.util.concurrent.{Callable, ExecutorService, Executors}
import com.asiainfo.ocdc.streaming.MainFrameConf
import org.slf4j.LoggerFactory
import scala.collection.JavaConverters._
import scala.collection.mutable.Map

/**
 * Created by leo on 6/17/15.
 */
object CacheQryThreadPool {
  // 初始化线程池
  val threadPool: ExecutorService = Executors.newFixedThreadPool(MainFrameConf.getInt("codisQryThreadNum"))

}

/*class CodisOperator(cacheManager: RedisCacheManager) {

  private val currentJedis = new ThreadLocal[Jedis] {
    override def initialValue = cacheManager.getResource
  }

  private val currentKryoTool = new ThreadLocal[KryoSerializerStreamAppTool] {
    override def initialValue = new KryoSerializerStreamAppTool
  }

  final def getConnection = {
    val curr_jedis = currentJedis.get()
    curr_jedis
  }

  final def getKryoTool = currentKryoTool.get()

  final def closeConnection = {
    getConnection.close()
    currentJedis.remove()
  }
}


class Qry(cacheManager: RedisCacheManager, keys: Seq[Array[Byte]]) extends CodisOperator(cacheManager) with Callable[util.List[Array[Byte]]] {
  override def call() = {
    //    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    val conn = getConnection
    val pgl = conn.pipelined()
    keys.foreach(x => pgl.get(x))
    val result = pgl.syncAndReturnAll().asInstanceOf[util.List[Array[Byte]]]
    conn.close()
    result
  }
}

class Insert(cacheManager: RedisCacheManager, value: Map[String, Any]) extends CodisOperator(cacheManager) with Callable[String] {
  override def call() = {
    //    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    val conn = getConnection
    val pgl = conn.pipelined()
    val ite = value.iterator
    val kryotool = new KryoSerializerStreamAppTool
    while (ite.hasNext) {
      val elem = ite.next()
      pgl.set(elem._1.getBytes, kryotool.serialize(elem._2).array())
      pgl.sync()
    }
    conn.close()
    ""
  }
}

class QryHashall(cacheManager: RedisCacheManager, keys: Seq[String]) extends CodisOperator(cacheManager) with Callable[util.List[util.Map[String, String]]] {
  override def call() = {
    //    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    val conn = getConnection
    val pgl = conn.pipelined()
    keys.foreach(x => pgl.hgetAll(x))
    val result = pgl.syncAndReturnAll().asInstanceOf[util.List[util.Map[String, String]]]
    conn.close()
    result
  }
}

class InsertHash(cacheManager: RedisCacheManager, value: Map[String, Map[String, String]]) extends CodisOperator(cacheManager) with Callable[String] {
  override def call() = {
    //    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    val conn = getConnection
    val pgl = conn.pipelined()
    val ite = value.iterator
    while (ite.hasNext) {
      val elem = ite.next()
      pgl.hmset(elem._1, elem._2.asJava)
      pgl.sync()
    }
    conn.close()
    ""
  }
}*/


class Qry(keys: Seq[Array[Byte]]) extends Callable[util.List[Array[Byte]]] {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def call() = {
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    var result : java.util.List[Array[Byte]] = null
    try{
      //    val conn = getConnection
      val pgl = conn.pipelined()
      keys.foreach(x => pgl.get(x))
      result = pgl.syncAndReturnAll().asInstanceOf[util.List[Array[Byte]]]
    } catch {
      case ex: Exception =>
        logger.error("= = " * 15 +"found error in Qry.call()")
    } finally{
      conn.close()
    }

    result
  }
}

class Insert(value: Map[String, Any]) extends Callable[String] {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def call() = {
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    try{
      //    val conn = getConnection
      val pgl = conn.pipelined()
      val ite = value.iterator
      val kryotool = new KryoSerializerStreamAppTool
      while (ite.hasNext) {
        val elem = ite.next()
        pgl.set(elem._1.getBytes, kryotool.serialize(elem._2).array())
        pgl.sync()
//        conn.close()
//        ""
      }
    } catch {
      case ex: Exception =>
        logger.error("= = " * 15 +"found error in Qry.call()")
    } finally{
      conn.close()
    }
    ""
  }
}

class QryHashall(keys: Seq[String]) extends Callable[util.List[util.Map[String, String]]] {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def call() = {
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    var result : java.util.List[java.util.Map[String, String]] = null
    try{
      //    val conn = getConnection
      val pgl = conn.pipelined()
      keys.foreach(x => pgl.hgetAll(x))
      result = pgl.syncAndReturnAll().asInstanceOf[util.List[util.Map[String, String]]]
//      conn.close()
//      result
    }catch {
      case ex: Exception =>
        logger.error("= = " * 15 +"found error in Qry.call()")
    } finally{
      conn.close()
    }

    result
  }
}

class InsertHash(value: Map[String, Map[String, String]]) extends Callable[String] {
  val logger = LoggerFactory.getLogger(this.getClass)

  override def call() = {
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource

    try{
      //    val conn = getConnection
      val pgl = conn.pipelined()
      val ite = value.iterator
      while (ite.hasNext) {
        val elem = ite.next()
        pgl.hmset(elem._1, elem._2.asJava)
        pgl.sync()
//        conn.close()
//        ""
      }
    }catch {
      case ex: Exception =>
        logger.error("= = " * 15 +"found error in Qry.call()")
    } finally{
      conn.close()
    }

    ""
  }
}
