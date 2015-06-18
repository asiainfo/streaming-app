package com.asiainfo.ocdc.streaming.tool

import java.util
import java.util.concurrent.{Callable, Executors, ExecutorService}

import com.asiainfo.ocdc.streaming.MainFrameConf

/**
 * Created by leo on 6/17/15.
 */
object CacheQryThreadPool {
  // 初始化线程池
  val threadPool: ExecutorService = Executors.newFixedThreadPool(MainFrameConf.getInt("codisQryThreadNum"))


}


class Qry(keys: Seq[Array[Byte]]) extends Callable[util.List[Array[Byte]]] {
  override def call() = {
    val conn = CacheFactory.getManager.asInstanceOf[CodisCacheManager].getResource
    val pgl = conn.pipelined()
    keys.foreach(x => pgl.get(x))
    val result = pgl.syncAndReturnAll().asInstanceOf[util.List[Array[Byte]]]
    conn.close()
    result
  }
}

class Insert extends Runnable {
  override def run(): Unit = {}
}