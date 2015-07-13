package com.asiainfo.ocdc.streaming.tool

import com.asiainfo.ocdc.streaming._

object CacheFactory {

  val getManager:CacheManager = {
    val manager = MainFrameConf.get("DefaultCacheManager")
    manager match {
      case "TextCacheManager" => new TextCacheManager
      case "CodisCacheManager" => new CodisCacheManager
      case "JodisCacheManager" => new JodisCacheManager
      case _ => throw new Exception("CacheFetchManager is not found!")
    }
  }

  //  val manager = MainFrameConf.get("DefaultCacheManager")
  //  val cacheManagerThreadLocal: ThreadLocal[CacheManager] = new ThreadLocal[CacheManager](){
  //    override def initialValue():CacheManager = {
  //      manager match {
  //        case "TextCacheManager" => new TextCacheManager
  //        case "CodisCacheManager" => new CodisCacheManager
  //        case "JodisCacheManager" => new JodisCacheManager
  //        case _ => throw new Exception("CacheFetchManager is not found!")
  //      }
  //    }
  //
  //  }
  //
  //  val getManager: CacheManager = cacheManagerThreadLocal.get()
}


