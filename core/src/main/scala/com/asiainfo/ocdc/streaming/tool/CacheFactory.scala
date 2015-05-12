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

}


