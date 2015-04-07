package com.asiainfo.ocdc.streaming

object CacheFactory {
  def getManager(manager: String = "TextCacheManager"):CacheManager = {
    if(manager.equals("TextCacheManager")){
      TextCacheManager
    }else if(manager.equals("CodisCacheManager")){
      CodisCacheManager
    }else{
      throw new RuntimeException("CacheFetchManager is not found!")
    }
  }
}