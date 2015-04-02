package com.asiainfo.ocdc.streaming

class CacheFetchManager {
  def factory(manager: String):CacheManager = {
    if(manager.equals("TextCacheManager")){
      TextCacheManager
    }else if(manager.equals("CodisCacheManager")){
      CodisCacheManager
    }else{
      throw new RuntimeException("CacheFetchManager is not found!")
    }
  }
}