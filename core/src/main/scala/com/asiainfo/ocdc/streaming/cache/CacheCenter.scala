package com.asiainfo.ocdc.streaming.cache

import java.util.{TimerTask, Timer}
import com.asiainfo.ocdc.streaming.MainFrameConf
import com.asiainfo.ocdc.streaming.constant.CacheConstant
import com.asiainfo.ocdc.streaming.tool.CacheFactory
import scala.collection.mutable

/**
 * Created by leo on 6/1/15.
 */
object CacheCenter {

  val cacheMap = new mutable.HashMap[String, (Long, mutable.Map[String, String])]()
  val codisManager = CacheFactory.getManager
  val interval = MainFrameConf.getLong("cache_update_interval", CacheConstant.CACHE_UPDATE_INTERVAL)

  var isLasted = false

  val updateKeys = Seq("lacci2area")
  var endTime = MainFrameConf.get("afternoon8time")

  updateCacheData

  synchronized{}

  def getValue(key: String, field: String): Any = {

    if (isLasted) {
      cacheMap.get(key) match {
        case Some(v) => {
          if (field != null) {
            v._2.get(field) match {
              case Some(v) => v
              case None => null
            }
          } else {
            v._2
          }
        }
        case None => null
        case _ =>
          println("[WARN] unsupported value for cacheMap.get(" + key + ") = " + cacheMap.get(key))
      }
    } else {
      if (field != null) codisManager.getCommonCacheValue(key, field)
      else codisManager.getHashCacheMap(key)
    }
  }

  def setValue(key: String) {
    var result: Any = null
    val newValue = codisManager.getHashCacheMap(key)
    cacheMap.put(key, (System.currentTimeMillis(), newValue))
  }

  def isOutTime(oldTime: Long) = oldTime + interval <= System.currentTimeMillis()

  def updateCacheData() {
    val timer: Timer = new Timer()
    val task: TimerTask = new TimerTask {
      def run {
        isLasted = false
        updateKeys.foreach(x => setValue(x))
        isLasted = true
      }
    }
    timer.schedule(task, 0, interval)
  }
}

