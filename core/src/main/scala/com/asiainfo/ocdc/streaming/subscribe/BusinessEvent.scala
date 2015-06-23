package com.asiainfo.ocdc.streaming.subscribe

import com.asiainfo.ocdc.streaming.MainFrameConf
import com.asiainfo.ocdc.streaming.constant.EventConstant
import com.asiainfo.ocdc.streaming.tool.CacheFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.Map

/**
 * Created by leo on 5/11/15.
 */
abstract class BusinessEvent extends Serializable with org.apache.spark.Logging {

  var id: String = null
  var sourceId: String = null
  var conf: BusinessEventConf = null
  var eventSources: Seq[String] = null
  var eventRules: Seq[String] = null
  var selectExp: Seq[String] = null
  var interval: Long = EventConstant.DEFAULTINTERVAL
  var delayTime: Long = EventConstant.DEFAULTDELAYTIME
  val locktime: String = EventConstant.LOCKTIMEFIELD

  def getDelim: String = conf.get("delim")

  def getHashKey(row: Row): String

  def getTime(row: Row): String

  def init(sid: String, beconf: BusinessEventConf) {
    conf = beconf
    id = conf.get("beid")
    sourceId = sid
    eventSources = MainFrameConf.getEventSourcesByBsEvent(id)
    eventRules = MainFrameConf.getEventRulesByBsEvent(id)
    selectExp = conf.get("selectExp").split(",").toSeq
    interval = conf.getLong("interval", EventConstant.DEFAULTINTERVAL)
    delayTime = conf.getLong("delaytime", EventConstant.DEFAULTDELAYTIME)
  }

  /*def execEvent(eventMap: Map[String, DataFrame]) {
    val filtevents = eventMap.filter(x => eventRules.contains(x._1))
    val currentEvent = filtevents.iterator.next()._2
    val selectedData = currentEvent.selectExpr(selectExp: _*)

    val checkedData = selectedData.map(row => {
      var resultData: Option[Row] = None
      val currTime = System.currentTimeMillis()
      val hashkey = getHashKey(row)
      val time = getTime(row)
      var status = CacheFactory.getManager.getHashCacheMap(hashkey)
      // muti event source
      if (eventSources.size > 1) {
        if (status.size == 0) {
          status = Map(sourceId -> time)
          CacheFactory.getManager.setHashCacheMap(hashkey, status)
        } else {
          //          val lt = status.get(locktime).get.toLong
          val lt = status.get(locktime).getOrElse("0").toLong
          if (lt == 0) {
            status += (sourceId -> time)
            val maxTime = status.toList.sortBy(_._2).last._2
            status.filter(_._2 + delayTime >= maxTime)

            if (status.size == eventSources.size) status += (locktime -> currTime.toString)
            CacheFactory.getManager.setHashCacheMap(hashkey, status)
            resultData = Some(row)
          } else {
            if (lt + interval < currTime) {
              status.clear()
              status += (sourceId -> time)
              CacheFactory.getManager.setHashCacheMap(hashkey, status)
            }
          }
        }
      }
      // just single event source
      else {
        if (status.size == 0) {
          status = Map(sourceId -> time)
          status += (locktime -> currTime.toString)
          CacheFactory.getManager.setHashCacheMap(hashkey, status)
          resultData = Some(row)
        } else {
          val lt = status.get(locktime).getOrElse("0").toLong
          if (lt + interval < currTime) {
            status.clear()
            status += (sourceId -> time)
            status += (locktime -> currTime.toString)
            CacheFactory.getManager.setHashCacheMap(hashkey, status)
            resultData = Some(row)
          }
        }
      }

      resultData
    })

//    val checkedData = selectedData.map(row => Option(row))

    output(checkedData)

  }*/


  def execEvent(eventMap: Map[String, DataFrame]) {
    val filtevents = eventMap.filter(x => eventRules.contains(x._1))
    val currentEvent = filtevents.iterator.next()._2
    val selectedData = currentEvent.selectExpr(selectExp: _*)

    val hashKeys = mutable.Set[String]()
    selectedData.foreach(row => {
      val hashkey = getHashKey(row)
      hashKeys += hashkey
    })

    val t1 = System.currentTimeMillis()
    val old_cache = CacheFactory.getManager.hgetall(hashKeys.toList)
    println(" query saled user data cost time : " + (System.currentTimeMillis() - t1) + " millis ! ")

    val updateKeys = mutable.Set[String]()

    val checkedData = selectedData.map(row => {
      var resultData: Option[Row] = None
      val currTime = System.currentTimeMillis()
      val hashkey = getHashKey(row)
      val time = getTime(row)
      var status = old_cache.get(hashkey).get
      // muti event source
      if (eventSources.size > 1) {
        if (status.size == 0) {
          status = Map(sourceId -> time)
          old_cache.update(hashkey, status)
          updateKeys += hashkey
        } else {
          //          val lt = status.get(locktime).get.toLong
          val lt = status.get(locktime).getOrElse("0").toLong
          if (lt == 0) {
            status += (sourceId -> time)
            val maxTime = status.toList.sortBy(_._2).last._2
            status.filter(_._2 + delayTime >= maxTime)

            if (status.size == eventSources.size) status += (locktime -> currTime.toString)
            old_cache.update(hashkey, status)
            updateKeys += hashkey
            resultData = Some(row)
          } else {
            if (lt + interval < currTime) {
              status.clear()
              status += (sourceId -> time)
              old_cache.update(hashkey, status)
              updateKeys += hashkey
            }
          }
        }
      }
      // just single event source
      else {
        if (status.size == 0) {
          status = Map(sourceId -> time)
          status += (locktime -> currTime.toString)
          old_cache.update(hashkey, status)
          updateKeys += hashkey
          resultData = Some(row)
        } else {
          val lt = status.get(locktime).getOrElse("0").toLong
          if (lt + interval < currTime) {
            status.clear()
            status += (sourceId -> time)
            status += (locktime -> currTime.toString)
            old_cache.update(hashkey, status)
            updateKeys += hashkey
            resultData = Some(row)
          }
        }
      }

      resultData
    })

    val updateData = old_cache.filter(x => updateKeys.contains(x._1))

    val t2 = System.currentTimeMillis()
    CacheFactory.getManager.hmset(updateData)
    println(" update saled user data cost time : " + (System.currentTimeMillis() - t2) + " millis ! ")

    output(checkedData)

  }

  def output(data: RDD[Option[Row]])

}
