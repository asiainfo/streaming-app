package com.asiainfo.ocdc.streaming.subscribe

import com.asiainfo.ocdc.streaming.MainFrameConf
import com.asiainfo.ocdc.streaming.tool.CacheFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable.Map

/**
 * Created by leo on 5/11/15.
 */
abstract class BusinessEvent extends Serializable with org.apache.spark.Logging {
  val defaultInterval = 7 * 24 * 60 * 60 * 1000L
  val defaultDelayTime = 20 * 60 * 1000L

  var id: String = null
  var sourceId: String = null
  var conf: BusinessEventConf = null
  var eventSources: Seq[String] = null
  var eventRules: Seq[String] = null
  var selectExp: Seq[String] = null
  var interval: Long = defaultInterval
  var delayTime: Long = defaultDelayTime
  val locktime: String = "locktime"


  def joinkey: String

  def getDelim: String = conf.get("delim")

  def init(sid: String, beconf: BusinessEventConf) {
    conf = beconf
    id = conf.get("beid")
    sourceId = sid
    eventSources = MainFrameConf.getEventSourcesByBsEvent(id)
    eventRules = MainFrameConf.getEventRulesByBsEvent(id)
    selectExp = conf.get("selectExp").split(",").toSeq
    interval = conf.getLong("interval", defaultInterval)
    delayTime = conf.getLong("delaytime", defaultDelayTime)
  }

  def execEvent(eventMap: Map[String, DataFrame]) {
    val filtevents = eventMap.filter(x => eventRules.contains(x._1))

    val currentEvent = filtevents.iterator.next()._2

    /*val keys = filtevents.keys.toSeq
    var currentEvent = filtevents.get(keys(0)).get
    var i = 1
    while (i < keys.size) {
      val nextEvent = filtevents.get(keys(i)).get
      currentEvent = currentEvent.join(nextEvent).where(currentEvent(joinkey) === nextEvent(joinkey))
      i += 1
    }*/

    val selectedData = currentEvent.selectExpr(selectExp: _*)

    val checkedData = selectedData.map(row => {
      var resultData: Option[Row] = None
      val currTime = System.currentTimeMillis()
      val hashkey = "MC_" + id + ":" + row.getString(0)
      val time = row.getString(1)
      var status = CacheFactory.getManager.getHashCacheMap(hashkey)
      // muti event source
      if (eventSources.size > 1) {
        if (status == null) {
          status = Map(sourceId -> time)
          CacheFactory.getManager.setHashCacheMap(hashkey, status)
        } else {
          val lt = status.get(locktime).get.toLong
          if (lt == null) {
            status += (sourceId -> time)
            val maxTime = status.toList.sortBy(_._2).last._2
            status.filter(_._2 + delayTime >= maxTime)

            if (status.size == eventSources.size) status += (locktime -> currTime.toString)
            CacheFactory.getManager.setHashCacheMap(hashkey, status)
            resultData = Some(row)
          } else {
            if (interval + lt < currTime) {
              status.clear()
              status += (sourceId -> time)
              CacheFactory.getManager.setHashCacheMap(hashkey, status)
            }
          }
        }
      }
      // just single event source
      else {
        if (status == null) {
          status = Map(sourceId -> time)
          status += (locktime -> currTime.toString)
          resultData = Some(row)
        } else {
          val lt = status.get(locktime).get
          if (lt + interval < currTime.toString) {
            status.clear()
            status += (sourceId -> time)
            status += (locktime -> currTime.toString)
            resultData = Some(row)
          }
        }
      }

      resultData
    })

    output(checkedData)

  }

  def output(data: RDD[Option[Row]])

}
