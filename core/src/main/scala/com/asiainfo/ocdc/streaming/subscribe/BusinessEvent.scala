package com.asiainfo.ocdc.streaming.subscribe

import com.asiainfo.ocdc.streaming.MainFrameConf
import com.asiainfo.ocdc.streaming.tool.CacheFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.Map

/**
 * Created by leo on 5/11/15.
 */
class BusinessEvent extends Serializable with org.apache.spark.Logging {
  var id: String
  var sourceId: String
  var conf: BusinessEventConf
  var eventSources: Seq[String]
  var eventRules: Seq[String]
  var selectExp: Seq[String]
  var interval: Long
  var delayTime: Long
  val locktime: String = "locktime"

  def joinkey: String = "imsi"

  def init(sid: String, beconf: BusinessEventConf) {
    conf = beconf
    id = conf.get("id")
    sourceId = sid
    eventSources = MainFrameConf.getEventSourcesByBsEvent(id)
    eventRules = MainFrameConf.getEventRulesByBsEvent(id)
    selectExp = conf.get("selectExp").split(",").toSeq
    interval = conf.getLong("interval")
    delayTime = conf.getLong("delaytime")
  }

  def execEvent(eventMap: Map[String, DataFrame]) {
    val filtevents = eventMap.filter(x => eventRules.contains(x._1))

    val keys = filtevents.keys.toSeq
    var currentEvent = filtevents.get(keys(0)).get
    var i = 1
    while (i < keys.size) {
      val nextEvent = filtevents.get(keys(i)).get
      currentEvent = currentEvent.join(nextEvent).where(currentEvent(joinkey) === nextEvent(joinkey))
      i += 1
    }

    val selectedData = currentEvent.selectExpr(selectExp: _*)

    val checkedData = selectedData.map(row => {
      var resultData: Option = None
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
          val lt = status.get(locktime).get
          if (lt == null) {
            status += (sourceId -> time)
            val maxTime = status.map(_._2).toSeq.sortBy(_).last
            status.filter(_._2 + delayTime >= maxTime)

            if (status.size == eventSources.size) status += (locktime -> currTime)
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
        if (status == null) {
          status = Map(sourceId -> time)
          status += (locktime -> currTime)
          resultData = Some(row)
        } else {
          val lt = status.get(locktime).get
          if (lt + interval < currTime) {
            status.clear()
            status += (sourceId -> time)
            status += (locktime -> currTime)
            resultData = Some(row)
          }
        }
      }

      resultData
    })

    output(checkedData)

  }

  def output(data: RDD[Option])

}
