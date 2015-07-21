package com.asiainfo.ocdc.streaming.subscribe

import com.asiainfo.ocdc.streaming.MainFrameConf
import com.asiainfo.ocdc.streaming.constant.EventConstant
import com.asiainfo.ocdc.streaming.tool.{CacheFactory, DateFormatUtils}
import org.apache.spark.sql.{DataFrame, Row}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
 * Created by leo on 5/11/15.
 */
abstract class BusinessEvent extends Serializable with org.apache.spark.Logging {

  var id: String = null
//  def id = conf.get("beid")
  var sourceId: String = null
  var conf: BusinessEventConf = null
  var eventSources: Seq[String] = null
  var eventRules: Seq[String] = null
  var selectExp: Seq[String] = null
  var interval: Long = EventConstant.DEFAULTINTERVAL
  var delayTime: Long = EventConstant.DEFAULTDELAYTIME
  val locktime: String = EventConstant.LOCKTIMEFIELD
  var userKeyIdx: Int = _

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
    userKeyIdx = conf.getInt("userKeyIdx")
  }

  def filterBSEvent(row: Row,
                    old_cache: mutable.Map[String, mutable.Map[String, String]], 
                    updateKeys: mutable.Set[String],
                    outputRows: ArrayBuffer[Row]): Unit ={
    val currentSystemTimeMs = System.currentTimeMillis()
    var keyCache = old_cache.getOrElse(getHashKey(row), mutable.Map[String, String]())
    val qryKey = getHashKey(row)
    val timeStr = getTime(row)

    val flagTriggerBSEvent =
      if (eventSources.size > 1) {
        // muti event source
        if (keyCache.size == 0) {
          // cache为空，首次触发，更新cache
          keyCache += (sourceId -> timeStr)
          true
        } else {
          // cache不为空
          val lastBSActiveTimeMs = keyCache.get(locktime).getOrElse("0").toLong
          if (lastBSActiveTimeMs == 0) {
            // cache中最近一次触发业务事件的事件为0，
            keyCache += (sourceId -> timeStr)

            val maxTime = keyCache.map(sourceId2activeTimeStr => {
              val activeTimeStr = sourceId2activeTimeStr._2
              DateFormatUtils.dateStr2Ms(activeTimeStr, "yyyyMMdd HH:mm:ss.SSS")
            }).toList.max

            keyCache = keyCache.filter(sourceId2activeTimeStr => {
              DateFormatUtils.dateStr2Ms(sourceId2activeTimeStr._2, "yyyyMMdd HH:mm:ss.SSS") + delayTime >= maxTime
            })

            // 如果 cache 的所有event都满足，首次触发，更新cache
            if (keyCache.size == eventSources.size) keyCache += (locktime -> currentSystemTimeMs.toString)
            true

          } else {
            // cache 中业务事件已触发过
            if (lastBSActiveTimeMs + interval > currentSystemTimeMs) {
              // 如果最近一次触发事件在周期策略内，不触发
              keyCache += (sourceId -> timeStr)
              false
            } else {
              // 如果最近一次触发事件在周期策略外，可以重新触发
              keyCache += (sourceId -> timeStr)
              keyCache += (locktime -> currentSystemTimeMs.toString)
              true
            }
          }
        }
      }
      // just single event source
      else {
        if (keyCache.size == 0) {
          //cache为空，首次触发，更新cache
          keyCache += (sourceId -> timeStr)
          keyCache += (locktime -> currentSystemTimeMs.toString)
          true
        } else {
          // cache不为空
          val lastBSActiveTimeMs = keyCache.get(locktime).getOrElse("0").toLong
          if(lastBSActiveTimeMs == 0){
            // cache中最近一次触发业务事件的事件为0，
            keyCache += (sourceId -> timeStr)
            keyCache += (locktime -> currentSystemTimeMs.toString)
            true
          } else {
            // cache 中业务事件已触发过
            if (lastBSActiveTimeMs + interval > currentSystemTimeMs) {
              // 如果最近一次触发事件在周期策略内，不触发
              keyCache += (sourceId -> timeStr)
              false
            } else {
              // 如果最近一次触发事件在周期策略外，可以重新触发
              keyCache += (sourceId -> timeStr)
              keyCache += (locktime -> currentSystemTimeMs.toString)
              true
            }
          }
        }
      }

    old_cache.update(qryKey, keyCache)
    updateKeys += qryKey

//    println("= = " * 20 +" filterBSEvent.flagTriggerBSEvent = " + flagTriggerBSEvent)
    if(flagTriggerBSEvent){
      outputRows.append(row)
    }
  }

  def execEvent(eventMap: mutable.Map[String, DataFrame]) {
    val filtevents = eventMap.filter(x => eventRules.contains(x._1))
//    val currentEvent = filtevents.iterator.next()._2
    val (currentEventRuleId, currentEvent) = filtevents.iterator.next()

    val selectedData = currentEvent.selectExpr(selectExp: _*)

//    println("* * " * 20 +"currentEventRuleId = " + currentEventRuleId +", selectedData = ")
//    selectedData.show()
//    println("= = " * 20 +"currentEventRuleId = " + currentEventRuleId +", selectedData done")


    selectedData.mapPartitions(iter=>{

      new Iterator[Row]{
        private[this] var current: Row = _
        private[this] var currentPos: Int = -1
        private[this] var batchArray: Array[Row] = _
        private[this] val batchArrayBuffer = new ArrayBuffer[Row]()

        private[this] val updateKeys = mutable.Set[String]()
        private[this] val outputRows = ArrayBuffer[Row]()

        override def hasNext: Boolean ={
          iter.hasNext && batchNext()
        }

        override def next(): Row ={
          batchArray.head
        }

        var numBatches = 0
        var batchSize = 0
        val batchLimit = conf.getInt("batchLimit")

        def batchNext(): Boolean ={
          var result = false

          batchArrayBuffer.clear()

          while (iter.hasNext && (batchSize < batchLimit)) {
            current = iter.next()
            batchArrayBuffer.append(current)

            batchSize += 1
            currentPos += 1
          }

          if(batchArrayBuffer.length > 0) {
            batchArray = batchArrayBuffer.toArray
            result = true

            val qryKeys = batchArrayBuffer.map(getHashKey(_))
            val old_cache = CacheFactory.getManager.hgetall(qryKeys.toList)
            for(row <- batchArrayBuffer){
              filterBSEvent(row, old_cache, updateKeys, outputRows)
            }

            //更新cache
            val updateData = old_cache.filter(x => updateKeys.contains(x._1))
            val t2 = System.currentTimeMillis()
            CacheFactory.getManager.hmset(updateData)
            println(" update saled user data cost time : " + (System.currentTimeMillis() - t2) + " millis ! ")
            updateKeys.clear()

            //输出
            if(outputRows.length > 0) {
              output(outputRows.toArray)
              outputRows.clear()
            }

            batchSize = 0
            numBatches += 1
          }
          result
        }
      }
    }).count()

  }

//  def output(data: RDD[Option[Row]])

  def output(data: Array[Row])

}
