package com.asiainfo.ocdc.streaming.subscribe

import com.asiainfo.ocdc.streaming.MainFrameConf
import com.asiainfo.ocdc.streaming.constant.EventConstant
import com.asiainfo.ocdc.streaming.tool.{CacheFactory, DateFormatUtils}
import org.apache.spark.rdd.RDD
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
                    outputRows: ArrayBuffer[Row], outputRowsKeySet: scala.collection.mutable.Set[String]): Unit ={
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
          // cache不为空, 说明业务事件已触发过（配置模型：约定一个业务在一个流上只有一个事件）
          val lastBSActiveTimeMs = keyCache.get(locktime).getOrElse("0").toLong

          if (lastBSActiveTimeMs + interval > currentSystemTimeMs) {
            // 当前事件生成时间（与最近一次触发事件的时间相比）在周期策略内，不触发
            keyCache += (sourceId -> timeStr)
            false
          } else {
            // 当前事件生成时间（与最近一次触发事件的时间相比）在周期策略外，可以重新触发
            keyCache += (sourceId -> timeStr)
            keyCache += (locktime -> currentSystemTimeMs.toString)
            true
          }
        }
      }

    old_cache.update(qryKey, keyCache)

//    println("= = " * 20 +" filterBSEvent.flagTriggerBSEvent = " + flagTriggerBSEvent)
    if(flagTriggerBSEvent){
      outputRows.append(row)
      if(!outputRowsKeySet.add(row.getString(userKeyIdx))) {
        logWarning("= = " * 5 + "in one batch found duplicatedKey " + row.getString(userKeyIdx) +", getHashKey(row) = " + getHashKey(row))
      }
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
    val rddRow = transformDF2RDD(selectedData, userKeyIdx)

    rddRow.mapPartitions(iter=>{

      new Iterator[Row]{
        private[this] var current: Row = _
        private[this] var currentPos: Int = -1
        private[this] var batchArray: Array[Row] = _
        private[this] val batchArrayBuffer = new ArrayBuffer[Row]()

        private[this] val outputRows = ArrayBuffer[Row]()
        private[this] val outputRowsKeySet = scala.collection.mutable.Set[String]()


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
            val businessCache = CacheFactory.getManager.hgetall(qryKeys.toList)
            for(row <- batchArrayBuffer){
              filterBSEvent(row, businessCache, outputRows, outputRowsKeySet)
            }

            //更新cache
//            val updateData = old_cache.filter(x => updateKeys.contains(x._1)) //每个key都需要更新
            val t2 = System.currentTimeMillis()
            CacheFactory.getManager.hmset(businessCache)
            println(" update saled user data cost time : " + (System.currentTimeMillis() - t2) + " millis ! ")

            //输出
            if(outputRows.length > 0) {
              logInfo("batchSize = " + batchSize+ ", outputRows.length = " + outputRows.length +", numFiltered = " + (batchSize - outputRows.length))
              output(outputRows.toArray)
              outputRows.clear()
              outputRowsKeySet.clear()
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

  def transformDF2RDD(old_dataframe: DataFrame, partitionKeyIdx: Int): RDD[Row] = old_dataframe.rdd

}
