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
  var conf: BusinessEventConf = null
  var eventSources: Seq[String] = null
  var eventRules: Seq[String] = null
  var selectExp: Seq[String] = null
  var interval: Long = EventConstant.DEFAULTINTERVAL
  var delayTime: Long = EventConstant.DEFAULTDELAYTIME
  val locktime: String = EventConstant.LOCKTIMEFIELD
  var userKeyIdx: Int = _
  var timeIdx: Int = _

  def getDelim: String = conf.get("delim")

  def getHashKey(row: Row): String

  def getTime(row: Row): String

  def init(sid: String, beconf: BusinessEventConf) {
    conf = beconf
    id = conf.get("beid")
    eventSources = MainFrameConf.getEventSourcesByBsEvent(id)
    eventRules = MainFrameConf.getEventRulesByBsEvent(id)
    selectExp = conf.get("selectExp").split(",").toSeq
    interval = conf.getLong("interval", EventConstant.DEFAULTINTERVAL) //营销周期
    delayTime = conf.getLong("delaytime", EventConstant.DEFAULTDELAYTIME) //
    userKeyIdx = conf.getInt("userKeyIdx") //selectExp中用户唯一标识字段的位置索引
    timeIdx = conf.getInt("timeIdx") //selectExp中时间字段的位置索引
  }

  /**
   * 业务事件排重（周期营销），更新业务缓存
   * @param row
   * @param old_cache
   * @param outputRows
   * @param outputRowsKeySet
   */
  def filterBSEvent(row: Row, eventSourceIdOfRow: String,
                    old_cache: mutable.Map[String, mutable.Map[String, String]], 
                    outputRows: ArrayBuffer[Row], outputRowsKeySet: scala.collection.mutable.Set[String]): Unit ={
    val currentSystemTimeMs = System.currentTimeMillis()
    val qryKey = getHashKey(row)
    var keyCache = old_cache.getOrElse(qryKey, mutable.Map[String, String]())
    val timeStr = getTime(row)
    //TODO: 业务事件缓存中的事件满足时间，取日志中的时间还是分析时的系统时间？是否提供配置实现可选？
    //      配置 eventActiveTimeMs = if (选日志时间) timeStr else currentSystemTimeMs

    val flagTriggerBSEvent =
      if (eventSources.size > 1) {
        // muti event source
        if (keyCache.size == 0) {
          // cache为空，首次触发，更新cache
          //TODO: sourceId 对于多个流的情况有待改进
//          keyCache += (sourceId -> timeStr)
          updateEventActiveTime(row, keyCache, eventSourceIdOfRow, currentSystemTimeMs)
          true
        } else {
          // cache不为空
          val lastBSActiveTimeMs = keyCache.get(locktime).getOrElse("0").toLong
          if (lastBSActiveTimeMs == 0) {
            // cache中最近一次触发业务事件的事件为0，

//            keyCache += (sourceId -> timeStr)
            updateEventActiveTime(row, keyCache, eventSourceIdOfRow, currentSystemTimeMs)

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
//              keyCache += (sourceId -> timeStr)
              updateEventActiveTime(row, keyCache, eventSourceIdOfRow, currentSystemTimeMs)
              false
            } else {
              // 如果最近一次触发事件在周期策略外，可以重新触发
//              keyCache += (sourceId -> timeStr)
              updateEventActiveTime(row, keyCache, eventSourceIdOfRow, currentSystemTimeMs)
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
//          keyCache += (sourceId -> timeStr)
          updateEventActiveTime(row, keyCache, eventSourceIdOfRow, currentSystemTimeMs)
          keyCache += (locktime -> currentSystemTimeMs.toString)
          true
        } else {
          // cache不为空, 说明业务事件已触发过（配置模型：约定一个业务在一个流上只有一个事件）
          val lastBSActiveTimeMs = keyCache.get(locktime).getOrElse("0").toLong

          if (lastBSActiveTimeMs + interval > currentSystemTimeMs) {
            // 当前事件生成时间（与最近一次触发事件的时间相比）在周期策略内，不触发
//            keyCache += (sourceId -> timeStr)
            updateEventActiveTime(row, keyCache, eventSourceIdOfRow, currentSystemTimeMs)
            false
          } else {
            // 当前事件生成时间（与最近一次触发事件的时间相比）在周期策略外，可以重新触发
//            keyCache += (sourceId -> timeStr)
            updateEventActiveTime(row, keyCache, eventSourceIdOfRow, currentSystemTimeMs)
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

  /**
   * 更新业务缓存中指定事件的最新满足时间，目前配置模式：一个事件流配置一个事件
   * @param row
   * @param keyCache
   * @param eventSourceIdOfRow
   * @param ActiveSystemTimeMs
   */
  def updateEventActiveTime(row: Row, keyCache: mutable.Map[String, String],
                            eventSourceIdOfRow: String,
                            ActiveSystemTimeMs: Long = System.currentTimeMillis()): Unit ={

    val lastEventActiveTimeStr = keyCache.get(eventSourceIdOfRow)
    val lastEventActiveTimeMs =  lastEventActiveTimeStr match {
      case Some(timeStr) =>
        try{
          DateFormatUtils.dateStr2Ms(timeStr, "yyyyMMdd HH:mm:ss.SSS")
        } catch {
          case ex: java.text.ParseException => //时间字段转换时出现解析异常，认为时间字段无效
            0L
          case ex: Exception => //时间字段转换时出现其他异常，抛出异常
            throw ex
        }
      case None =>
        0L
    }

    if (lastEventActiveTimeMs < ActiveSystemTimeMs) {
      keyCache.put(eventSourceIdOfRow, DateFormatUtils.dateMs2Str(ActiveSystemTimeMs, "yyyyMMdd HH:mm:ss.SSS"))
    }
  }

  def execEvent(eventMap: mutable.Map[String, DataFrame]) {
    val filtevents = eventMap.filter(x => eventRules.contains(x._1))
//    val currentEvent = filtevents.iterator.next()._2
    val (currentEventRuleId, currentEvent) = filtevents.iterator.next() //约定配置模式：1个业务在1个流上只配置一个eventRules

    val currentEventSourceId = MainFrameConf.eventRule2eventSource.get(currentEventRuleId).get
    
    val selectedData = currentEvent.selectExpr(selectExp: _*)

    // println("* * " * 20 +"currentEventRuleId = " + currentEventRuleId +", selectedData = ")
    // selectedData.show()
    // println("= = " * 20 +"currentEventRuleId = " + currentEventRuleId +", selectedData done")
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

            val qryKeys = batchArrayBuffer.map(getHashKey(_)) //获取1个批次(此处的批次与spark-streaming的batch不同)需要查询业务订阅缓存的keys
            val businessCache = CacheFactory.getManager.hgetall(qryKeys.toList) //批次获取需要的业务订阅缓存
            for(row <- batchArrayBuffer){
              filterBSEvent(row, currentEventSourceId, businessCache, outputRows, outputRowsKeySet)
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
