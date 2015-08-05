package com.asiainfo.ocdc.streaming.eventsource

import com.asiainfo.ocdc.streaming._
import com.asiainfo.ocdc.streaming.eventrule.{EventRule, StreamingCache}
import com.asiainfo.ocdc.streaming.labelrule.LabelRule
import com.asiainfo.ocdc.streaming.subscribe.BusinessEvent
import com.asiainfo.ocdc.streaming.tool.{CacheFactory, DateFormatUtils}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.collection.{immutable, mutable}


abstract class EventSource() extends Serializable with org.apache.spark.Logging {
  var id: String = null
  var conf: EventSourceConf = null
  var shuffleNum: Int = 0

  var beginTime = MainFrameConf.get("morning8time")
  var endTime = MainFrameConf.get("afternoon8time")

  //  val timesdf = new SimpleDateFormat("HH:mm:ss")
  //  val morning8Time = timesdf.parse(beginTime).getTime
  //  val afternoon8Time = timesdf.parse(endTime).getTime

  val timePattern = "HH:mm:ss"
  val morning8Time = DateFormatUtils.dateStr2Ms(beginTime, timePattern)
  val afternoon8Time = DateFormatUtils.dateStr2Ms(endTime, timePattern)

  protected val labelRules = new ArrayBuffer[LabelRule]
  protected val eventRules = new ArrayBuffer[EventRule]
  protected val bsEvents = new ArrayBuffer[BusinessEvent]

  def addEventRule(rule: EventRule): Unit = {
    eventRules += rule
  }

  def addLabelRule(rule: LabelRule): Unit = {
    labelRules += rule
  }

  def addBsEvent(bs: BusinessEvent): Unit = {
    bsEvents += bs
  }

  def init(conf: EventSourceConf): Unit = {
    this.conf = conf
    id = this.conf.get("id")
    shuffleNum = conf.getInt("shufflenum")
  }

  def readSource(ssc: StreamingContext): DStream[String] = {
    EventSourceReader.readSource(ssc, conf)
  }

  def transform(source: String): Option[SourceObject]

  def transformDF(sqlContext: SQLContext, labeledRDD: RDD[SourceObject]): DataFrame

  final def process(ssc: StreamingContext) = {
    val sqlContext = new SQLContext(ssc.sparkContext)
    val inputStream = readSource(ssc)

    inputStream.foreachRDD { rdd =>
      //      val currtime = timesdf.parse(timesdf.format(System.currentTimeMillis())).getTime
      val currtime = DateFormatUtils.dateStr2Ms(DateFormatUtils.dateMs2Str(System.currentTimeMillis(), timePattern), timePattern)

      if (currtime > morning8Time && currtime < afternoon8Time && rdd.partitions.length > 0) {
        val sourceRDD = rdd.map(transform).collect {
          case Some(source: SourceObject) => source
        }

        /*if (shuffleNum > 0) sourceRDD = sourceRDD.map(x => (x.generateId, x)).groupByKey(shuffleNum).flatMap(_._2)
        else sourceRDD = sourceRDD.map(x => (x.generateId, x)).groupByKey().flatMap(_._2)*/

        if (sourceRDD.partitions.length > 0) {
          val labeledRDD = execLabelRule(sourceRDD: RDD[SourceObject])
          if (labeledRDD.partitions.length > 0) {
            val df = transformDF(sqlContext, labeledRDD)
            // cache data
            df.persist

            val eventMap = makeEvents(df)

            subscribeEvents(eventMap)

            df.unpersist()
          }
        }
      }
    }
  }

  def subscribeEvents(eventMap: Map[String, DataFrame]) {
    println(" Begin subscribe events : " + System.currentTimeMillis())
    if (eventMap.size > 0) {

      eventMap.foreach(x => {
        x._2.persist()
      })

      val bsEventIter = bsEvents.iterator

      while (bsEventIter.hasNext) {
        val bsEvent = bsEventIter.next
        //println("= = " * 20 +"bsEvent.id = " + bsEvent.id)
        bsEvent.execEvent(eventMap)
      }

      eventMap.foreach(x => {
        x._2.unpersist()
      })
    }
  }

  def makeEvents(df: DataFrame) = {
    val eventMap: Map[String, DataFrame] = Map[String, DataFrame]()
    println(" Begin exec evets : " + System.currentTimeMillis())

    // println("* * " * 20 + " df.show")
    // df.show()
    // println("= = " * 20 + " df.show done")

    val f4 = System.currentTimeMillis()
    val eventRuleIter = eventRules.iterator

    while (eventRuleIter.hasNext) {
      makeEvent(df , eventRuleIter.next(), eventMap)
      // makeEvent(df , eventRuleIter.next(), eventMap, flagPrint = true) //forDebug
    }
    logDebug(" Exec eventrules cost time : " + (System.currentTimeMillis() - f4) + " millis ! ")

    eventMap
  }

  def makeEvent(df: DataFrame, eventRule: EventRule, eventResultMap: Map[String, DataFrame], flagPrint: Boolean = false): Map[String, DataFrame] ={
    val eventRuleConf = eventRule.conf
    val parentEventRuleId = eventRuleConf.get("parentEventRuleId")
    if(parentEventRuleId == "-1" || parentEventRuleId.isEmpty){ //如果不存在 parentEventRule，先判断 eventResultMap 中是否已存在 eventRule 对应结果集

      if(!eventResultMap.contains(eventRuleConf.get("id"))){ //如果不存在，直接过滤；如果存在，不做操作
        val filteredData = df.filter(eventRule.filterExp)
        eventResultMap.put(eventRuleConf.get("id"), filteredData)
      }
    } else { //如果存在 parentEventRule，先检查parentEventRule对应的结果集是否存在
      if(eventResultMap.contains(parentEventRuleId)){ //如果parent的结果集已经计算出来，在parent结果集之上进行过滤
        val parentDF = eventResultMap.get(parentEventRuleId).get
        val filteredData = parentDF.filter(eventRule.filterExp)
        eventResultMap.put(eventRuleConf.get("id"), filteredData)
      } else { //如果parent结果集没有计算出来，计算parent结果集
        val parentEventRule = eventRules.map(eventRule => {
          (eventRule.conf.get("id"), eventRule)
        }).toMap.get(parentEventRuleId)

        parentEventRule match {
          case Some(x) =>
            //计算parent eventRule的结果集
            makeEvent(df, parentEventRule.get, eventResultMap)
            //之后计算子 eventRule的结果集

            val parentDF = eventResultMap.get(parentEventRuleId).get
            val filteredData = parentDF.filter(eventRule.filterExp)
            eventResultMap.put(eventRuleConf.get("id"), filteredData)
          case None =>
            // 如果设置了 parentEventRuleId，但没有 parentEventRuleId 对应的eventRuleId, 设置其eventRule的结果集为空
            val filteredData = df.limit(0)
            eventResultMap.put(eventRuleConf.get("id"), filteredData)
        }
      }
    }

    if(flagPrint) eventResultMap.get(eventRuleConf.get("id")).get.show(10)

    eventResultMap
  }


  def execLabelRule(sourceRDD: RDD[SourceObject]) = {
    println(" Begin exec labes : " + System.currentTimeMillis())

    val labelRuleArray = labelRules.toArray
    sourceRDD.mapPartitions(iter => {
      new Iterator[SourceObject] {
        private[this] var currentRow: SourceObject = _
        private[this] var currentPos: Int = -1
        private[this] var arrayBuffer: Array[SourceObject] = _

        override def hasNext: Boolean = {
          val flag = (currentPos != -1 && currentPos < arrayBuffer.length) || (iter.hasNext && fetchNext())
          flag
        }

        override def next(): SourceObject = {
          currentPos += 1
          arrayBuffer(currentPos - 1)
        }

        private final def fetchNext(): Boolean = {
          val currentArrayBuffer = new ArrayBuffer[SourceObject]
          currentPos = -1
          var totalFetch = 0
          var result = false

          val totaldata = mutable.MutableList[SourceObject]()
          val minimap = mutable.Map[String, SourceObject]()

          val labelQryKeysSet = mutable.Set[String]()

          while (iter.hasNext && totalFetch < conf.getInt("batchsize")) {
            val currentLine = iter.next()
            totaldata += currentLine
            minimap += ("Label:" + currentLine.generateId -> currentLine)

            labelRuleArray.foreach(labelRule => {
              val labelId = labelRule.conf.get("id")
              val qryKeys = labelRule.getQryKeys(currentLine)
              if (qryKeys != null && qryKeys.nonEmpty) {
                labelQryKeysSet ++= qryKeys
              }
            })

            totalFetch += 1
            currentPos = 0
            result = true
          }

          println(" partition data size = " + totalFetch)

          val f1 = System.currentTimeMillis()
          var cachemap_old: Map[String, Any] = null
          try {
            cachemap_old = CacheFactory.getManager.getMultiCacheByKeys(minimap.keys.toList)
          } catch {
            case ex: Exception =>
              logError("= = " * 15 + " got exception in EventSource while get cache")
              throw ex
          }
          //val cachemap_old = CacheFactory.getManager.getByteCacheString(minimap.keys.head)
          val f2 = System.currentTimeMillis()
          println(" query label cache data cost time : " + (f2 - f1) + " millis ! ")


          val labelQryData = CacheFactory.getManager.hgetall(labelQryKeysSet.toList)
          val f3 = System.currentTimeMillis()
          println(" query label need data cost time : " + (f3 - f2) + " millis ! ")

          val cachemap_new = mutable.Map[String, Any]()
          totaldata.foreach(x => {
            val key = "Label:" + x.generateId
            val value = x

            var rule_caches = cachemap_old.get(key).get match {
              case cache: immutable.Map[String, StreamingCache] => cache
              case null => {
                val cachemap = mutable.Map[String, StreamingCache]()
                labelRuleArray.foreach(labelRule => {
                  cachemap += (labelRule.conf.get("id") -> null)
                })

                cachemap.toMap
              }
            }

            /*var rule_caches = cachemap_old match {
              case cache: immutable.Map[String, StreamingCache] => cache
              case null => {
                val cachemap = mutable.Map[String, StreamingCache]()
                labelRuleArray.foreach(labelRule => {
                  cachemap += (labelRule.conf.get("id") -> null)
                })

                cachemap.toMap
              }
            }*/

            labelRuleArray.foreach(labelRule => {

              val cacheOpt = rule_caches.get(labelRule.conf.get("id"))
              var old_cache: StreamingCache = null
              if (cacheOpt != None) old_cache = cacheOpt.get

              /*if("5".eq(labelRule.conf.get("id"))){
                println(" label " + labelRule.conf.get("id") + " qry datas : ")
                val ite = labelQryData.iterator
                if(ite.hasNext){
                  val v = ite.next()
                  println("key : " + v._1)
                  val ite2 = v._2.iterator
                  if(ite2.hasNext){
                    val v2 = ite2.next()
                    println("innerkey : " + v2._1)
                    println("innervalue : " + v2._2)
                  }
                }
              }*/

              val newcache = labelRule.attachLabel(value, old_cache, labelQryData)
              rule_caches = rule_caches.updated(labelRule.conf.get("id"), newcache)

            })
            currentArrayBuffer.append(value)

            cachemap_new += (key -> rule_caches.asInstanceOf[Any])
          })

          val f4 = System.currentTimeMillis()
          println(" Exec labels cost time : " + (f4 - f3) + " millis ! ")

          //update caches to CacheManager
          CacheFactory.getManager.setMultiCache(cachemap_new)
          //          CacheFactory.getManager.setByteCacheString(cachemap_new.head._1,cachemap_new.head._2)
          println(" update labels cache cost time : " + (System.currentTimeMillis() - f4) + " millis ! ")

          arrayBuffer = currentArrayBuffer.toArray
          result
        }
      }
    })
  }
}

