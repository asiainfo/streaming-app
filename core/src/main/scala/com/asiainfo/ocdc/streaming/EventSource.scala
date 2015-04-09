package com.asiainfo.ocdc.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

abstract class EventSource() extends Serializable {
  var id: String = null
  var conf: EventSourceConf = null

  private val labelRules = new ArrayBuffer[LabelRule]
  private val eventRules = new ArrayBuffer[EventRule]

  def beanclass: String

  def addEventRule(rule: EventRule): Unit = {
    eventRules += rule
  }

  def addLabelRule(rule: LabelRule): Unit = {
    labelRules += rule
  }

  def init(conf: EventSourceConf): Unit = {
    this.conf = conf
    id = this.conf.get("id")
  }

  def readSource(ssc: StreamingContext): DStream[String] = {
    EventSourceReader.readSource(ssc, conf)
  }

  def transform(source: String): Option[SourceObject]

  final def process(ssc: StreamingContext) = {
    val sqlContext = new SQLContext(ssc.sparkContext)

    val inputStream = readSource(ssc)

    inputStream.foreachRDD { rdd =>

      if (rdd.partitions.length > 0) {

        val l = rdd.map(transform).partitions.length

        val sourceRDD = rdd.map(transform).collect {
          case Some(source: SourceObject) => source
        }

        val labelRuleArray = labelRules.toArray

        val labeledRDD = sourceRDD.mapPartitions(iter => {
          new Iterator[SourceObject] {
            private[this] var currentRow: SourceObject = _
            private[this] var currentPos: Int = -1
            private[this] var arrayBuffer: Array[SourceObject] = _

            override def hasNext: Boolean = (currentPos != -1 && currentPos < arrayBuffer.length) || fetchNext()

            override def next(): SourceObject = {
              currentPos += 1
              arrayBuffer(currentPos - 1)
            }

            private final def fetchNext(): Boolean = {
              val currentArrayBuffer = new ArrayBuffer[SourceObject]
              currentPos = -1
              var totalFetch = 0
              var result = false

              val minimap = mutable.Map[String, SourceObject]()

              while (iter.hasNext && totalFetch < 10) {
                val currentLine = iter.next()
                minimap += (currentLine.generateId -> currentLine)
                totalFetch += 1
                currentPos = 0
                result = true
              }

              val cachemap_old = CacheFactory.getManager().getMultiCacheByKeys(minimap.keys.toList)
              val cachemap_new = minimap.map(x => {
                val key = x._1
                val value = x._2
                val rule_caches = cachemap_old.get(key).get.asInstanceOf[Map[String, StreamingCache]]
                labelRuleArray.foreach(labelRule => {
                  val cache = rule_caches.get(labelRule.conf.get("id")).get
                  labelRule.attachLabel(value, cache)
                })
                currentArrayBuffer.append(value)
                (key, rule_caches.asInstanceOf[Any])
              })

              //update caches to CacheManager
              CacheFactory.getManager().setMultiCache(cachemap_new)

              arrayBuffer = currentArrayBuffer.toArray
              result
            }
          }
        })

        println(labeledRDD.count())

        /* val df = sqlContext.createDataFrame(labeledRDD, Class.forName(beanclass))

         // cache data
         df.persist
         df.count()*/
        /*val eventRuleIter = eventRules.iterator
        while (eventRuleIter.hasNext) {
          val eventRule = eventRuleIter.next

          // handle filter first
          val filteredData = {
            var inputDF = df
            for (filter: String <- eventRule.filterExpList) {
              inputDF = inputDF.filter(filter)
            }
            inputDF
          }

          // handle select
          val selectedData = filteredData.selectExpr(eventRule.getSelectExprs: _*)

          selectedData.map(row => {
            // TODO
            // row => Event
            // sendEvent
          }).count()
        }*/
      }
    }
  }
}

