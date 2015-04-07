package com.asiainfo.ocdc.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer


abstract class EventSource() {
  val name: String = ""
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
  }

  def readSource(ssc: StreamingContext): DStream[String] = {
    EventSourceReader.getEventSource(ssc, conf)
  }

  def transform(source: String): Option[SourceObject]

  final def process(ssc: StreamingContext) = {
    val sqlContext = new SQLContext(ssc.sparkContext)

    val inputStream = readSource(ssc)

    inputStream.foreachRDD { rdd =>
      val sourceRDD = rdd.map(transform).collect {
        case Some(source: SourceObject) => source
      }

      val labelRuleArray = labelRules.toArray

      val labeledRDD = sourceRDD.mapPartitions(iter => {
        new Iterator[SourceObject] {
          private[this] var currentRow: SourceObject = _
          private[this] var currentPos: Int = 0
          private[this] var arrayBuffer: Array[SourceObject] = null

          override def hasNext: Boolean = (currentPos < arrayBuffer.length - 1) || (iter.hasNext && fetchNext())

          override def next(): SourceObject = {
            currentPos += 1
            arrayBuffer(currentPos - 1)
          }

          private final def fetchNext(): Boolean = {
            val currentArrayBuffer = new ArrayBuffer[SourceObject]
            currentPos = 0
            val totalFetch = 0
            var result = false
            // TODO read caches from CacheManager
            val cache = new StreamingCache
            while (iter.hasNext && totalFetch < 10) {
              val currentLine = iter.next()
              result = true
              labelRuleArray.map(labelRule => {
                labelRule.attachLabel(currentLine, cache)
              })
              currentArrayBuffer.append(currentLine)
            }
            // TODO update caches to CacheManager
            arrayBuffer = currentArrayBuffer.toArray
            result
          }
        }
      })
      val df = sqlContext.createDataFrame(labeledRDD, Class.forName(beanclass))

      // cache data
      df.persist

      val eventRuleIter = eventRules.iterator
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
        })
      }
    }
  }
}

