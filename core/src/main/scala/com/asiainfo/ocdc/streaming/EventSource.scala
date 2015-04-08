package com.asiainfo.ocdc.streaming

import org.apache.spark.sql.SQLContext
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable
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
    EventSourceReader.readSource(ssc, conf)
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
          private[this] var currentPos: Int = -1
          private[this] var arrayBuffer: Array[SourceObject] = _

          override def hasNext: Boolean = (currentPos < arrayBuffer.length) || fetchNext()

          override def next(): SourceObject = {
            currentPos += 1
            arrayBuffer(currentPos - 1)
          }

          private final def fetchNext(): Boolean = {
            val currentArrayBuffer = new ArrayBuffer[SourceObject]
            currentPos = -1
            val totalFetch = 0
            var result = false

            val minimap = mutable.Map[String, SourceObject]()

            while (iter.hasNext && totalFetch < 10) {
              val currentLine = iter.next()
              minimap += (currentLine.id -> currentLine)
              totalFetch += 1
              currentPos = 0
              result = true
            }

            val caches = CacheFactory.getManager().getMultiCacheByKeys(minimap.keys.toSeq)
            var i = 0
            minimap.values.foreach(x -> {
              val cache = caches(i).asInstanceOf[StreamingCache]
              labelRuleArray.foreach(labelRule => {
                labelRule.attachLabel(x, cache)
              })
              currentArrayBuffer.append(x)
              i += 1
            })

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

