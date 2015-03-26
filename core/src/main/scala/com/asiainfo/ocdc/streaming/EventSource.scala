package com.asiainfo.ocdc.streaming

import java.util.ArrayList

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.sql.{Row, SQLContext, DataFrame}

abstract class EventSource() {

  private val labelRules = new ArrayList[LabelRule]
  private val eventRules = new ArrayList[EventRule]
  private var beanclass = ""

  def addEventRule(rule: EventRule): Unit = eventRules.add(rule)

  def addLabelRule(rule: LabelRule): Unit = labelRules.add(rule)

  def init(conf: EventSourceConf): Unit

  // interface
  def readSource(ssc: StreamingContext): DStream[String]

  def transform(source: String): Option[SourceObject]

  final def process(ssc: StreamingContext) = {
    val sqlContext = new SQLContext(ssc.sparkContext)

    val inputStream = readSource(ssc)

    inputStream.foreachRDD { rdd =>
      val sourceRDD = rdd.map(transform).collect {
        case Some(source) => source
      }

      val labelRuleIter = labelRules.iterator
      while(labelRuleIter.hasNext) {
        sourceRDD.map(labelRuleIter.next.attachLabel)
      }
      val df = sqlContext.createDataFrame(sourceRDD, Class.forName(beanclass))

      // cache data
      df.persist

      val eventRuleIter = eventRules.iterator
      while(eventRuleIter.hasNext) {
        val eventRule = eventRuleIter.next
        val selectExprArray = eventRule.selExpr.map({
          case (alias :String, expr :String) => expr + " as " + alias
        })
        val targetEvent = df.filter(eventRule.filterExpr).selectExpr(selectExprArray.toSeq: _*)
        targetEvent.map(row =>{
          // TODO
          // row => Event
          // sendEvent
        })
      }
    }
  }
}

