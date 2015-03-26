package com.asiainfo.ocdc.streaming

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkConf

/**
 * Created by tianyi on 3/26/15.
 */
object MainFrame {
  def main(args: Array[String]): Unit = {
    // read config first
    StreamingConf.init()

    // init spark streaming context
    val sparkConf = new SparkConf().setAppName("OCDC-Streaming")
    val interval = StreamingConf.get(StreamingConf.STREAMING_INTERVAL)
    val ssc = new StreamingContext(sparkConf, Seconds(interval))

    // init all the eventsources
    val eventSourceList = StreamingConf.getDatasources.map(conf =>{
      // use reflect to create all eventsources
      val eventSource :EventSource =
        Class.forName(conf.classname).newInstance().asInstanceOf[EventSource]
      eventSource.init(conf)
      StreamingConf.getLabelRulesBySource(conf.name).map(labelRuleConf => {
        val labelRule :LabelRule =
          Class.forName(labelRuleConf.classname).newInstance().asInstanceOf[LabelRule]
        labelRule.init(labelRuleConf)
        eventSource.addLabelRule(labelRule)
      })

      StreamingConf.getEventRulesBySource(conf.name).map(eventRuleConf => {
        val eventRule :EventRule =
          Class.forName(eventRuleConf.classname).newInstance().asInstanceOf[EventRule]
        eventRule.init(eventRuleConf)
        eventSource.addEventRule(eventRule)
      })

      eventSource.process(ssc)
    })

  }
}
