package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.eventrule.EventRule
import com.asiainfo.ocdc.streaming.eventsource.EventSource
import com.asiainfo.ocdc.streaming.labelrule.LabelRule
import com.asiainfo.ocdc.streaming.subscribe.BusinessEvent
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by tianyi on 3/26/15.
 */
object MainFrame {
  def main(args: Array[String]): Unit = {

    if (args.length < 2) {
      System.err.println("Usage:  ./bin/spark-class com.asiainfo.ocdc.streaming.MainFrame [Master] [AppName]")
      System.exit(1)
    }

    // read config first
    //    CacheFactory.getManager

    // init spark streaming context
    val sparkConf = new SparkConf()
    sparkConf.setMaster(args(0))
    sparkConf.setAppName(args(1))

    val interval = MainFrameConf.getInternal
    val ssc = new StreamingContext(sparkConf, Seconds(interval))

    ssc.addStreamingListener(new ReceiveRecordNumListener)

    // init all the eventsources
    val eventSourceList = MainFrameConf.sources.map(conf => {
      // use reflect to create all eventsources
      val eventSource: EventSource =
        Class.forName(conf.getClassName()).newInstance().asInstanceOf[EventSource]
      eventSource.init(conf)
      MainFrameConf.getLabelRulesBySource(eventSource.id).map(labelRuleConf => {
        val labelRule: LabelRule =
          Class.forName(labelRuleConf.getClassName()).newInstance().asInstanceOf[LabelRule]
        labelRule.init(labelRuleConf)
        eventSource.addLabelRule(labelRule)
      })

      MainFrameConf.getEventRulesBySource(eventSource.id).map(eventRuleConf => {
        val eventRule: EventRule =
          Class.forName(eventRuleConf.getClassName()).newInstance().asInstanceOf[EventRule]
        eventRule.init(eventRuleConf)
        eventSource.addEventRule(eventRule)
      })

      MainFrameConf.getBsEventsBySource(eventSource.id).map(bsEvnetConf => {
        val bsEvent: BusinessEvent =
          Class.forName(bsEvnetConf.getClassName()).newInstance().asInstanceOf[BusinessEvent]
        bsEvent.init(eventSource.id, bsEvnetConf)
        eventSource.addBsEvent(bsEvent)
      })

      eventSource.process(ssc)
    })

    ssc.start()
    ssc.awaitTermination()
    exit()

  }
}
