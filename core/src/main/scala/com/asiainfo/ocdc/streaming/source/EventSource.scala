package com.asiainfo.ocdc.streaming.source

import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.ocdc.streaming.rule.{EventRule, LabelRule}

abstract class EventSource(conf: String) {
  private val (labelRules, eventRules) = {
    //read conf
    //反射加载rules
    (Seq[LabelRule](), Seq[EventRule]())
  }
  def readSource: DStream[String]
  def transform(source: String): Option[SourceObj]
  final def process() = {
    //read stream
    readSource.foreachRDD{rdd =>

      //transform
      //遍历labelRules，执行attachLabel
      //生成df
      //遍历eventRules，执行expr，send topic
    }
  }
}

abstract class SourceObj extends Serializable {
  private val labels = new java.util.HashMap[String, Object]()
  final def setLabel(key: String, value: Object) = {
    labels.put(key, value)
  }
  final def removeLabel(key: String) = {
    labels.remove(key)
  }
}
