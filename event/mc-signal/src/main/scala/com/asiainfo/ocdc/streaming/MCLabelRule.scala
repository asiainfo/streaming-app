package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.eventrule.StreamingCache
import com.asiainfo.ocdc.streaming.labelrule.LabelRule

import scala.collection.mutable.Map

abstract class MCLabelRule extends LabelRule {

  override def attachLabel(source: SourceObject, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]) = source match {
    case so: MCSourceObject => attachMCLabel(so, cache, labelQryData)
    case _ => throw new Exception("")
  }

  def attachMCLabel(mc: MCSourceObject, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache

}
