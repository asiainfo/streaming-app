package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.eventrule.StreamingCache
import com.asiainfo.ocdc.streaming.labelrule.LabelRule

abstract class MCLabelRule extends LabelRule {

  override def attachLabel(source: SourceObject, cache: StreamingCache) = source match {
    case so: MCSourceObject => attachMCLabel(so, cache)
    case _ => throw new Exception("")
  }

  def attachMCLabel(mc: MCSourceObject, cache: StreamingCache): StreamingCache

//  override def getQryKeys(mc: MCSourceObject): String = mc.generateId
}
