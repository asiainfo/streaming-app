package com.asiainfo.ocdc.streaming

abstract class MCLabelRule extends LabelRule {

  override def attachLabel(source: SourceObject, cache: StreamingCache) = source match {
    case so: MCSourceObject => attachMCLabel(so, cache)
    case _ => throw new Exception("")
  }

  def attachMCLabel(mc: MCSourceObject, cache: StreamingCache)
}
