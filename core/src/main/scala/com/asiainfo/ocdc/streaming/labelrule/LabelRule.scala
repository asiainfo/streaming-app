package com.asiainfo.ocdc.streaming.labelrule

import com.asiainfo.ocdc.streaming.SourceObject
import com.asiainfo.ocdc.streaming.eventrule.StreamingCache
import scala.collection.mutable.Map

/**
 * Created by tianyi on 3/26/15.
 */
trait LabelRule extends Serializable {
  // load config from LabelRuleConf
  var conf: LabelRuleConf = null

  def init(lrconf: LabelRuleConf) {
    conf = lrconf
  }

  def attachLabel(source: SourceObject, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache

  def getQryKeys(source: SourceObject): String = null
}