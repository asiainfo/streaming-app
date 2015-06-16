package com.asiainfo.ocdc.streaming.labelrule

import com.asiainfo.ocdc.streaming.SourceObject
import com.asiainfo.ocdc.streaming.eventrule.StreamingCache

/**
 * Created by tianyi on 3/26/15.
 */
trait LabelRule extends Serializable {
  // load config from LabelRuleConf
  var conf: LabelRuleConf = null

  def init(lrconf: LabelRuleConf) {
    conf = lrconf
  }

  def attachLabel(source: SourceObject, cache: StreamingCache): StreamingCache

//  def getQryKey(source: SourceObject): String = null
}