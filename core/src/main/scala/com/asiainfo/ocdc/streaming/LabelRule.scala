package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */
trait LabelRule extends Serializable {
  // load config from LabelRuleConf
  var conf: LabelRuleConf

  def init(lrconf: LabelRuleConf) {
    conf = lrconf
  }

  def attachLabel(source: SourceObject, cache: StreamingCache)
}