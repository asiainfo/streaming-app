package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */
trait LabelRule extends Serializable{
  // load config from LabelRuleConf
  def init(conf: LabelRuleConf) = ???

  def attachLabel(source: SourceObject, cache: StreamingCache)
}