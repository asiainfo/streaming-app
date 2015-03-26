package com.asiainfo.ocdc.streaming

trait EventRule extends Serializable{
  def init(conf: EventRuleConf) = {}

  val selExpr: Map[String, String]
  val filterExpr: String
}