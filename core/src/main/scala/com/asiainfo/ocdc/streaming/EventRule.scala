package com.asiainfo.ocdc.streaming

trait EventRule extends Serializable {

  var conf: EventRuleConf

  def init(erconf: EventRuleConf) {
    conf = erconf
  }

  val selectExpMap: Map[String, String]
  val filterExpList: List[String]

  def getSelectExprs = selectExpMap.map({
    case (alias: String, expr: String) => expr + " as " + alias
  }).toSeq
}