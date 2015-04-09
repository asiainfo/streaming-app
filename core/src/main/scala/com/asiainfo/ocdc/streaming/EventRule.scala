package com.asiainfo.ocdc.streaming

trait EventRule extends Serializable {

  var conf: EventRuleConf
  val selectExpMap: Map[String, String]
  val filterExpList: List[String]

  def init(erconf: EventRuleConf) {
    conf = erconf
  }


  def getSelectExprs = selectExpMap.map({
    case (alias: String, expr: String) => expr + " as " + alias
  }).toSeq
}