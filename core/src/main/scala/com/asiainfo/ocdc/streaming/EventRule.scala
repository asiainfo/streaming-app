package com.asiainfo.ocdc.streaming

trait EventRule extends Serializable{
  def init(conf: EventRuleConf) = {}

  val selectExpMap: Map[String, String]
  val filterExpList: List[String]

  def getSelectExprs = selectExpMap.map({
    case (alias :String, expr :String) => expr + " as " + alias
  }).toSeq
}