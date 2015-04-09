package com.asiainfo.ocdc.streaming

import org.apache.spark.sql.DataFrame

abstract class EventRule extends Serializable {

  var conf: EventRuleConf = null
  var selectExp: Seq[String] = null
  var filterExp: String = null

  def init(erconf: EventRuleConf) {
    conf = erconf
    selectExp = conf.get("selectExp").split(",").toSeq
    filterExp = conf.get("filterExp")
  }

  def output(data: DataFrame)

}