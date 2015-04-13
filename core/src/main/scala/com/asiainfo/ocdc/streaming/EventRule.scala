package com.asiainfo.ocdc.streaming

import org.apache.spark.rdd.RDD
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

  def getDelim: String = conf.get("delim")
  def inputLength: Int = conf.getInt("inputLength")

  def transforEvent2Message(data: DataFrame): RDD[String]
  def transforMessage2Event(message: RDD[String]): RDD[Option[SourceObject]]
  def output(data: DataFrame)
}