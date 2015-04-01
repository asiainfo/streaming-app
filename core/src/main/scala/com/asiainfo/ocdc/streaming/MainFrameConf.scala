package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */

import java.util.HashMap

object MainFrameConf {

  def getEventRulesBySource(value: String) = sourceEventRules.get(value)

  def getLabelRulesBySource(value: String) = sourceLabelRules.get(value)

  var sourceLabelRules = new HashMap[String, Seq[LabelRuleConf]]()
  var sourceEventRules = new HashMap[String, Seq[EventRuleConf]]()
  var sources = Array[EventSourceConf]()

  def getInternal: Long = 20


  def init(): Unit = {
    // TODO

    // read event source list and config
    JDBCUtils.query("select * from EVENT_SOURCE")

  }

}
