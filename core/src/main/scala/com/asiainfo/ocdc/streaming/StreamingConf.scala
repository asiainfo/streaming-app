package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */

import java.util.HashMap

object StreamingConf {
  val STREAMING_INTERVAL = "streaming-inteval"

  def getEventRulesBySource(value: Nothing) = sourceEventRules.get(value)

  def getLabelRulesBySource(value: String) = sourceLabelRules.get(value)

  var sourceLabelRules = new HashMap[String, Seq[LabelRuleConf]]()
  var sourceEventRules = new HashMap[String, Seq[EventRuleConf]]()
  var sources = List[EventSourceConf]()

  def getDatasources = sources

  def get(key: String): Long = { 20 }


  def init(): Unit = {
    // TODO
  }

}
