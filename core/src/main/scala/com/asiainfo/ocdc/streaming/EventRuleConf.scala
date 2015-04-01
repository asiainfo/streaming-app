package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */
class EventRuleConf(conf: Map[String,String]) extends BaseConf(conf) {
  val classname: String = getString("eventrule.classname")
}
