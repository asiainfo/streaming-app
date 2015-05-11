package com.asiainfo.ocdc.streaming.labelrule

import com.asiainfo.ocdc.streaming.BaseConf

/**
 * Created by tianyi on 3/26/15.
 */
class LabelRuleConf(conf: Map[String,String] = null) extends BaseConf(conf) {

  def getClassName(): String = get("classname")

}
