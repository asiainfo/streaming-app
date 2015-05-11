package com.asiainfo.ocdc.streaming.subscribe

import com.asiainfo.ocdc.streaming.BaseConf

/**
 * Created by leo on 5/11/15.
 */
class BusinessEventConf(conf: Map[String,String] = null) extends BaseConf(conf) {
  def getClassName(): String = get("classname")
}
