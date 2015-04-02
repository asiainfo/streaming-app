package com.asiainfo.ocdc.streaming

import scala.beans.BeanProperty
object Constant {
  // 区域标签
  val LABEL_ONSITE = "onsite"
  // 持续停留标签
  val LABEL_STAY = "stay"
  // 在某区域的首条记录的时间
  val LABEL_STAY_FIRSTTIME = "firstTime"
  // 在某区域的最后一条记录的时间
  val LABEL_STAY_LASTTIME = "lastTime"
  // 连续停留时间
  val STAY_TIME = "lastTime"
}