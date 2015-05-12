package com.asiainfo.ocdc.streaming.constant

/**
 * Created by leo on 5/12/15.
 */
object EventConstant {
  /**--------------------标签名称------------------------*/
  // business sale default interval
  val DEFAULTINTERVAL = 7 * 24 * 60 * 60 * 1000L

  // diff source default delay time
  val DEFAULTDELAYTIME = 30 * 60 * 1000L

  // when a user be saled, save it to codis as this field
  val LOCKTIMEFIELD = "locktime"
}
