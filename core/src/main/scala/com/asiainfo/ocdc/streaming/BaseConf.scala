package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/30/15.
 */
abstract class BaseConf(val conf: Map[String, String]) {
  def getInt(s: String): Int = ???
  def getString(s: String): String = ???
}
