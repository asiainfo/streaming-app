package com.asiainfo.ocdc.streaming

import org.apache.commons.lang.StringUtils
import org.apache.spark.Logging


/**
 * Utility trait for classes that want to log data. This wraps around Spark's
 * Logging trait. It creates a SLF4J logger for the class and allows logging
 * messages at different levels using methods that only evaluate parameters
 * lazily if the log level is enabled.
 *
 * It differs from the Spark's Logging trait in that it can print out the
 * error to the specified console of the Hive session.
 */

/**
 * Created by liuhao8 on 14-6-26.
 */
trait LogHelper extends Logging {

  override def logError(msg: => String) = {
    System.out.println(msg)
    super.logError(msg)
  }

  def logError(msg: String, detail: String) = {
    System.out.println(msg)
    super.logError(msg + StringUtils.defaultString(detail))
  }

  def logError(msg: String, exception: Throwable) = {
    val err = System.out
    err.println(msg)
    exception.printStackTrace(err)
    super.logError(msg, exception)
  }
}
