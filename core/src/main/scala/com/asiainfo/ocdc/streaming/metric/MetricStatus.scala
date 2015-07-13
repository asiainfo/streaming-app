package com.asiainfo.ocdc.streaming.metric

import java.util.concurrent.ConcurrentHashMap

/**
 * Created by tsingfu on 15/7/3.
 */
object MetricStatus{

  val metricsMap = new ConcurrentHashMap[String, ConcurrentHashMap[String, String]]()
  val START_TIME_KEY = "startTimeMs"
  val STOP_TIME_KEY = "stopTimeMs"
  val PROCESS_TIME_KEY = "processimeMS"

  def getMetrics(metricKey: String): ConcurrentHashMap[String, String] = {

    metricsMap.get(metricKey) match {
      case metrics: ConcurrentHashMap[String, String] =>
        metrics
      case null =>
        new ConcurrentHashMap[String, String]()
    }

  }

}