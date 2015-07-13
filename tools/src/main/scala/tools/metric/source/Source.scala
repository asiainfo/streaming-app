package tools.metric.source

import com.codahale.metrics.MetricRegistry

/**
 * Created by tsingfu on 15/6/18.
 */
trait Source {
  def sourceName: String
  def metricRegistry: MetricRegistry
}
