package tools.metric.sink

import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import tools.metric.MetricUtils

/**
 * Created by tsingfu on 15/6/18.
 */
class ConsoleSink (property: Properties, val registry: MetricRegistry) extends Sink{

  val CONSOLE_DEFAULT_PERIOD = 10
  val CONSOLE_DEFAULT_UNIT = "SECONDS"

  val CONSOLE_KEY_PERIOD = "app.sink.console.period"
  val CONSOLE_KEY_UNIT = "app.sink.console.unit"

  val pollPeriod = Option(property.getProperty(CONSOLE_KEY_PERIOD)) match {
    case Some(s) => s.toInt
    case None => CONSOLE_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(CONSOLE_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase)
    case None => TimeUnit.valueOf(CONSOLE_DEFAULT_UNIT)
  }

  MetricUtils.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val reporter: ConsoleReporter = ConsoleReporter.forRegistry(registry)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .convertRatesTo(TimeUnit.SECONDS)
          .build()

  override def start() {
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
