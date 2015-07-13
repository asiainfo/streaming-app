package tools.metric

import java.io.IOException
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.ganglia.GangliaReporter
import com.codahale.metrics.{ConsoleReporter, MetricRegistry}
import info.ganglia.gmetric4j.gmetric.GMetric
import tools.metric.sink.{ConsoleSink, GangliaSink}

/**
 * Created by tsingfu on 15/5/12.
 */
object MetricUtils{

  //  val metricRegistry = new MetricRegistry()
  val metricsMap = new scala.collection.mutable.HashMap[String, String]()

  private val DEFAULT_METRICS_CONF_FILENAME = "metrics.properties"
  val metricProps = new Properties()
  metricProps.load(this.getClass.getClassLoader.getResourceAsStream(DEFAULT_METRICS_CONF_FILENAME))

  println("= = " * 20)
  metricProps.list(System.out)
  println("= = " * 20)

  def propertyToOption(prop: String): Option[String] = Option(metricProps.getProperty(prop))
  private[this] val MINIMAL_POLL_UNIT = TimeUnit.SECONDS
  private[this] val MINIMAL_POLL_PERIOD = 1

  def checkMinimalPollingPeriod(pollUnit: TimeUnit, pollPeriod: Long) {
    val period = MINIMAL_POLL_UNIT.convert(pollPeriod, pollUnit)
    if (period < MINIMAL_POLL_PERIOD) {
      throw new IllegalArgumentException("Polling period " + pollPeriod + " " + pollUnit +
              " below than minimal polling period ")
    }
  }


  def startConsoleReporter(periodMs: Long, metrics: MetricRegistry) {
    val reporter: ConsoleReporter = ConsoleReporter.forRegistry(metrics)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build

    reporter.start(periodMs / 1000, TimeUnit.SECONDS)
  }


  def startConsoleReporter(properties: Properties, metricRegistry: MetricRegistry) {
    val consoleSink = new ConsoleSink(properties, metricRegistry)
    consoleSink.start()
  }




  def startGangliaReporter(host: String, port: Int,
                           periodMs: Long,
                           mode: String, ttl: Int,
                           metricRegistry: MetricRegistry) {

    var uDPAddressingMode: GMetric.UDPAddressingMode = null
    if (mode.toLowerCase == "multicast") {
      uDPAddressingMode = GMetric.UDPAddressingMode.MULTICAST
    } else {
      uDPAddressingMode = GMetric.UDPAddressingMode.UNICAST
    }

    var ganglia: GMetric = null
    try {
      ganglia = new GMetric(host, port, uDPAddressingMode, 1)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }

    val reporter: GangliaReporter = GangliaReporter.forRegistry(metricRegistry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build(ganglia)

    reporter.start(periodMs / 1000, TimeUnit.SECONDS)
  }

  def startGangliaReporter(properties: Properties, metricRegistry: MetricRegistry) {
    val gangliaSink = new GangliaSink(properties, metricRegistry)
    gangliaSink.start()
  }

}


