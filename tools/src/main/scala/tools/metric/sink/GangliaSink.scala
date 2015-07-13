package tools.metric.sink

import java.io.IOException
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.codahale.metrics.MetricRegistry
import com.codahale.metrics.ganglia.GangliaReporter
import info.ganglia.gmetric4j.gmetric.GMetric
import tools.metric.MetricUtils
import org.slf4j.LoggerFactory

/**
 * Created by tsingfu on 15/6/18.
 */
class GangliaSink(property: Properties, val registry: MetricRegistry) extends Sink{

  val logger = LoggerFactory.getLogger(this.getClass)

  val GANGLIA_DEFAULT_HOST = "239.2.11.71"
  val GANGLIA_DEFAULT_PORT = 9649
  val GANGLIA_DEFAULT_PERIOD = 10
  val GANGLIA_DEFAULT_UNIT = "SECONDS"
  val GANGLIA_DEFAULT_MODE = "multicast"
  val GANGLIA_DEFAULT_TTL = 1

  val GANGLIA_KEY_HOST = "app.sink.ganglia.host"
  val GANGLIA_KEY_PORT = "app.sink.ganglia.port"
  val GANGLIA_KEY_PERIOD = "app.sink.ganglia.period"
  val GANGLIA_KEY_UNIT = "app.sink.ganglia.unit"
  val GANGLIA_KEY_MODE = "app.sink.ganglia.mode"
  val GANGLIA_KEY_TTL = "app.sink.ganglia.ttl"

  val gangliaHost = Option(property.getProperty(GANGLIA_KEY_HOST)) match {
    case Some(s) => s
    case None => GANGLIA_DEFAULT_HOST
  }

  val gangliaPort = Option(property.getProperty(GANGLIA_KEY_PORT)) match {
    case Some(s) => s.toInt
    case None => GANGLIA_DEFAULT_PORT
  }


  val pollPeriod = Option(property.getProperty(GANGLIA_KEY_PERIOD)) match {
    case Some(s) => s.toLong
    case None => GANGLIA_DEFAULT_PERIOD
  }

  val pollUnit: TimeUnit = Option(property.getProperty(GANGLIA_KEY_UNIT)) match {
    case Some(s) => TimeUnit.valueOf(s.toUpperCase)
    case None => TimeUnit.valueOf(GANGLIA_DEFAULT_UNIT)
  }

  val gangliaMode = Option(property.getProperty(GANGLIA_KEY_MODE)) match {
    case Some(s) => s
    case None => GANGLIA_DEFAULT_MODE
  }

  val gangliaTtl = Option(property.getProperty(GANGLIA_KEY_TTL)) match {
    case Some(s) => s.toInt
    case None => GANGLIA_DEFAULT_TTL
  }

  MetricUtils.checkMinimalPollingPeriod(pollUnit, pollPeriod)

  val uDPAddressingMode: GMetric.UDPAddressingMode =
    if (gangliaMode.toLowerCase == "multicast") {
      GMetric.UDPAddressingMode.MULTICAST
    } else {
      GMetric.UDPAddressingMode.UNICAST
    }

  var ganglia: GMetric = null
  try {
    logger.info("Initializing Ganglia GMetric, params: gangliaHost = "+gangliaHost+
            ", gangliaPort = "+gangliaPort+
            ", uDPAddressingMode = "+ uDPAddressingMode+
            ", gangliaTtl = "+gangliaTtl)
    ganglia = new GMetric(gangliaHost, gangliaPort, uDPAddressingMode, gangliaTtl)
  } catch {
    case e: IOException =>
      e.printStackTrace()
  }

  val reporter: GangliaReporter = GangliaReporter.forRegistry(registry)
          .convertRatesTo(TimeUnit.SECONDS)
          .convertDurationsTo(TimeUnit.MILLISECONDS)
          .build(ganglia)


  override def start() {
    logger.info("GangliaReporter starting, params: pollPeriod = "+pollPeriod+", pollUnit="+pollUnit)
    reporter.start(pollPeriod, pollUnit)
  }

  override def stop() {
    reporter.stop()
  }

  override def report() {
    reporter.report()
  }
}
