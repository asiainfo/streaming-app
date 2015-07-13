package tools.metric.sink

/**
 * Created by tsingfu on 15/6/18.
 */
trait Sink {
  def start(): Unit
  def stop(): Unit
  def report(): Unit
}