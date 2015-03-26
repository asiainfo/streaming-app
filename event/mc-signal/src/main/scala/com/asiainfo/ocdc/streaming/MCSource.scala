package com.asiainfo.ocdc.streaming

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

class MCEventSource() extends EventSource() {

  override def readSource(ssc: StreamingContext): DStream[String] = {
     null
  }

  override def transform(source: String): Option[MCSourceObj] = {
     None
  }

  override def init(conf: EventSourceConf): Unit = {

  }
}

class MCSourceObj(imsi: String) extends SourceObject {
}
