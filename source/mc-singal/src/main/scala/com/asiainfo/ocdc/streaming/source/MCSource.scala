package com.asiainfo.ocdc.streaming.source

import org.apache.spark.streaming.dstream.DStream

class MCEventSource(conf: String) extends EventSource(conf) {

  override def readSource: DStream[String] = {
     null
  }
  override def transform(source: String): Option[MCSourceObj] = {
     None
  }
}

class MCSourceObj(imsi: String) extends SourceObj {
}
