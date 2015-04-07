package com.asiainfo.ocdc.streaming

class MCEventSource() extends EventSource() {

  def formatSource(inputs: Array[String]): Option[MCSourceObject] = {
    // 事件ID,时间,LAC,CI,主叫IMEI,被叫IMEI,主叫IMSI,被叫IMSI
    try {
      val eventID = inputs(0).toInt
      val time = inputs(1).toLong
      val lac = inputs(2).toInt
      val ci = inputs(3).toInt
      val imei = inputs(4).toInt
      val imsi = inputs(6).toInt
      Some(MCSourceObject(eventID, time, lac, ci, imsi, imei))
    } catch {
      case e: Exception => {
        None
      }
    }
  }

  override def transform(source: String): Option[MCSourceObject] = {
    val inputArray = source.split(conf.get("source.format.delim"))
    if(source.length != conf.getInt("source.format.length")) {
      None
    } else {
      formatSource(inputArray)
    }
  }

  override def beanclass: String = MCSourceObject.getClass.getName
}


