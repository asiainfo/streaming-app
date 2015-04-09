package com.asiainfo.ocdc.streaming

import scala.beans.BeanProperty

/**
 * Created by tianyi on 3/30/15.
 */
case class MCSourceObject(
                           @BeanProperty val eventID: Int,
                           @BeanProperty val time: Long,
                           @BeanProperty val lac: Int,
                           @BeanProperty val ci: Int,
                           @BeanProperty val imei: Long,
                           @BeanProperty val imsi: Long) extends SourceObject {
  override def generateId = imsi.toString
}
