package com.asiainfo.ocdc.streaming

import scala.beans.BeanProperty
import scala.collection.mutable.Map

/**
 * Created by tianyi on 3/30/15.
 */
case class MCSourceObject(
                           @BeanProperty val eventID: Int,
                           @BeanProperty val time: Long,
                           @BeanProperty val lac: String,
                           @BeanProperty val ci: String,
                           @BeanProperty val imsi: String,
                           @BeanProperty val imei: String,
                           @BeanProperty val eventresult: Int = 0,
                           @BeanProperty val alertstatus: Int = 0,
                           @BeanProperty val assstatus: Int = 0,
                           @BeanProperty val clearstatus: Int = 0,
                           @BeanProperty val relstatus: Int = 0,
                           @BeanProperty val xdrtype: Int = 0,
                           @BeanProperty val issmsalone: Int = 0,
                           @BeanProperty val labels: Map[String, Map[String, String]] = Map[String, Map[String, String]]()
                           ) extends SourceObject(labels) {

  override def generateId = imsi.toString

}
