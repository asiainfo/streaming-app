package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/30/15.
 */
case class MCSourceObject(
    eventID: Int,
    time: Long,
    lac: Int,
    ci: Int,
    imsi: Long,
    imei: Long) extends SourceObject {
}
