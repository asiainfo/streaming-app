package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.constant.TableNameConstants
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils
import scala.collection.immutable
import scala.collection.mutable.Map

/**
 * Created by leo on 4/7/15.
 */
object EventSourceReader {
  def getEventSource(ssc: StreamingContext, conf: EventSourceConf): DStream[String] = {
    val sourceType = conf.get("type")
    val sourceId = conf.get("id")

    val sql = "select name,pvalue from " + TableNameConstants.EventSourceDetailTableName + " where esourceid = " + sourceId
    val result = JDBCUtils.query(sql)
    val map = Map[String, String]()
    result.foreach(x => {
      map.+=(x.get("name").get -> x.get("pvalue").get)
    })

    if ("kafka".equals(sourceType)) {
      val kafkaParams = immutable.Map("zookeeper.connect" -> map.get("zookeeper").get,
        "group.id" -> map.get("group").get,
        "auto.offset.reset" -> map.get("autooffset").get)
      val stream = KafkaUtils.createStream(
        ssc, kafkaParams, immutable.Map(map.get("topic").get -> 1), StorageLevel.MEMORY_ONLY)
      stream.map(_._2)

    } else if ("hdfs".equals(sourceType)) {
      ssc.textFileStream(map.get("path").get)
    } else {
      throw new Exception("EventSourceType " + sourceType + " is not support !")
    }
  }
}
