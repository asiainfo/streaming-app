package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.constant.TableNameConstants
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka.KafkaUtils

/**
 * Created by leo on 4/1/15.
 */
object EventSourceFactory {

  def getEventSource(ssc: StreamingContext, sourceType: String, id: Int): DStream[String] = {
    if ("kafka".equals(sourceType)) {
      val sql = "select id,topic,groupid as group.id,zookeeper as zookeeper.connect,brokerlist,serializerclass,msgkey,autooffset as auto.offset.reset from " + TableNameConstants.KafkaSourceTableName
      val result = JDBCUtils.query(sql)(0)
      val stream = KafkaUtils.createStream(
        ssc, result, Map(result.get("topic").get -> 1), StorageLevel.MEMORY_ONLY)
      stream.map(_._2)

    } else if ("hdfs".equals(sourceType)) {
      null
    } else {
      null
    }
  }

}
