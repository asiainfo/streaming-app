package com.asiainfo.ocdc.streaming

import kafka.serializer.StringDecoder
import com.asiainfo.ocdc.streaming.constant.TableNameConstants
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by leo on 4/1/15.
 */
object EventSourceFactory {

  def getEventSource(ssc: StreamingContext, sourceType: String, id: Int): DStream[String] = {

    if ("kafka".equals(sourceType)) {
      val sql = "select id,topic,groupid,zookeeper,brokerlist,serializerclass,msgkey,autooffset from " + TableNameConstants.KafkaSourceTableName
      val result = JDBCUtils.query(sql)
      val kafkaParams = Map(
      //      "zookeeper.connect" -> conf.getString("source.zookeeper"),
      //      "group.id" -> s"test-consumer-${Random.nextInt(10000)}",
      //      "auto.offset.reset" -> "smallest")

      //    val topic = conf.getString("source.topic")
          val stream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](
            ssc, kafkaParams, Map(topic -> 1), StorageLevel.MEMORY_ONLY)
          stream.map(_._2)

    } else if ("hdfs".equals(sourceType)) {

    } else {

    }

  }

}
