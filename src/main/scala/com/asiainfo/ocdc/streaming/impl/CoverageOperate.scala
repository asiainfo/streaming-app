package com.asiainfo.ocdc.streaming.impl

import org.apache.spark.Logging
import com.asiainfo.ocdc.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.hbase.util.Bytes
import scala.xml.Node
import com.asiainfo.ocdc.streaming.tools._

class CoverageOperate extends StreamingStep with Logging {

  def onStep(step: Node, inStream: DStream[Array[(String, String)]]): DStream[Array[(String, String)]] = {

		val table = (step \ "HBaseTable").text.toString.trim
		val key = (step \ "HBaseKey").text.toString.trim
		val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",")
		val expressions = (step \ "expressions").text.toString.trim.split(",")
		val output = (step \ "output").text.toString.trim.split(",")
		
		val handle = inStream.map(record => {
		var recordMap = record.toMap
		
		val expeValue = for { index <- 0 until expressions.size } yield (JexlTool.getExpValue(expressions(index), recordMap.toArray))
		
		val cellexp = hBaseCells.zip(expeValue)
		HbaseTool.putValue(table, recordMap(key), HbaseTool.family, cellexp)
		
		record
    })
    handle
  }

}