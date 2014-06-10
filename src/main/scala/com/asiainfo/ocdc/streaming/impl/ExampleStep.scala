package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming._
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.ocdc.streaming.tools.HbaseTable
import org.apache.hadoop.hbase.util.Bytes
import scala.xml.Node

class ExampleStep extends StreamingStep{

  def onStep(step:Node,inStream:DStream[Array[String]]):DStream[Array[String]]={
    val input = (step \ "input").text.toString.split(",")
    val output = (step \ "output").text.toString.split(",")
    val table = (step \ "HBaseTable").text.toString
    val col = (step \ "HBaseCol").text.toString
    val key = (step \ "HBaseKey").text.toString
    val where = (step \ "where").text.toString
    var handle = inStream.map(x=>{
      val imap = (0 to x.length-1).map(i=>(input(i),x(i)))
      val row = HbaseTable.getValue(table,key)
      col.split(",").map(c=>{
        (table+"."+c,Bytes.toString(row.getValue(Bytes.toBytes("f1"), Bytes.toBytes(c))))
      })++imap
    })
    if(!where.equals("")){
      handle = handle.filter(getResult(where,_))
    }
    handle.map(x=>{
      val item = x.toMap
      (0 to output.length-1).map(i=>item.getOrElse(output(i),output(i))).toArray
    })
  }
}
