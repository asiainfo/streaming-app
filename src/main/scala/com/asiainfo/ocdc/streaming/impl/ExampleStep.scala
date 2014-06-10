package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming._
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.ocdc.streaming.tools.HbaseTable
import org.apache.hadoop.hbase.util.Bytes

class ExampleStep(input:String,table:String,key:String,col:String,where:String,output:String)
  extends StreamingStep{

  def onStep(inStream:DStream[Array[String]]):DStream[Array[String]]={
    val inputItem=output.split(",")
    var handle = inStream.map(x=>{
      val imap = (0 to x.length-1).map(i=>(inputItem(i),x(i)))
      val row = HbaseTable.getValue(table,key)
      col.split(",").map(c=>{
        (table+"."+c,Bytes.toString(row.getValue(Bytes.toBytes("f1"), Bytes.toBytes(c))))
      })++imap
    })
    if(!where.equals("")){
      handle = handle.filter(getResult(where,_))
    }
    val outputItem=output.split(",")
    handle.map(x=>{
      val item = x.toMap
      (0 to outputItem.length-1).map(i=>item.getOrElse(outputItem(i),outputItem(i))).toArray
    })
  }
}
