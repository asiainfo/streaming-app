package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.hbase.util.Bytes
import scala.xml.Node
import com.asiainfo.ocdc.streaming.tools._

class StreamFilter extends StreamingStep{

  def onStep(step:Node,inStream:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]={
    val delim = ","
    var result = inStream
    val HBaseTable = (step \ "HBaseTable").text.toString
    val HBaseKey = (step \ "HBaseKey").text.toString.split(delim)
    val output = (step \ "output").text.toString.split(delim)
    val HBaseCell = (step \ "HBaseCell").text.toString.split(delim)
    var where = (step \ "where").text.toString // table1.city!=CITY_ID

    var handle = inStream.map(mapFunc = x => {
      var key = ""
      for (arg <- HBaseKey) {
        val item = x.toMap
        key += item.getOrElse(arg,"")
      }
      x ++HbaseTool.getValue(HBaseTable, key, HbaseTool.family, HBaseCell)
    })

    if(where != null){
      handle = handle.filter(x=>{
        where = x.toMap.getOrElse(where,where)
        val b = (JexlTool.getExpValue(where, x.toArray)).toBoolean
        println("================StreamFilter 过滤条件为："+where+"==========判断结果为： "+b)
        b
      })
    }
    result  = handle.map(x=>{
      //如果input output相同的字段完全相同，说明不需要规整数据，不做map
      val item = x.toMap
      (0 to output.length-1).map(i=>{
        println("================StreamFilter 输出值: =====================" )
        print(output(i) + "####" + item.getOrElse(output(i),output(i)))
        (output(i),item.getOrElse(output(i),output(i)))
      }).toArray
    })
    result

  }
}
