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
    val HBaseTable = (step \ "HBaseTable").text.toString.trim
    val HBaseKey = (step \ "HBaseKey").text.toString.split(delim)
    val output = (step \ "output").text.toString.split(delim)
    val HBaseCells = (step \ "HBaseCells").text.toString.split(delim)
    var where = (step \ "where").text.toString // table1.city!=CITY_ID

    var handle = inStream.map(mapFunc = x => {
      var key = ""
      for (arg <- HBaseKey) {
        val item = x.toMap
        key += item.getOrElse(arg,"")
      }

      println("=============hbase 查询结果================" )
      HbaseTool.getValue(HBaseTable, key, HbaseTool.family, HBaseCells).foreach(x=>{println(x._1+"###"+x._2)})

      x ++HbaseTool.getValue(HBaseTable, key, HbaseTool.family, HBaseCells)
    })

    if(where != null){
      println("===where===" + where)
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


  override def check(step:Node){
    val delim = ","
    //判断字符串是否未设置,是否包含非法空格
    val checks = Array("HBaseTable","HBaseKey","output","HBaseCells")
    checks.foreach(x=>(eleIsEmpty(step,x)))

    // 判断表明是否大写
    val checkUpper = Array( "HBaseCells","HBaseTable")
    checkUpper.foreach(x=>{
      val tmp = (step \ x).text.toString.trim
      isUpper(tmp)
    })

    val output = (step \ "output").text.toString.trim.split(delim)
    output.foreach(x=>{
      if(x.indexOf(".")==1){
        val tmp = x.split(".")
        isUpper(tmp(0))
      }
    })
  }


  def isUpper(iString:String):Boolean={
    var result = true
    iString.foreach(x=>{
      if(x>='a' && x<='z'){
        result = false
        throw new Exception(this.getClass.getSimpleName +iString + "HBase表及列明应该大写")
      }
    })
    result
  }

  def eleIsEmpty(step:Node,element:String){
    val iString=(step \ element).text.toString.trim
    if(iString.isEmpty || iString ==null){
      throw new Exception(this.getClass.getSimpleName + "<"+element + ">" + "标签不能为空")
     }
    if(iString.indexOf(" ")== 1) {
      throw new Exception(this.getClass.getSimpleName + "<"+element + ">" + "字符串间不能包含空格")
    }
  }
}
