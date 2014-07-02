package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming._
import org.apache.spark.streaming.dstream.DStream
import scala.xml.Node
import com.asiainfo.ocdc.streaming.tools._

class StreamSpilt extends StreamingStep{

  def onStep(step:Node,inStream:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]={
    val delim = ","
    val iSpilt = (step \ "Split").text.toString.trim
    val iField = (step \ "StreamFiled").text.toString.trim
    val output = (step \ "output").text.toString.trim.split(delim)
    val result = inStream.flatMap(x=>{
        //(11,p5|p2)(12,p2|p4)
       //(11,p5)(11,p2)
       //(11,p2)
       //(11,p5)
       val line = x.toMap
       //获得需要拆分的流字段的数据p1|p2|p3
       val iFieldMap =line.getOrElse(iField,iField).split(iSpilt)
       //流字段的差分p1,p2,p3   string
        iFieldMap.map(x=>{
         val tresult = (0 to output.length-1).map(i=>{
           var outResult = ("","")
           if(output(i).equals(iField)){
             println("Streamsplit 输出值:"+output(i) + "####" + x)
             outResult=(output(i),x)
           }
           else{
             println("Streamsplit 输出值:"+output(i) + "####" + line.getOrElse(output(i),output(i)) )
             outResult=(output(i),line.getOrElse(output(i),output(i)))
           }
           outResult
         }).toArray
         tresult
       })
    })
    result
  }


  override def check(step:Node){
    val delim = ","
    //判断字符串是否未设置,是否包含非法空格
    val checks = Array( "Split", "StreamFiled","output")
    checks.foreach(x=>(eleIsEmpty(step,x)))

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
