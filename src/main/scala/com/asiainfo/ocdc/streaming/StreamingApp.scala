package com.asiainfo.ocdc.streaming


import org.apache.commons.jexl2._
import scala.Array
import scala.xml.XML
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._


object StreamingApp {
  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: <jobXMLPath>")
      System.exit(1)
    }
    val Array(jobConfFile) = args

     val xmlFile = XML.load(jobConfFile)
     val source = (xmlFile \ "source")
     val clz = Class.forName((source \ "class").text.toString)
     val method = clz.getDeclaredMethod("createStream")
     var streamingData = method.invoke(clz.newInstance())

     val steps = (xmlFile \ "step")
     for(step <- steps){
       val clz = Class.forName((step \ "class").text.toString)
       val method = clz.getDeclaredMethod("onStep", classOf[DStream[Array[String]]])
       streamingData = method.invoke(clz.newInstance(), streamingData)
     }
   }
 }

trait StreamingStep{

  val engine=new JexlEngine()

  def getResult(str:String,param:Array[(String,String)]):Boolean = {
    val context = new MapContext()
    param.foreach(x=>context.set(x._1,x._2))
    engine.createExpression(str).evaluate(context).toString.toBoolean
  }

  def onStep(input:DStream[Array[String]]):DStream[Array[String]]
}

trait StreamingSource{

  def createStream():DStream[Array[String]]
}