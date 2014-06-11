package com.asiainfo.ocdc.streaming


import org.apache.commons.jexl2._
import scala.Array
import scala.xml.{Node, XML}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf


object StreamingApp {
  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: <jobXMLPath>")
      System.exit(1)
    }
    val Array(jobConfFile) = args

    val sparkConf = new SparkConf().setAppName("StreamingApp")
    val ssc =  new StreamingContext(sparkConf, Seconds(2))

     val xmlFile = XML.load(jobConfFile)
     val dataSource = xmlFile \ "DataSource"
     val clz = Class.forName((dataSource \ "class").text.toString)
     val method = clz.getDeclaredMethod("createStream",classOf[Node])
     var streamingData = method.invoke(clz.getConstructor(classOf[StreamingContext]).newInstance(ssc),dataSource)

     val steps = xmlFile \ "step"
     for(step <- steps){
       val clz = Class.forName((step \ "class").text.toString)
       val method = clz.getDeclaredMethod("onStep",classOf[Node], classOf[DStream[Array[String]]])
       streamingData = method.invoke(clz.newInstance(), step,streamingData)
     }
    streamingData.asInstanceOf[DStream[Array[String]]].print
    ssc.start()
    ssc.awaitTermination()
   }
 }

abstract class StreamingStep{

  val engine=new JexlEngine()

  def getResult(str:String,param:Array[(String,String)]):Boolean = {
    val context = new MapContext()
    param.foreach(x=>context.set(x._1,x._2))
    engine.createExpression(str).evaluate(context).toString.toBoolean
  }

  def onStep(step:Node,input:DStream[Array[String]]):DStream[Array[String]]
}

abstract class StreamingSource(sc:StreamingContext){
	val abc = ""
  def createStream(source:Node):DStream[Array[String]]
}