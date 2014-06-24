package com.asiainfo.ocdc.streaming

import scala.Array
import scala.xml.{Node, XML}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka.KafkaUtils


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
     val dataSource = xmlFile \ "dataSource"
     val clz = Class.forName((dataSource \ "class").text.toString)
     val method = clz.getDeclaredMethod("createStream",classOf[Node])
     val topicpMap = "abcdef".split(",").map((_,1)).toMap
     KafkaUtils.createStream(ssc, "", "group", topicpMap)
     var streamingData = method.invoke(clz.getConstructor(classOf[StreamingContext]).newInstance(ssc),dataSource(0))

     val steps = xmlFile \ "step"
     for(step <- steps){
       val clz = Class.forName((step \ "class").text.toString)
       val method = clz.getMethod()("runStep",classOf[Node], classOf[DStream[Array[(String,String)]]])
       streamingData = method.invoke(clz.newInstance(), step,streamingData)
     }
    streamingData.asInstanceOf[DStream[Array[(String,String)]]].print()
    ssc.start()
    ssc.awaitTermination()
   }
 }

abstract class StreamingStep{
  def runStep(step:Node,input:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]={
      println("===================="+this.getClass.getSimpleName+" is running ! =====================")
      val stepResult = this.onStep(step,input)
      println("===================="+this.getClass.getSimpleName+" results is : =====================")
      stepResult.map(x => {
        x.map(_._2).foreach(println(_))
      })
      stepResult
  }

  def onStep(step:Node,input:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]

}

abstract class StreamingSource(sc:StreamingContext){

  def createStream(source:Node):DStream[Array[(String,String)]]
}