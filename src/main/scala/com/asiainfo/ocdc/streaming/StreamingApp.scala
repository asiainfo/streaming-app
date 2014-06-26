package com.asiainfo.ocdc.streaming

import scala.Array
import scala.xml.{Node, XML}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkConf


object StreamingApp {
  def main(args: Array[String]) {

    if (args.length < 1) {
      System.err.println("Usage: <jobXMLPath>")
      System.exit(1)
    }
    val Array(appName,stream_space,jobConfFile) = args

    val xmlFile = XML.load(jobConfFile)
    val dataSource = xmlFile \ "dataSource"
    val clz = Class.forName((dataSource \ "class").text.toString)
    val method = clz.getDeclaredMethod("createStream",classOf[Node])

    val sparkConf = new SparkConf().setAppName(appName)
    val ssc =  new StreamingContext(sparkConf, Seconds(stream_space.toInt))

    var streamingData = method.invoke(clz.getConstructor(classOf[StreamingContext]).newInstance(ssc),dataSource(0))

     val steps = xmlFile \ "step"
     for(step <- steps){
       val clz = Class.forName((step \ "class").text.toString)
       val method = clz.getDeclaredMethod("run",classOf[Node], classOf[DStream[Array[(String,String)]]])
       streamingData = method.invoke(clz.newInstance(), step,streamingData)
     }
    streamingData.asInstanceOf[DStream[Array[(String,String)]]].print()
    ssc.start()
    ssc.awaitTermination()
   }
 }

abstract class StreamingStep(){

  /**
   * Step 运行主方法
   * @param step
   * @param input
   * @return
   */
  def run(step:Node,input:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]={
    // 预检查
    check(step)

    // 流处理主操作
    val stepStream = onStep(step,input)

    // 后续处理操作
    afterStep()

    stepStream
  }

  def check(step:Node){
    if(step==null)
      throw new Exception(this.getClass.getSimpleName + " 未配置此Step标签信息！")
  }

  def onStep(step:Node,input:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]

  def afterStep(){}
}

abstract class StreamingSource(sc:StreamingContext){

  def createStream(source:Node):DStream[Array[(String,String)]]

}