package com.asiainfo.ocdc.streaming

import scala.Array
import scala.xml.{Node, XML}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.SparkConf


object StreamingApp extends LogHelper{
  def main(args: Array[String]) {

    if (args.length < 3 ) {
      System.err.println("Usage: <AppName> <Stream_Space> <jobXMLPath>")
      System.exit(1)
    }
    val Array(appName,stream_space,jobConfFile) = args

    val xmlFile = XML.load(jobConfFile)

    val sparkConf = new SparkConf().setAppName(appName)
    val ssc =  new StreamingContext(sparkConf, Seconds(stream_space.toInt))

    // 数据流引入
    val dataSource = xmlFile \ "dataSource"
    val clz = Class.forName((dataSource \ "class").text.toString)
    val method = clz.getDeclaredMethod("createStream",classOf[Node])
    var streamingData = method.invoke(clz.getConstructor(classOf[StreamingContext]).newInstance(ssc),dataSource(0))

    // 流数据Step运行
    for(step <- steps){
       val clz = Class.forName((step \ "class").text.toString)
       val method = clz.getMethod("run",classOf[Node], classOf[DStream[Array[(String,String)]]])
       streamingData = method.invoke(clz.newInstance(), step,streamingData)
    }

    streamingData.asInstanceOf[DStream[Array[(String,String)]]].print()
    ssc.start()
    ssc.awaitTermination()
   }
 }

abstract class StreamingStep() extends LogHelper{

  /**
   * Step 运行主方法
   * @param step
   * @param input
   * @return
   */
  def run(step:Node,input:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]={
    // Step xml 检查
    check(step)

    // 流处理主操作
    onStep(step,input)

  }

  def check(step:Node){
    if(step==null)
      throw new Exception(this.getClass.getSimpleName + " 未配置此Step标签信息！")
  }

  def onStep(step:Node,input:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]

}

abstract class StreamingSource(sc:StreamingContext) extends LogHelper{

  def createStream(source:Node):DStream[Array[(String,String)]]

}