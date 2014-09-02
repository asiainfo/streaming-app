package com.asiainfo.ocdc.streaming

import scala.Array
import scala.xml.{Node, XML}
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.{Logging, SparkConf}

import scala.collection.mutable.HashMap
import scala.collection.mutable.Map
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.hadoop.conf.Configuration
import java.io.{InputStreamReader, BufferedReader}


object StreamingApp extends Logging{
  def main(args: Array[String]) {

    if (args.length < 3 ) {
      System.err.println("Usage: <AppName> <Stream_Space> <jobXMLPath>")
      System.exit(1)
    }
    val Array(appName,stream_space,jobConfFile) = args

    val xmlFile = XML.load(jobConfFile)

    val sparkConf = new SparkConf().setAppName(appName)
    val ssc =  new StreamingContext(sparkConf, Seconds(stream_space.toInt))

    val broadcastValue= ssc.sparkContext.broadcast(createbroadcast(xmlFile))

    // 数据流引入
    val dataSource = xmlFile \ "dataSource"
    val clz = Class.forName((dataSource \ "class").text.toString)
    val method = clz.getDeclaredMethod("createStream",classOf[Node])
    var streamingData = method.invoke(clz.getConstructor(classOf[StreamingContext]).newInstance(ssc),dataSource(0))

    // 流数据Step运行
    val steps = xmlFile \ "step"
    for(step <- steps){
      val clz = Class.forName((step \ "class").text.toString)
      val method = clz.getMethod("run",classOf[Node], classOf[DStream[Array[(String,String)]]])
      streamingData = method.invoke(clz.getConstructor(classOf[HashMap[String, HashMap[String, Array[(String,String)]]]])newInstance(broadcastValue), step,streamingData)
    }

    streamingData.asInstanceOf[DStream[Array[(String,String)]]].print()
    ssc.start()
    ssc.awaitTermination()
  }

  def createbroadcast(xmlFile:Node):HashMap[String, HashMap[String, Array[(String,String)]]]={
    val result =  HashMap[String, HashMap[String, Array[(String,String)]]]()
    val delim = ","

    val steps = xmlFile \ "broadcasts"
    for(step <- steps){
      val lineSplit = (step \ "lineSplit").text.toString.trim
      val hdfsDir = (step \ "hdfsdir").text.toString.trim
      val output = (step \ "output").text.toString.trim.split(delim)
      val Key = (step \ "Key").text.toString.trim.split(delim)
      val tableName = (step \ "tableName").text.toString.trim

      val hdfs = FileSystem.get( new Configuration())
      val in = hdfs.open(new Path(hdfsDir))
      val bis = new BufferedReader(new InputStreamReader(in,"GBK"))
      var temp = ""

      var out = HashMap[String, Array[(String,String)]]()
      while ((temp=bis.readLine() )!= null){
        //按行分割，行内分列 暂时有问题
        temp.map(line=>{
          val column= line.toString.split(lineSplit)
          val comlumarray= (0 to output.length-1).map(i=>{
            (output(i),column(i))
          }).toArray
          var boradkey = ""
          for (arg <-Key) {
            val item = comlumarray.toMap
            boradkey +=  item.getOrElse(arg,"")
          }
          out += (boradkey ->comlumarray)
        })
      }
      result +=(tableName->out)
    }
    result
  }
}

abstract class StreamingStep(broadcast:HashMap[String, HashMap[String, Array[(String,String)]]]) extends Logging{

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

abstract class StreamingSource(sc:StreamingContext) extends Logging{

  def createStream(source:Node):DStream[Array[(String,String)]]

}


