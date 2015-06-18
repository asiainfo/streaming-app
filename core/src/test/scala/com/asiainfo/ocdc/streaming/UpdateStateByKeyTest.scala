package com.asiainfo.ocdc.streaming

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by leo on 6/1/15.
 */
object UpdateStateByKeyTest {

  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("SparkSQLJoinCountTest")
    conf.setSparkHome("/home/leo/app/spark-1.2.0")
    //    conf.setMaster("spark://localhost:4040")
    conf.setMaster("local[4]")
    val sc = new SparkContext(conf)

    // stream data source
    val streamDataDir = "/home/leo/testdata/streaming/main/data"
    val ssc = new StreamingContext(sc, Seconds(20))
    ssc.checkpoint("/home/leo/testdata/streaming/checkpoint")

    // step 4 : stream updataStateByKey
    val updateFunc = (values: Seq[String], state: Option[String]) => {
      if(!values.isEmpty) {
        if(state != None){
          val a = state.get.split(":")
          Some(a.head+":"+(a.last.toInt + 1 ))
        }else{
          Some(values.head+":"+1)
        }
      }else None


    }

//    ssc.textFileStream(streamDataDir).map(_.split(" ")).map(x => {(x(0),x(1))}).join()

    ssc.textFileStream(streamDataDir).map(_.split(" ")).map(x => {(x(0),x(1))}).updateStateByKey(updateFunc).print()
    ssc.start()
    ssc.awaitTermination()
  }


}
