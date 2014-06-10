package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming._
import org.apache.spark.streaming.dstream.DStream

class ExampleStep extends StreamingStep{

  def onStep(input:DStream[Array[String]]):DStream[Array[String]]={
      input
  }
}
