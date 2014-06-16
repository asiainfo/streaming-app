package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming.StreamingStep
import org.apache.spark.streaming.dstream.DStream
import scala.xml.Node
import org.apache.spark.streaming.StreamingContext._
import com.asiainfo.ocdc.streaming.tools.HbaseTool
import com.asiainfo.ocdc.streaming.tools.JexlTool

/**
 * @author surq
 * 
 */
class DynamicOperate  extends StreamingStep {

    override def onStep(step: Node, DSinput: DStream[Array[(String, String)]]): DStream[Array[(String, String)]] = {
      
      
          val table = (step \ "HBaseTable").text.toString
    val family = (step \ "family").text.toString
    val key = (step \ "HBaseKey").text.toString
    val operaters = (step \ "operater")
    val output = (step \ "output").text.toString.split(",")
      return DSinput
    }
}