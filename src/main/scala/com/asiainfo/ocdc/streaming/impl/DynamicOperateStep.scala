package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming.StreamingStep
import org.apache.spark.streaming.dstream.DStream
import scala.xml.Node
import org.apache.spark.streaming.StreamingContext._
import com.asiainfo.ocdc.streaming.tools.HbaseTable

/**
 * Created by liuhao8 on 14-6-11.
 */
class DynamicOperateStep extends StreamingStep{

  def onStep(step:Node,inStream:DStream[Array[String]]):DStream[Array[String]]={
    val input = (step \ "input").text.toString.split(",")
    val table = (step \ "HBaseTable").text.toString
    val family = (step \ "family").text.toString
    val key = (step \ "HBaseKey").text.toString
    val Htable1 = HbaseTable.getTable(table)

    val operaters = (step \ "operater")
    val output = (step \ "output").text.toString.split(",")

    for(operater <- operaters){
      val cells = (operater \ "cells").text.toString.split(",")
      var reduce = (operater \ "reduce")
      val value = (operater \ "value").text.toString

      if(reduce.isEmpty){
        val values = value.split(",")
        if(cells.length==values.length){
          inStream.map(x=>{
            val imap = (0 to x.length-1).map(i=>(input(i),x(i))).toMap
            val saveParams = (0 to cells.length-1).map(i=>{
              (cells(i),imap.get(values(i)))
            }).toMap


          })
        }else throw new Exception("保存数据格式配置有误！")
      }else{
        val col = (reduce \ "col").text.toString
        val op = (reduce \ "op").text.toString
        inStream.map(x=>{
          // 添加计数列
          val y = x :+ 1
          val imap = (0 to y.length-1).map(i=>(input(i),y(i))).toMap
          (imap(key),y)
        }).reduceByKey((a,b) => {
          val imap1 = (0 to a.length-1).map(i=>(input(i)+1,a(i))).toMap
          val imap2 = (0 to b.length-1).map(i=>(input(i)+2,b(i))).toMap
          val newValue = getResult(op, imap++imap2.toArray)
          imap1.updated(col,newValue)
          imap1.toArray.map(_._2)
        }).map(x=>{


        })
      }

    }


  }




}
