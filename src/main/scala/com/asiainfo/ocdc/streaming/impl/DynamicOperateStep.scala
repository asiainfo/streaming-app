package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming.StreamingStep
import org.apache.spark.streaming.dstream.DStream
import scala.xml.Node
import org.apache.spark.streaming.StreamingContext._
import com.asiainfo.ocdc.streaming.tools.HbaseTool
import com.asiainfo.ocdc.streaming.tools.JexlTool

/**
 * Created by liuhao8 on 14-6-11.
 */
class DynamicOperateStep extends StreamingStep{

  override def onStep(step:Node,DSinput:DStream[Array[(String,String)]]):DStream[Array[(String,String)]]={
    val input = (step \ "input").text.toString.split(",")
    val table = (step \ "HBaseTable").text.toString
    val family = (step \ "family").text.toString
    val key = (step \ "HBaseKey").text.toString
    val operaters = (step \ "operater")
    val output = (step \ "output").text.toString.split(",")

    if (input.size == 0) Console.err.println("请定义配置文件的<input>结点！"); return null
    
    for(operater <- operaters){
      val cells = (operater \ "cells").text.toString.split(",")
      var reduce = (operater \ "reduce")
      val value = (operater \ "value").text.toString

      if(reduce == null || reduce.isEmpty){
        val values = value.split(",")
        if(cells.length==values.length){
          DSinput.map(x=>{
         var imap= x.toMap
          val saveParams = (0 until cells.length).map(i=>(cells(i).trim,imap.getOrElse(values(i).trim, ""))).toArray
          HbaseTool.putValue(table, key, family, saveParams)
          })
        }else throw new Exception("保存数据格式配置有误！")
      }else{
        val col = (reduce \ "col").text.toString
        val op = (reduce \ "op").text.toString
        DSinput.map(x=>{
         var imap= x.toMap
          // 添加计数列
          imap += ("index"->"1")
         val itemList= for {index <- 0 until x.length} yield x(index)._2
          (imap(key),itemList)
        }).reduceByKey((a,b) => {
          val imap1 = (0 until a.length).map(i=>(input(i) + "1",a(i))).toMap
          val imap2 = (0 to b.length-1).map(i=>(input(i) + "2",b(i))).toMap
          val newValue = JexlTool.getExpValue(op, imap1++imap2.toArray)
          imap1.updated(col,newValue)
          imap1.toArray.map(_._2)
        }).map(x=>{
          
          val rowData = HbaseTool.getRow(table, key)
          var imap = (0 to (x._2).length - 1).map(i => (input(i), x._2(i).toString)).toMap
          imap += ("htable." + cells(0) -> Bytes.toString(rowData.getValue(Bytes.toBytes(family), Bytes.toBytes(cells(0)))))
          val newValue = JexlTool.getExpValue(op, imap.toArray)
          HbaseTool.PutValue(table, (x._1).toString, family, cells(0), newValue)

        })
      }
    }
     
    val result  = DSinput.map(x=>{
      //如果input output相同的字段完全相同，说明不需要规整数据，不做map
      val item = x.toMap
      (0 to output.length-1).map(i=>(output(i),item.getOrElse(output(i),output(i)))).toArray
    })
    result
  }
}