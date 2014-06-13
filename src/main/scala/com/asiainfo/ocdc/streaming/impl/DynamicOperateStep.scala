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
class DynamicOperateStep extends StreamingStep {

  override def onStep(step: Node, DSinput: DStream[Array[(String, String)]]): DStream[Array[(String, String)]] = {

    val table = (step \ "HBaseTable").text.toString
    val family = (step \ "family").text.toString
    val key = (step \ "HBaseKey").text.toString
    val operaters = (step \ "operater")
    val output = (step \ "output").text.toString.split(",")

    for (operater <- operaters) {
      val cells = (operater \ "cells").text.toString.split(",")
      var reduce = (operater \ "reduce")
      val value = (operater \ "value").text.toString

      if (reduce == null || reduce.isEmpty) {
        val values = value.split(",")
        if (cells.length == values.length) {
          DSinput.map(x => {
            var imap = x.toMap
            val saveParams = (0 until cells.length).map(i => (cells(i).trim, imap.getOrElse(values(i).trim, ""))).toArray
            HbaseTool.putValue(table, key, family, saveParams)
          })
        } else throw new Exception("保存数据格式配置有误！")
      } else {
        val col = (reduce \ "col").text.toString
        val op = (reduce \ "op").text.toString
        DSinput.map(x => {
          var imap = x.toMap
          // 添加计数列
          imap += ("index" -> "1")
          (imap(key), x)
        }).reduceByKey((a, b) => {
          val itemList = for { index <- 0 until a.length } yield a(index)._1

          val imap1 = (0 until a.length).map(i => (itemList(i) + "1", a(i)._2)).toMap
          val imap2 = (0 to b.length - 1).map(i => (itemList(i) + "2", b(i)._2)).toMap

          val newValue = JexlTool.getExpValue(op, (imap1 ++ imap2).toArray)
          imap1.updated(col,newValue)
          imap1.toArray
        }).map(x => {
          // 流数据的字段名称
          val itemList = for { index <- 0 until x._2.length } yield (x._2)(index)._1

          val itemarry = new Array[String](1)
          itemarry(0) = cells(0).trim()
          val cellvalue = HbaseTool.getValue(table, x._1, family, itemarry)
          var imap = (0 until (x._2).length).map(i => (itemList(i), (x._2)(i)._2)).toMap
          imap += ("htable." + cells(0) -> cellvalue(0)._2)
          val newValue = JexlTool.getExpValue(op, imap.toArray)

          val saveParams = new Array[(String, String)](1)
          saveParams(0) = (itemarry(0), newValue)
          HbaseTool.putValue(table, x._1, family, saveParams)
        })
      }
    }

    val result = DSinput.map(x => {
      //如果input output相同的字段完全相同，说明不需要规整数据，不做map
      val item = x.toMap
      (0 to output.length - 1).map(i => (output(i), item.getOrElse(output(i), output(i)))).toArray
    })
    result
  }
}