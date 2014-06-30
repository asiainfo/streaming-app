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
class DynamicOperate extends StreamingStep with Serializable {

  override def onStep(step: Node, dstream: DStream[Array[(String, String)]]): DStream[Array[(String, String)]] = {

    val debug_flg = true

    val (numTasks, table, key, hBaseCells, operaters, output) = validityCheck(step)
    
    printDebugLog(debug_flg, "numTasks=" + numTasks)
    printDebugLog(debug_flg, "HBaseKey=" + key)
    printDebugLog(debug_flg, "HBaseTable=" + table)
    printDebugLog(debug_flg, "hBaseCells=" + hBaseCells.mkString(" "))
    printDebugLog(debug_flg, "operaters=" + operaters.mkString(" "))
    printDebugLog(debug_flg, "output=" + output.mkString(" "))

    //未通过check
    if (numTasks == null && key == null && table == null && hBaseCells == null && operaters == null && output == null) return null

    val tempSream = dstream.map(recode => {
      printDebugLog(debug_flg, "DynamicOperate Dstream is startting...")
      var imap = recode.toMap
      (imap(key), recode)
    }).groupByKey(numTasks.toInt).map(keyrcode => {
      printDebugLog(debug_flg, "[rowkey=" + keyrcode._1 + "] is executing...")
      //　从hbase中取出要累加的初始数据
      val getHbaseValue = HbaseTool.getValue(table, keyrcode._1, HbaseTool.family, hBaseCells)
      printDebugLog(debug_flg, "getHbaseValue's size:=" + getHbaseValue.size)

      // 如果hbase中无基础数据时，把null转换为“0”
      var cellvalue = Map[String, String]()
      getHbaseValue.foreach(f => { if (f._2 == null || (f._2).trim.isEmpty || (f._2).toLowerCase() == "null") cellvalue += (f._1 -> "0") else cellvalue += (f._1 -> f._2) })
      cellvalue.foreach(f => printDebugLog(debug_flg, "getHbaseValue's:" + f._1 + ":" + f._2))
      // 要更新的operaters与hbase.cell一一对应
      val cellexp = hBaseCells.zip(operaters)
      cellexp.foreach(f => printDebugLog(debug_flg, "cellexp:" + f._1 + ":" + f._2))

      // 累计结果用[key:cell,value:对应表达式的累计值]
      var mapSet = Map[String, String]()
      // 初始化cell中的各值
      cellexp.foreach(f => (mapSet += (f._1 -> "0")))

      (keyrcode._2).foreach(f => {
        var experValue = f.toMap ++ cellvalue
        cellexp.foreach(f => {
          val tmpdata = JexlTool.getExpValue(f._2, experValue.toArray)
          mapSet += (f._1 -> (JexlTool.getExpValue("last+next", Array.apply(("last", mapSet(f._1)), ("next", tmpdata)))))
        })
      })
      printDebugLog(debug_flg, "更新hbase值:" + mapSet.mkString(" "))
      // 表达式对应的结果值更新到hbase
      HbaseTool.putValue(table, keyrcode._1, HbaseTool.family, mapSet.toArray)
      keyrcode
    }).flatMap(_._2)

    // 结果输出
    val result = tempSream.map(x => {
      //如果input output相同的字段完全相同，说明不需要规整数据，不做map
      val item = x.toMap
      printDebugLog(debug_flg, "Dstream data is outputting...")
      (0 to output.length - 1).foreach(i => { println(output(i) + "###" + item.getOrElse(output(i), output(i))) })

      (0 to output.length - 1).map(i => (output(i), item.getOrElse(output(i), output(i)))).toArray
    })
    result
  }

  /**
   * print debug logs
   */
  def printDebugLog(flg: Boolean, log: String) = {
    if (flg) println("[DEBUGLOG] DynamicOperate-----:" + log)
  }

  /**
   * 数据有效性检查
   */
  def validityCheck(step: Node) = {

    var numTasks = (step \ "numTasks").text.toString.trim
    val table = (step \ "HBaseTable").text.toString.trim
    val key = (step \ "HBaseKey").text.toString.trim
    val hBaseCellsText = (step \ "HBaseCells").text.toString.trim
    val operatersText = (step \ "expressions").text.toString.trim
    val outputText = (step \ "output").text.toString.trim

    val hBaseCells = hBaseCellsText.split(",")
    val operaters = operatersText.split(",")
    val output = outputText.split(",")
    if (numTasks == null || numTasks.isEmpty) numTasks = "8"

    var error_index = 0
    var ermsgMap = Map[Int, String]()

    // taskNum's check
    // numTasks 默认为8个并行任务进行分组
    if (numTasks.isEmpty) numTasks = "8"
    if (!numTasks.matches("[0-9]+")) {
      ermsgMap += (error_index -> "<numTasks>'s type is number. Please keep the value is 1~n!")
      error_index += 1
    }

    // 必需输入项check 
    if (table.isEmpty) {
      ermsgMap += (error_index -> "must be inputed nodes <HBaseTable>'s value. and make soure the node's name is <HBaseTable>.")
      error_index += 1
    }
    if (key.isEmpty) {
      ermsgMap += (error_index -> "must be inputed nodes <HBaseKey>'s value. and make soure the node's name is <HBaseKey>.")
      error_index += 1
    }
    if (hBaseCellsText.isEmpty) {
      ermsgMap += (error_index -> "must be inputed nodes <HBaseCells>'s value. and make soure the node's name is <HBaseCells>.")
      error_index += 1
    }
    if (operatersText.isEmpty) {
      ermsgMap += (error_index -> "must be inputed nodes <expressions>'s value. and make soure the node's name is <expressions>.")
      error_index += 1
    }
    if (outputText.isEmpty) {
      ermsgMap += (error_index -> "must be inputed nodes <output>'s value. and make soure the node's name is <output>.")
      error_index += 1
    }

    //check expressions中所写的hbase表名是否正确 (大小写)
    var opindex = 0
    while (!operatersText.isEmpty && opindex < operaters.size) {

      if (operaters(opindex).toUpperCase.trim.matches(table.toUpperCase + ".")) {
        if (!operaters(opindex).matches(table + ".")) {
          ermsgMap += (error_index -> ("in expression [" + operaters(opindex) + "] of <expressions>, hbase table's name case is not consistent."))
        }
      }
      opindex += 1
    }

    // 把印xml check结果，并返回结果
    if (error_index != 0) {
      ermsgMap.foreach(f => Console.err.println("[error-DynamicOperate]:" + f._2))
      (null, null, null, null, null, null)
    } else (numTasks, table, key, timListItem(hBaseCells), timListItem(operaters), timListItem(output))
  }

  /**
   * 去除List各项目的前后空格
   */
  def timListItem(array: Array[String]): Array[String] = {
    (for { index <- 0 until array.size } yield (array(index).trim)).toArray
  }
}