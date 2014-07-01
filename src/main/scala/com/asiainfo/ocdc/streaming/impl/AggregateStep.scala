package com.asiainfo.ocdc.streaming.impl

import com.asiainfo.ocdc.streaming.StreamingStep
import org.apache.spark.streaming.dstream.DStream
import scala.xml.Node
import org.apache.spark.streaming.StreamingContext._
import com.asiainfo.ocdc.streaming.tools.HbaseTool
import com.asiainfo.ocdc.streaming.tools.JexlTool

/**
 * @author surq
 * 流数据的聚合计算<br>
 * 1、此操作不与HBASＥ交互，功能等效sql的distinct,sum,count<br>
 * 2、 operationType＝[distinct，sum，count]<br>
 * 3、无需指定输出字段，默认输出为groupByKeys，operationKeys<br>
 * 4、只有当operationType＝sum时，operationKeys才会生效<br>
 * 5、只有当operationType＝count时，输出字段为groupByKeys和自动生成的字段"［count］"<br>
 */
class AggregateStep extends StreamingStep with Serializable {
  override def onStep(step: Node, dstream: DStream[Array[(String, String)]]): DStream[Array[(String, String)]] = {
    val debug_flg = true

    // 解析xml及check后结果 
    val (numTasks, operationType, groupByKeys, operationKeys) = xmlanalysis(step)
    //未通过check
    if (numTasks == null && operationType == null && groupByKeys == null && operationKeys == null) return null

    val keyUniteSeparator = "Asiaseparator"
    dstream.map(recode => {
      printDebugLog(debug_flg, "[polymerization.scala] Dstream is startting...")
      val imap = recode.toMap
      ((for { key <- 0 until groupByKeys.size } yield (imap(groupByKeys(key)))).mkString(keyUniteSeparator), recode)
    }).groupByKey(numTasks.toInt).map(keyrcode => {

      printDebugLog(debug_flg, "--------=keyrcode===key:----------" + keyrcode._1)
      val outGroupByKeys = (keyrcode._1).split(keyUniteSeparator)
      // groupbykey　key-value 拼接
      val outGroupByKeyArray = groupByKeys.zip(outGroupByKeys)

      operationType match {
        case "distinct" => outGroupByKeyArray
        case "sum" => {
          // 装载积集结果的map
          var sumMap = Map[String, String]()
          // 初始化operationKeys中的各值
          operationKeys.foreach(f => (sumMap += (f -> "0")))
          (keyrcode._2).foreach(f => {
            var keyValueMap = f.toMap
            operationKeys.foreach(f => {
              sumMap += (f -> (JexlTool.getExpValue("last+next", Array.apply(("last", sumMap(f)), ("next", keyValueMap(f))))))
            })
          })
          val sumList = sumMap.toList
          // 输出拼接
          val outputArray = (outGroupByKeyArray.toList) ::: sumList
          outputArray.toArray
        }
        case "count" =>(("[count]", (keyrcode._2.size).toString) :: outGroupByKeyArray.toList).toArray
      }
    })
  }

  /**
   * xml analysis<br>
   * get sub-node from configuration file.
   */
  def xmlanalysis(step: Node) = {
    var numTasks = (step \ "numTasks").text.toString.trim
    val operationType = (step \ "operationType").text.toString.trim.toLowerCase()
    val groupByKeysText = (step \ "groupByKeys").text.toString.trim
    val operationKeysText = (step \ "operationKeys").text.toString.trim

    val groupByKeys = groupByKeysText.split(",")
    val operationKeys = operationKeysText.split(",")

    var error_index = 0
    var ermsgMap = Map[Int, String]()

    // taskNum's check
    if (numTasks.isEmpty) numTasks = "8"
    if (!numTasks.matches("[0-9]+")) {
      ermsgMap += (error_index -> "<numTasks>'s type is number. Please keep the value is 1~n!")
      error_index += 1
    }

    // 必需输入项check 
    if (operationType.isEmpty) {
      ermsgMap += (error_index -> "must be inputed nodes <operationType>'s value. and make soure the node's name is <operationType>.")
      error_index += 1
    }
    if (groupByKeysText.isEmpty) {
      ermsgMap += (error_index -> "must be inputed nodes <groupByKeys>'s value. and make soure the node's name is <groupByKeys>.")
      error_index += 1
    }
    
    //check operationType输入的正确性
    if (!(operationType =="distinct" || operationType =="sum"|| operationType =="count")) {
      ermsgMap += (error_index -> "please make sure the value of <operationType> is any one of the list [distinct，sum，count].")
      error_index += 1
    }
    
    if (operationType == "sum") {
      if (operationKeysText.isEmpty) {
        ermsgMap += (error_index -> "must be inputed nodes <operationKeys>'s value. and make soure the node's name is <operationKeys>.")
        error_index += 1
      }
    }
    // 把印xml check结果，并返回结果
    if (error_index != 0) {
      ermsgMap.foreach(f => Console.err.println("[error-AggregateStep]:" + f._2))
      (null, null, null, null)
    } else (numTasks, operationType, timListItem(groupByKeys), timListItem(operationKeys))
  }

  /**
   * 去除List各项目的前后空格
   */
  def timListItem(array: Array[String]): Array[String] = {
    (for { index <- 0 until array.size } yield (array(index).trim)).toArray
  }

  /**
   * print debug logs
   */
  def printDebugLog(flg: Boolean, log: String) = {
    if (flg) println("[DEBUGLOG] DynamicOperate-----:" + log)
  }
}