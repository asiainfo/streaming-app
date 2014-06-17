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

  override def onStep(step: Node, dstream: DStream[Array[(String, String)]]): DStream[Array[(String, String)]] = {

    val table = (step \ "HBaseTable").text.toString.trim
    val key = (step \ "HBaseKey").text.toString.trim
    val family = "F"
    val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",")
    val operaters = (step \ "expressions").text.toString.trim.split(",")
    val output = (step \ "output").text.toString.trim.split(",")

    //xml check
    if (!validityCheck(step: Node)) return dstream 
    
    dstream.map(recode => {
      var imap = recode.toMap
      (imap(key), recode)
    }).groupByKey.map(keyrcode => {
      //　从hbase中取出要累加的初始数据
      val cellvalue = HbaseTool.getValue(table, keyrcode._1, family, hBaseCells)
      // 要更新的operaters与hbase.cell一一对应
      val cellexp = hBaseCells.zip(operaters)

      // 累计结果用[key:cell,value:对应表达式的累计值]
      var mapSet = Map[String, String]()
      // 初始化cell中的各值
      cellexp.foreach(f=>(mapSet += (f._1 -> "0")))
      
      (keyrcode._2).foreach(f => {
        var experValue = f.toMap ++ cellvalue
        cellexp.foreach(f => {
          val tmpdata = JexlTool.getExpValue(f._2, experValue.toArray)
           mapSet += (f._1 -> (JexlTool.getExpValue("last+next", Array.apply(("last",mapSet(f._1)), ("next",tmpdata)))))
        })
      })
      // 表达式对应的结果值更新到hbase
      HbaseTool.putValue(table, keyrcode._1, family, mapSet.toArray)
    })

    // 结果输出
    val result = dstream.map(x => {
      //如果input output相同的字段完全相同，说明不需要规整数据，不做map
      val item = x.toMap
      (0 to output.length - 1).map(i => (output(i), item.getOrElse(output(i), output(i)))).toArray
    })
    return result
  }
  
  /**
   * 数据有效性检查
   */
  def validityCheck(step: Node):Boolean={
    var checkresult =true
    val table = (step \ "HBaseTable").text.toString.trim
    val key = (step \ "HBaseKey").text.toString.trim
    val family = "F"
    val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",")
    val operaters = (step \ "expressions").text.toString.trim.split(",")
    val output = (step \ "output").text.toString.trim.split(",")
    
    //check expressions和HBaseCells的个数
    if (hBaseCells.size != operaters.size){checkresult = false; Console.err.println("<expressions>中的个数和<HBaseCells>中的个数不一致！请确认")}
    
    //check expressions中所写的hbase表名是否正确 (大小写)
    var opindex =0 
    while (checkresult && opindex<operaters.size) {
      
       if(operaters(opindex).toUpperCase.trim.matches(table.toUpperCase+".")){
          if (!operaters(opindex).matches(table+".")){
            checkresult = false
            Console.err.println("<expressions>中所记述的表名大小写不正确，请改为［正解的表名.字段名］")
          }}
      opindex +=1
    }
      checkresult
  }
}