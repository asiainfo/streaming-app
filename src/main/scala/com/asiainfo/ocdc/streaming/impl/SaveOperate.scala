package com.asiainfo.ocdc.streaming.impl

import org.apache.spark.Logging
import com.asiainfo.ocdc.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.hbase.util.Bytes
import scala.xml.Node
import com.asiainfo.ocdc.streaming.tools._

class SaveOperate extends StreamingStep with Logging {
  
  /**
   * check and validate configuration in SaveOperate-test.xml with followings:
   * 	1) step is not null
   * 	2) value in HBaseTable,HBaseKey,HbaseCells,expressions,output is not null
   *  	3) column count in HbaseCells and expressions is equal
   * @param step
   * @return 
   */
  override def check(step:Node){
	  if(step==null)
		  throw new Exception(this.getClass.getSimpleName + " 未配置此Step标签信息！")
	  else {
	    val table = (step \ "HBaseTable").text.toString.trim
		val key = (step \ "HBaseKey").text.toString.trim
		val cells=(step \ "HBaseCells").text.toString.trim
		val exprs=(step \ "expressions").text.toString.trim
		val outs=(step \ "output").text.toString.trim
		
		if(table==null||table.isEmpty){
		  throw new Exception(this.getClass.getSimpleName + " 指定Step标签中 HBaseTable 取值为空，请检查!")
		}
	    
	    if(key==null||key.isEmpty){
	      throw new Exception(this.getClass.getSimpleName + " 指定Step标签中 HBaseKey 取值为空，请检查!")
	    }
	    
	    if(cells==null||cells.isEmpty){
	      throw new Exception(this.getClass.getSimpleName + " 指定Step标签中 HBaseCells 取值为空，请检查!")
	    } else {
	      val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",").map(c=>{
		  c.trim()
		})
	    }
	    
	    if(exprs==null||exprs.isEmpty){
	      throw new Exception(this.getClass.getSimpleName + " 指定Step标签中 expressions 取值为空，请检查!")
	    } else {
	      val expressions = (step \ "expressions").text.toString.trim.split(",").map(e=>{
		  e.trim()
		})
	    } 
	    
	    if(outs==null||outs.isEmpty){
	      throw new Exception(this.getClass.getSimpleName + " 指定Step标签中 output 取值为空，请检查!")
	    }
		
		val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",")
		val expressions = (step \ "expressions").text.toString.trim.split(",")
		
	    if(hBaseCells.length != expressions.length){
	      throw new Exception(this.getClass.getSimpleName + " 指定Step标签中 HBaseCells 和 expressions 配置个数不匹配，请检查!")
	    }
	    
	  }
  }
  
  def onStep(step: Node, inStream: DStream[Array[(String, String)]]): DStream[Array[(String, String)]] = {

		val table = (step \ "HBaseTable").text.toString.trim
		val key = (step \ "HBaseKey").text.toString.trim
		val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",").map(c=>{
		  c.trim()
		})
		val expressions = (step \ "expressions").text.toString.trim.split(",").map(e=>{
		  e.trim()
		})
		val output = (step \ "output").text.toString.trim.split(",").map(o=>{
		  o.trim()
		})
		
		val handle = inStream.map(record => {
			var recordMap = record.toMap
			
			//retrieve old data from hbase table
			val getHbaseValue = HbaseTool.getValue(table, recordMap(key), HbaseTool.family, hBaseCells)
			
			//replace null with 0
			var cellValue = Map[String, String]()
		    getHbaseValue.foreach(f => { 
		      if ( (f._2) == null || (f._2).toLowerCase().isEmpty || (f._2).toLowerCase() == "null") {
		        cellValue += (f._1 -> "0") 
		      } else {
		        cellValue += (f._1 -> f._2)
		      }
		    })
		    
		    //compute expressions
		    val expressionValue = for { index <- 0 until expressions.size } yield (JexlTool.getExpValue(expressions(index), (recordMap ++ cellValue).toArray))
	
		    val cellexp = hBaseCells.zip(expressionValue)
		    
		    //rewrite new data to hbase
			HbaseTool.putValue(table, recordMap(key), HbaseTool.family, cellexp)
			record
		})
    handle
  }
}