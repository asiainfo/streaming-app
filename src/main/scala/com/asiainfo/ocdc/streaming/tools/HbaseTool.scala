package com.asiainfo.ocdc.streaming.tools

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Put, Result, Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable

object HbaseTool {

  val table = new mutable.HashMap[String,HTable]()
  var conf = HBaseConfiguration.create()

  def setConf(c:Configuration)={
    conf = c
  }

  def getTable(tableName:String):HTable={

    table.getOrElse(tableName,{
      println("----new connection ----")
      val tbl = new HTable(conf, tableName)
      table(tableName)= tbl
      tbl
    })
  }

  def getValue(tableName:String,rowKey:String,family:String,qualifiers:Array[String]):Array[(String,String)]={
    var result:AnyRef = null
    val table_t =getTable(tableName)
    val row1 =  new Get(Bytes.toBytes(rowKey))
    val HBaseRow = table_t.get(row1)
    if(HBaseRow != null){
      result = qualifiers.map(c=>{
        (tableName+"."+c, Bytes.toString(HBaseRow.getValue(Bytes.toBytes(family), Bytes.toBytes(c))))
      })
    }
    else{
      result=qualifiers.map(c=>{
        (tableName+"."+c,"null")  })
    }
    result.asInstanceOf[Array[(String,String)]]
  }

  def putValue(tableName:String,rowKey:String, family:String,qualifierValue:Array[(String,String)]) {
    val table =getTable(tableName)
    val new_row  = new Put(Bytes.toBytes(rowKey))
    qualifierValue.map(x=>{
      var column = x._1
      val value = x._2
      val tt = column.split("\\.")
      if (tt.length == 2) column=tt(1)
      if(!(value.isEmpty))
        new_row.add(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value))
    })
    table.put(new_row)
  }


}