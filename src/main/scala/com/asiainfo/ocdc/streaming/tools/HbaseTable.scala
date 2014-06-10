package com.asiainfo.ocdc.streaming.tools

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Result, Get, HTable}
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.mutable

object HbaseTable {

  val table = new mutable.HashMap[String,HTable]()

  def getTable(tableName:String):HTable={

    table.getOrElse(tableName,{
      println("----new connection ----")
      var from = System.currentTimeMillis();
      val conf = HBaseConfiguration.create()
      var tbl = new HTable(conf, tableName)
      var end = System.currentTimeMillis();
      println("connection time----"+(end-from))
      table(tableName)= tbl
      tbl
    })
  }

  def GetValue(tableName:String,rowKey:String):Result={
    val table_t =getTable(tableName)
    val row1 =  new Get(Bytes.toBytes(rowKey))
    table_t.get(row1)
  }
}