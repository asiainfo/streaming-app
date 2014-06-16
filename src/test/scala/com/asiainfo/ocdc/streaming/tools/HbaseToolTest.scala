package com.asiainfo.ocdc.streaming.tools

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.hadoop.hbase._
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import com.asiainfo.ocdc.streaming.TestSuitBase


class HbaseToolTest  extends TestSuitBase{

  test("hbase tool test") {
    testUtil.createTable("testTable","f1")
    val rowValue = (1 to 3).map(i=>("col"+i,"v"+i)).toArray
    putValue("testTable","row1","f1",rowValue)
    getValue("testTable","row1","f1",(1 to 3).map("col"+_).toArray).foreach(i=>{
      assert(i._2!="null","HbaseTool get row failed")
    })
  }
}
