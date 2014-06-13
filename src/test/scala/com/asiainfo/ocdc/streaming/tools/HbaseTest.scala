package com.asiainfo.ocdc.streaming.tools

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.hadoop.hbase._
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes


class HbaseTest  extends FunSuite with BeforeAndAfter{

  val testUtil = HBaseTestingUtility.createLocalHTU()
  setConf(testUtil.getConfiguration)
  testUtil.startMiniCluster(1)

  before{

    testUtil.createTable("testTable","f1")
    val rowValue = (1 to 3).map(i=>("col"+i,"v"+i)).toArray
    putValue("testTable","row1","f1",rowValue)

  }
  after{

  }
  test("get") {
    println("getResult: ")
    getValue("testTable","row1","f1",(1 to 3).map("col"+_).toArray).foreach(i=>println(i))
  }
}
