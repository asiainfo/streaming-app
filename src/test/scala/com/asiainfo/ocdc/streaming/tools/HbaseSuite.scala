package com.asiainfo.ocdc.streaming.tools

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.apache.hadoop.hbase._
import com.asiainfo.ocdc.streaming.tools.HbaseTool._


trait HbaseSuite extends FunSuite{

   val testUtil = HBaseTestingUtility.createLocalHTU()
   setConf(testUtil.getConfiguration)
   testUtil.startMiniCluster(1)
 }
