package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.impl.StreamFilter
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import org.apache.hadoop.hbase._
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream

import scala.xml.XML


class TianyiTestSuite extends org.apache.spark.streaming.TestSuiteBase with Logging {

  val testUtil= HBaseTestingUtility.createLocalHTU()

  override def beforeFunction {
    if (useManualClock) {
      logInfo("Using manual clock")
      conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.ManualClock")
    } else {
      logInfo("Using real clock")
      conf.set("spark.streaming.clock", "org.apache.spark.streaming.util.SystemClock")
    }
    testUtil.startMiniCluster(1)
    setConf(testUtil.getConfiguration)
  }

  override def afterFunction {
    System.clearProperty("spark.driver.port")
    System.clearProperty("spark.streaming.clock")
    testUtil.shutdownMiniCluster()
  }

  testExample()

  def testExample() {
    test("tianyi test example") {
      logInfo("tianyi test example started ")

      val xmlFile = XML.load("src/test/resources/tianyi-test.xml")
      val step = xmlFile \ "step"

      val input = Seq(
        Seq(Array(("a", "a"),("b", "b"))),
        Seq(Array(("a", "a"),("b", "b")))
      )

      val expectedOutput = Seq(
        Seq(Array(("a", "a"),("b", "b"),("t1.c1", "c1"))),
        Seq(Array(("a", "a"),("b", "b"),("t1.c1", "c1")))
      )

      testUtil.createTable("t1","f1")
      val rowValue = (1 to 3).map(i=>("c"+i,"c"+i)).toArray
      putValue("t1","a","f1",rowValue)

      val filter = new StreamFilter();
      val operation = (s:DStream[Array[ (String, String) ] ]) => filter.onStep(step(0), s)

      testOperation(input, operation, expectedOutput, true)
      logInfo("tianyi test example finished ")
    }
  }
}