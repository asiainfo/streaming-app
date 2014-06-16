package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.impl.StreamFilter
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import org.apache.hadoop.hbase._
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag
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

  override def verifyOutput[V: ClassTag](
                                 output: Seq[Seq[V]],
                                 expectedOutput: Seq[Seq[V]],
                                 useSet: Boolean
                                 ) {
    logInfo("--------------------------------")
    logInfo("output.size = " + output.size)
    logInfo("output")
    output.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.foreach(x => logInfo("[" + x.mkString(",") + "]"))
    logInfo("--------------------------------")

    // Match the output with the expected output
    assert(output.size === expectedOutput.size, "Number of outputs do not match")

    output.asInstanceOf[Seq[Seq[Array[Tuple2[String,String]]]]](0)(0)(0) match {
      case (_, _) =>
        output.asInstanceOf[Seq[Seq[Array[Tuple2[String,String]]]]].foreach {
          x => x.foreach {
            y =>
              for (t <- y) {
                logInfo("1: "+t._1+" 2: "+t._2)
              }
          }
        }
    }

    for (i <- 0 until output.size) {
      if (useSet) {
        assert(output(i).toSet === expectedOutput(i).toSet)
      } else {
        assert(output(i).toList === expectedOutput(i).toList)
      }
    }
    logInfo("Output verified successfully")
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