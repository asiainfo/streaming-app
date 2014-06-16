package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.impl.StreamFilter
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import org.apache.hadoop.hbase._
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream

import scala.reflect.ClassTag
import scala.xml.XML


class TianyiTestSuite extends TestSuitBase with Logging {

  test("test example") {
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

      testUtil.createTable("t1","F")
      val rowValue = (1 to 3).map(i=>("c"+i,"c"+i)).toArray
      putValue("t1","a","F",rowValue)

      val filter = new StreamFilter();
      val operation = (s:DStream[Array[ (String, String) ] ]) => filter.onStep(step(0), s)

      testOperation(input, operation, expectedOutput, true)
      logInfo("test example finished ")
  }

}