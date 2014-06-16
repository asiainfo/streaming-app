package com.asiainfo.ocdc.streaming.impl

import org.apache.spark.Logging
import org.apache.hadoop.hbase.HBaseTestingUtility
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import scala.Tuple2
import scala.reflect.ClassTag
import scala.xml.XML
import org.apache.spark.streaming.dstream.DStream

/**
 * Created by liuhao8 on 14-6-16.
 */
class DynamicOperateStepSuite extends org.apache.spark.streaming.TestSuiteBase with Logging {

  test("DynamicOperateStep test example") {
      logInfo("DynamicOperateStep test example started ")

      val xmlFile = XML.load("src/test/resources/tianyi-test.xml")
      val step = xmlFile \ "step"

      val input = Seq(
        Seq(Array(("a", "a"),("b", "b"))),
        Seq(Array(("a", "a"),("b", "b")))
      )

      val expectedOutput = Seq(
        Seq(Array(("a", "a"),("b", "b"),("DTable.c1", "c1"))),
        Seq(Array(("a", "a"),("b", "b"),("DTable.c1", "c1")))
      )

      testUtil.createTable("DTable","f1")
      val rowValue = (1 to 3).map(i=>("c"+i,"c"+i)).toArray
      putValue("DTable","a","f1",rowValue)

      val filter = new StreamFilter();
      val operation = (s:DStream[Array[ (String, String) ] ]) => filter.onStep(step(0), s)

      testOperation(input, operation, expectedOutput, true)
      logInfo("DynamicOperateStep test example finished ")
  }

}
