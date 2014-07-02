package com.asiainfo.ocdc.streaming.impl

import org.apache.spark.Logging
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import com.asiainfo.ocdc.streaming.TestSuitBase
import scala.xml.XML
import org.apache.spark.streaming.dstream.DStream
import scala._

/**
 * Created by wangxj8 on 14-6-16.
 */
class StreamSplitStepSuite extends TestSuitBase with Logging {
  test("StreamSplit test example1") {
    logInfo("StreamFilter test example1 started ")

    val xmlFile = XML.load("src/test/resources/streamsplit-test.xml")
    val step = xmlFile \ "step"

    val input = Seq(
      Seq(Array(("imsi", "460020060188214"),("product_id", "p1|p2|p3"),("cell","000B"))),
      Seq(Array(("imsi", "460020060188222"),("producr_id", "p2|p3|p4"),("cell","0000")))
    )
    val expectedOutput = Seq(
      Seq(Array(("imsi", "460020060188214"),("product_id", "p1"),("cell","000B"))),
      Seq(Array(("imsi", "460020060188214"),("product_id", "p2"),("cell","000B"))),
      Seq(Array(("imsi", "460020060188214"),("product_id", "p3"),("cell","000B"))),
      Seq(Array(("imsi", "460020060188222"),("producr_id", "p2"),("cell","0000"))),
      Seq(Array(("imsi", "460020060188222"),("producr_id", "p3"),("cell","0000"))),
      Seq(Array(("imsi", "460020060188222"),("producr_id", "p4"),("cell","0000")))
    )


    val filter = new StreamSpilt();
    val operation = (s:DStream[Array[ (String, String) ] ]) =>filter.onStep(step(0), s)


    testOperation(input, operation, expectedOutput, true)
    logInfo("StreamFilter test example1 finished ")
  }
}
