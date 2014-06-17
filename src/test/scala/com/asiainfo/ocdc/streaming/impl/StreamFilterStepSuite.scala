package com.asiainfo.ocdc.streaming.impl

import org.apache.spark.Logging
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import com.asiainfo.ocdc.streaming.TestSuitBase
import scala.xml.XML
import org.apache.spark.streaming.dstream.DStream
import scala._

/**
 * Created by liuhao8 on 14-6-16.
 */
class StreamFilterStepSuite extends TestSuitBase with Logging {
  test("Streamfilter test example1") {
    logInfo("StreamFilter test example1 started ")

    val xmlFile = XML.load("src/test/resources/streamfilter.xml")
    val step = xmlFile \ "step"

    val input = Seq(
      Seq(Array(("imsi", "460020060188214"),("lac", "0867"),("cell","000B"))),
      Seq(Array(("imsi", "460020060188222"),("lac", "0867"),("cell","0000")))
    )
    val expectedOutput = Seq(
      Seq(Array(("imsi", "460020060188214"),("t1.city_id", "2")))
    )

    testUtil.createTable("t1","F")
    val rowValue = Array(("city_id","2"))
    putValue("t1","0867000B","F",rowValue)

    val filter = new StreamFilter();
    val operation = (s:DStream[Array[ (String, String) ] ]) => filter.onStep(step(0), s)

    testOperation(input, operation, expectedOutput, true)
    logInfo("StreamFilter test example1 finished ")
  }



  test(" streamFilter test example2") {
    logInfo("StreamFilter test example2 started ")

    val xmlFile = XML.load("src/test/resources/streamfilter.xml")
    val step = xmlFile \ "step"

    val input = Seq(
      Seq(Array(("imsi", "460020060188214"),("t1.city_id", "2")))
      ,Seq(Array(("imsi", "46002006018822"),("t1.city_id", "3")))
    )

    val expectedOutput = Seq(
      Seq(Array(("imsi", "460020060188214"),("t1.city_id", "2")))
    )

    testUtil.createTable("t2","F")
    val rowValue=Array(("city_id","3"),("city_id1","3"))
    putValue("t2","460020060188214","F",rowValue)
    putValue("t2","460020060188222","F",rowValue)

    val filter = new StreamFilter();
    val operation = (s:DStream[Array[ (String, String) ] ]) => filter.onStep(step(1), s)

    testOperation(input, operation, expectedOutput, true)
    logInfo("StreamFilter test example2 finished ")
  }


  test(" streamFilter test example3") {
    logInfo("StreamFilter test example3 started ")

    val xmlFile = XML.load("src/test/resources/streamfilter.xml")
    val step = xmlFile \ "step"

    val input = Seq(
      Seq(Array(("imsi", "460020060188214"),("t1.city_id", "2")))
      ,Seq(Array(("imsi", "46002006018822"),("t1.city_id", "3")))
    )

    val expectedOutput = Seq(
      Seq(Array(("imsi", "460020060188214"),("t3.product_no","215801535555"),("t3.product_id","p1|p2")))
    )

    testUtil.createTable("t3","F")
    val rowValue=Array(("product_no","215801535555"),("product_id","p1|p2"))
    putValue("t3","460020060188214","F",rowValue)

    val filter = new StreamFilter();
    val operation = (s:DStream[Array[ (String, String) ] ]) => filter.onStep(step(2), s)

    testOperation(input, operation, expectedOutput, true)
    logInfo("StreamFilter test example3 finished ")
  }

  test(" streamFilter test example4") {
    logInfo("StreamFilter test example4 started ")

    val xmlFile = XML.load("src/test/resources/streamfilter.xml")
    val step = xmlFile \ "step"

    val input = Seq(
      Seq(Array(("imsi", "460020060188214"),("t3.product_no","215801535555"),("t3.product_id","p1|p2"))),
      Seq(Array(("imsi", "460020060188222"),("t3.product_no","215801535555"),("t3.product_id","p1|p2")))
    )


    val expectedOutput = Seq(
      Seq(Array(("imsi", "460020060188214"),("t3.product_no","215801535555"),("t3.product_id","p1|p2")))
    )

    testUtil.createTable("t4","F")
    val rowValue=Array(("c1","215801535555"),("c2","p1|p2"))
    putValue("t4","460020060188214","F",rowValue)

    val rowValue1=Array(("c1","null"),("c2","21"))
    putValue("t4","460020060188222","F",rowValue)
    val filter = new StreamFilter();
    val operation = (s:DStream[Array[ (String, String) ] ]) => filter.onStep(step(3), s)

    testOperation(input, operation, expectedOutput, true)
    logInfo("StreamFilter test example3 finished ")
  }
}
