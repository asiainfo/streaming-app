package com.asiainfo.ocdc.streaming.impl

import org.apache.spark.Logging
import scala.xml.XML
import org.apache.spark.streaming.dstream.DStream
import com.asiainfo.ocdc.streaming.TestSuitBase

/*
class KafkaOutSuite extends TestSuitBase with Logging {

  test("KakfaOut test example") {
    logInfo("KakfaOut test example started ")

    val xmlFile = XML.load("src/test/resources/KafkaOut-test.xml")
    val step = xmlFile \ "step"

    val input = Seq(
      Seq(Array(("a", "1"),("b", "2"),("c", "3"))),
      Seq(Array(("a", "1"),("b", "2"),("c", "3")))
    )

    val expectedOutput = Seq(
      Seq(Array(("outstream", "1,2,"))),
      Seq(Array(("outstream", "1,2,")))
    )

    val tester = new KafkaOut()
    val operation = (s:DStream[Array[ (String, String) ] ]) => tester.onStep(step(0), s)

    testOperation(input, operation, expectedOutput, true)
    logInfo("KakfaOut test example finished ")
  }

}*/
