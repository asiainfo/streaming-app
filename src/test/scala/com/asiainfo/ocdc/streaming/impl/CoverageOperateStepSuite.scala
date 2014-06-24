package com.asiainfo.ocdc.streaming.impl

import org.apache.spark.Logging
import org.apache.hadoop.hbase.HBaseTestingUtility
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import com.asiainfo.ocdc.streaming.TestSuitBase
import scala.Tuple2
import scala.reflect.ClassTag
import scala.xml.XML
import org.apache.spark.streaming.dstream.DStream

class CoverageOperateStepSuite extends TestSuitBase with Logging {

  logInfo("CoverageOperateStepSuite test  started ")
  test1(0, "no initial data")
  test2(0, "has init data)")

  logInfo("CoverageOperateStepSuite test finished ")
  
  def test1(stepNum: Int, caseName: String) {

    test(caseName) {
      logInfo("CoverageOperateStepSuite-" + caseName + " test example start... ")

      val xmlFile = XML.load("src/test/resources/CoverageOperate-test.xml")

      val steps = xmlFile \ "step"
      val step = steps(stepNum)

      val table = (step \ "HBaseTable").text.toString.trim

      testUtil.createTable(table, family)

      val input = Seq(
        Seq(Array(("imsi", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("imsi", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("imsi", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))))

      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "20"), (table + ".Fee", "10"))),
        Seq(Array.apply((table + ".NTcount", "33"), (table + ".Fee", "11"))),
        Seq(Array.apply((table + ".NTcount", "31"), (table + ".Fee", "1"))))
      val expectedOutput = Seq(Seq(Array.apply(("imsi", "18600640175"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array.apply(("imsi", "18600640176"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array.apply(("imsi", "18600640177"), ("netcount", "31"), ("fee", "1"))))

      val filter = new CoverageOperate();

      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      // 输入字段全部输出
      testOperation(input, operation, expectedOutput, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))

      verifyOutput(resultset, expectedHbase, true)
      logInfo("CoverageOperateStepSuite-" + caseName + " test example completion!")
    }
  }

  def test2(stepNum: Int, caseName: String) {

    test(caseName) {
      logInfo("CoverageOperateStepSuite-" + caseName + " test example start... ")

      val xmlFile = XML.load("src/test/resources/CoverageOperate-test.xml")

      val steps = xmlFile \ "step"
      val step = steps(stepNum)
      val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",")
      val family = "F"
      val table = (step \ "HBaseTable").text.toString.trim
      //      val table = "CoverageOperateStepSuite"
      testUtil.createTable(table, family)

      putValue(table, "18600640175", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640176", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640177", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))

      val input = Seq(
        Seq(Array(("imsi", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("imsi", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("imsi", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))))

      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "20"), (table + ".Fee", "10"))),
        Seq(Array.apply((table + ".NTcount", "33"), (table + ".Fee", "11"))),
        Seq(Array.apply((table + ".NTcount", "31"), (table + ".Fee", "1"))))
      val expectedOutput = Seq(Seq(Array.apply(("imsi", "18600640175"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array.apply(("imsi", "18600640176"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array.apply(("imsi", "18600640177"), ("netcount", "31"), ("fee", "1"))))

      val filter = new CoverageOperate();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      testOperation(input, operation, expectedOutput, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))

      verifyOutput(resultset, expectedHbase, true)
      logInfo("CoverageOperateStepSuite-" + caseName + " test example completion!")
    }
  }

}