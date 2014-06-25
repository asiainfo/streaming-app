package com.asiainfo.ocdc.streaming.impl

import org.apache.spark.Logging
import org.apache.hadoop.hbase.HBaseTestingUtility
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import com.asiainfo.ocdc.streaming.TestSuitBase
import scala.Tuple2
import scala.reflect.ClassTag
import scala.xml.XML
import org.apache.spark.streaming.dstream.DStream

class SaveOperateStepSuite extends TestSuitBase with Logging {

  logInfo("SaveOperateStepSuite test  started ")
  test1(0, "test1: test add value into hbase without initial data using stepid=0")
  test2(0, "test2: test update value based on exited value into hbase with initial data using stepid=1")
  
  test2_1(1, "test2_1: test update value based on exited value into hbase with initial data using stepid=1")
  test3(2, "test3: test update value based on stream value into hbase with initial data using stepid=2")
  test4(3, "test4: test update value based on stream value into hbase with initial data using stepid=3")
  
  logInfo("SaveOperateStepSuite test finished ")
  
  /**
   * test1: test add value into hbase without initial data using stepid=0 in xml
   */
  def test1(stepNum: Int, caseName: String) {

    test(caseName) {
      logInfo("SaveOperateStepSuite-" + caseName + " test example start... ")

      val xmlFile = XML.load("src/test/resources/SaveOperate-test.xml")

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

      val filter = new SaveOperate();

      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      // output all fields
      testOperation(input, operation, expectedOutput, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))

      verifyOutput(resultset, expectedHbase, true)
      logInfo("SaveOperateStepSuite-" + caseName + " test example completion!")
    }
  }
  
  /**
   * test2: test update value based on exited value into hbase with initial data using stepid=1 in xml
   */
  def test2(stepNum: Int, caseName: String) {

    test(caseName) {
      logInfo("SaveOperateStepSuite-" + caseName + " test example start... ")

      val xmlFile = XML.load("src/test/resources/SaveOperate-test.xml")

      val steps = xmlFile \ "step"
      val step = steps(stepNum)
      val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",")
      val family = "F"
      val table = (step \ "HBaseTable").text.toString.trim
      //      val table = "SaveOperateStepSuite"
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

      val filter = new SaveOperate();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      testOperation(input, operation, expectedOutput, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))

      verifyOutput(resultset, expectedHbase, true)
      logInfo("SaveOperateStepSuite-" + caseName + " test example completion!")
    }
  }
  
  /**
   * test2_1: test update value based on exited value into hbase with initial data using stepid=1 in xml
   */
  def test2_1(stepNum: Int, caseName: String) {

    test(caseName) {
      logInfo("SaveOperateStepSuite-" + caseName + " test example start... ")

      val xmlFile = XML.load("src/test/resources/SaveOperate-test.xml")

      val steps = xmlFile \ "step"
      val step = steps(stepNum)
      val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",")
      val family = "F"
      val table = (step \ "HBaseTable").text.toString.trim
      //      val table = "SaveOperateStepSuite"
      testUtil.createTable(table, family)

      putValue(table, "18600640175", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640176", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640177", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))

      val input = Seq(
        Seq(Array(("imsi", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("imsi", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("imsi", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))))

      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "120"), (table + ".Fee", "1010"))),
        Seq(Array.apply((table + ".NTcount", "133"), (table + ".Fee", "1011"))),
        Seq(Array.apply((table + ".NTcount", "131"), (table + ".Fee", "1001"))))
      val expectedOutput = Seq(Seq(Array.apply(("imsi", "18600640175"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array.apply(("imsi", "18600640176"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array.apply(("imsi", "18600640177"), ("netcount", "31"), ("fee", "1"))))

     val filter = new SaveOperate();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      testOperation(input, operation, expectedOutput, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))
      verifyOutput(resultset, expectedHbase, true)
      logInfo("SaveOperateStepSuite-" + caseName + " test example completion!")
    }
  }
  
  /**
   * test3: test update value based on stream value into hbase with initial data using stepid=2 in xml
   */
  def test3(stepNum: Int, caseName: String) {

    test(caseName) {
      logInfo("SaveOperateStepSuite-" + caseName + " test example start... ")

      val xmlFile = XML.load("src/test/resources/SaveOperate-test.xml")

      val steps = xmlFile \ "step"
      val step = steps(stepNum)
      val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",")
      val family = "F"
      val table = (step \ "HBaseTable").text.toString.trim
      //      val table = "SaveOperateStepSuite"
      testUtil.createTable(table, family)

      putValue(table, "18600640175", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640176", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640177", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))

      val input = Seq(
        Seq(Array(("imsi", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "10"), ("fee", "10"))),
        Seq(Array(("imsi", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "20"), ("fee", "20"))),
        Seq(Array(("imsi", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "30"), ("fee", "30"))))

      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "110"), (table + ".Fee", "20"))),
        Seq(Array.apply((table + ".NTcount", "120"), (table + ".Fee", "40"))),
        Seq(Array.apply((table + ".NTcount", "130"), (table + ".Fee", "60"))))
      val expectedOutput = Seq(Seq(Array.apply(("imsi", "18600640175"), ("netcount", "10"), ("fee", "10"))),
        Seq(Array.apply(("imsi", "18600640176"), ("netcount", "20"), ("fee", "20"))),
        Seq(Array.apply(("imsi", "18600640177"), ("netcount", "30"), ("fee", "30"))))
/*
      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "110"), (table + ".Fee", "10", "20"))),
        Seq(Array.apply((table + ".NTcount", "120"), (table + ".Fee", "20", "40"))),
        Seq(Array.apply((table + ".NTcount", "130"), (table + ".Fee", "30", "60"))))
      val expectedOutput = Seq(Seq(Array.apply(("imsi", "18600640175"), ("netcount", "110"), ("fee", "10", "20"))),
        Seq(Array.apply(("imsi", "18600640176"), ("netcount", "120"), ("fee", "20", "40"))),
        Seq(Array.apply(("imsi", "18600640177"), ("netcount", "130"), ("fee", "30", "60"))))
*/
      val filter = new SaveOperate();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      testOperation(input, operation, expectedOutput, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))

      verifyOutput(resultset, expectedHbase, true)
      logInfo("SaveOperateStepSuite-" + caseName + " test example completion!")
    }
  }
  
  /**
   * test4: test update value based on stream value into hbase with initial data using stepid=3 in xml
   */
  def test4(stepNum: Int, caseName: String) {

    test(caseName) {
      logInfo("SaveOperateStepSuite-" + caseName + " test example start... ")

      val xmlFile = XML.load("src/test/resources/SaveOperate-test.xml")

      val steps = xmlFile \ "step"
      val step = steps(stepNum)
      val hBaseCells = (step \ "HBaseCells").text.toString.trim.split(",")
      val family = "F"
      val table = (step \ "HBaseTable").text.toString.trim
      //      val table = "SaveOperateStepSuite"
      testUtil.createTable(table, family)

      putValue(table, "18600640175", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640176", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640177", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))

      val input = Seq(
        Seq(Array(("imsi", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "10"), ("fee", "10"))),
        Seq(Array(("imsi", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "20"), ("fee", "20"))),
        Seq(Array(("imsi", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "30"), ("fee", "30"))))

      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "110"), (table + ".Fee", "1020"))),
        Seq(Array.apply((table + ".NTcount", "120"), (table + ".Fee", "1040"))),
        Seq(Array.apply((table + ".NTcount", "130"), (table + ".Fee", "1060"))))
      val expectedOutput = Seq(Seq(Array.apply(("imsi", "18600640175"), ("netcount", "10"), ("fee", "10"))),
        Seq(Array.apply(("imsi", "18600640176"), ("netcount", "20"), ("fee", "20"))),
        Seq(Array.apply(("imsi", "18600640177"), ("netcount", "30"), ("fee", "30"))))

      val filter = new SaveOperate();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      testOperation(input, operation, expectedOutput, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))

      verifyOutput(resultset, expectedHbase, true)
      logInfo("SaveOperateStepSuite-" + caseName + " test example completion!")
    }
  }
}