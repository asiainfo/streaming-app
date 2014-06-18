package com.asiainfo.ocdc.streaming.impl

import org.apache.spark.Logging
import org.apache.hadoop.hbase.HBaseTestingUtility
import com.asiainfo.ocdc.streaming.tools.HbaseTool._
import com.asiainfo.ocdc.streaming.TestSuitBase
import scala.Tuple2
import scala.reflect.ClassTag
import scala.xml.XML
import org.apache.spark.streaming.dstream.DStream

/**
 * @author surq
 * @since 2014-06-17
 */
class DynamicOperateStepSuite extends TestSuitBase with Logging {

  logInfo("DynamicOperateStep test example started ")
  step0(0, "为hbase各rowkey设定初始基值的情况下计算流量，话费同时累加计算，并且筛选输入流字段做为输出")
  step0_1(0, "hbase为初始状态没有任何数据的情况下计算流量，话费同时累加计算，并且筛选输入流字段做为输出")
  step1(1, "把输入的全部字段输出(附带累加计算)")
  step2(2, "把流中的记录的个别字段更新到hbase(hbase初始有基础数据)")
  
  logInfo("DynamicOperate test example finished ")

  /**
   *  为hbase各rowkey设定初始基值的情况下计算流量，话费同时累加计算，并且筛选输入流字段做为输出
   */
  def step0(stepNum: Int, caseNmae: String) {
    test(caseNmae) {

      logInfo("DynamicOperateStep-" + caseNmae + " test example start... ")

      val xmlFile = XML.load("src/test/resources/DynamicOperate-test.xml")
      val steps = xmlFile \ "step"
      val step = steps(stepNum)
      val family = "F"
      val table = (step \ "HBaseTable").text.toString.trim

      testUtil.createTable(table, family)
      putValue(table, "18600640175", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640176", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640177", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))

      val input = Seq(
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("TELNo", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("TELNo", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "30"), ("fee", "20"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "40"), ("fee", "31"))))

      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "190"), (table + ".Fee", "1061"))), Seq(Array.apply((table + ".NTcount", "133"), (table + ".Fee", "1011"))), Seq(Array.apply((table + ".NTcount", "131"), (table + ".Fee", "1001"))))
      val expectedOutput = Seq(Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq"))), Seq(Array.apply(("TELNo", "18600640176"), ("name", "asia"))), Seq(Array.apply(("TELNo", "18600640177"), ("name", "asia1"))), Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq"))), Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq"))))

      val filter = new DynamicOperate();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      testOperation(input, operation, expectedOutput, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))
      //      resultset.foreach(f=>{
      //        f.foreach(x=>{
      //          x.foreach(k=>println("=====result============"+ k._1+":"+k._2))
      //        })
      //      })

      verifyOutput(resultset, expectedHbase, true)
      logInfo("DynamicOperateStep-" + caseNmae + " test example completion!")
    }
  }

  /**
   * hbase为初始状态没有任何数据的情况下计算流量，话费同时累加计算，并且筛选输入流字段做为输出
   */
  def step0_1(stepNum: Int, caseNmae: String) {
    test(caseNmae) {

      logInfo("DynamicOperateStep-" + caseNmae + " test example start... ")

      val xmlFile = XML.load("src/test/resources/DynamicOperate-test.xml")
      val step = (xmlFile \ "step")(stepNum)
      val family = "F"

      val table = (step \ "HBaseTable").text.toString.trim

      testUtil.createTable(table, family)

      val input = Seq(
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("TELNo", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("TELNo", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "30"), ("fee", "20"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "40"), ("fee", "31"))))

      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "90"), (table + ".Fee", "61"))), Seq(Array.apply((table + ".NTcount", "33"), (table + ".Fee", "11"))), Seq(Array.apply((table + ".NTcount", "31"), (table + ".Fee", "1"))))
      val expectedOutput = Seq(Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq"))), Seq(Array.apply(("TELNo", "18600640176"), ("name", "asia"))), Seq(Array.apply(("TELNo", "18600640177"), ("name", "asia1"))), Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq"))), Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq"))))

      val filter = new DynamicOperate();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      testOperation(input, operation, expectedOutput, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))

      verifyOutput(resultset, expectedHbase, true)
      logInfo("DynamicOperateStep-" + caseNmae + " test example completion!")
    }
  }

  /**
   * 把输入的全部字段输出(附带累加计算)
   */
  def step1(stepNum: Int, caseNmae: String) {

    test(caseNmae) {

      logInfo("DynamicOperateStep-" + caseNmae + " test example start... ")

      val xmlFile = XML.load("src/test/resources/DynamicOperate-test.xml")
      val step = (xmlFile \ "step")(stepNum)
      val family = "F"

      val table = (step \ "HBaseTable").text.toString.trim

      testUtil.createTable(table, family)
      putValue(table, "18600640175", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640176", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640177", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))

      val input = Seq(
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("TELNo", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("TELNo", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "30"), ("fee", "20"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "40"), ("fee", "31"))))

      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "190"), (table + ".Fee", "1061"))), Seq(Array.apply((table + ".NTcount", "133"), (table + ".Fee", "1011"))), Seq(Array.apply((table + ".NTcount", "131"), (table + ".Fee", "1001"))))

      val filter = new DynamicOperate();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      // 输入字段全部输出
      testOperation(input, operation, input, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))

      verifyOutput(resultset, expectedHbase, true)
      logInfo("DynamicOperateStep-" + caseNmae + " test example completion!")
    }
  }
  
  /**
   * 把流中的记录的个别字段更新到hbase
   */
  def step2(stepNum: Int, caseNmae: String) {

    test(caseNmae) {

      logInfo("DynamicOperateStep-" + caseNmae + " test example start... ")

      val xmlFile = XML.load("src/test/resources/DynamicOperate-test.xml")
      val step = (xmlFile \ "step")(stepNum)
      val family = "F"

      val table = (step \ "HBaseTable").text.toString.trim

      testUtil.createTable(table, family)
      putValue(table, "18600640175", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640176", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))
      putValue(table, "18600640177", family, Array.apply(("NTcount", "100"), ("Fee", "1000")))

      val input = Seq(
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("TELNo", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("TELNo", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))))

      val expectedHbase = Seq(Seq(Array.apply((table + ".NTcount", "20"), (table + ".Fee", "10"))), Seq(Array.apply((table + ".NTcount", "33"), (table + ".Fee", "11"))), Seq(Array.apply((table + ".NTcount", "31"), (table + ".Fee", "1"))))

      val filter = new DynamicOperate();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      // 输入字段全部输出
      testOperation(input, operation, input, true)

      val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
      val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
      val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
      val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))

      verifyOutput(resultset, expectedHbase, true)
      logInfo("DynamicOperateStep-" + caseNmae + " test example completion!")
    }
  }
  
}
