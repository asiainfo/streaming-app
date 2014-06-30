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
class AggregateStepSuite extends TestSuitBase with Logging {

  logInfo("AggregateStepSuite test example started ")
//  step0(0, "--distinct test--")
//  step1(1, "--sum test--")
  step2(2, "--count test--")
  
  logInfo("AggregateStepSuite test example finished ")

  /**
   *  为hbase各rowkey设定初始基值的情况下计算流量，话费同时累加计算，并且筛选输入流字段做为输出
   */
  def step0(stepNum: Int, caseNmae: String) {
    test(caseNmae) {

      logInfo("AggregateStepSuite-" + caseNmae + " test example start... ")

      val xmlFile = XML.load("src/test/resources/polymerization-test.xml")
      val steps = xmlFile \ "step"
      val step = steps(stepNum)

      val input = Seq(
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("TELNo", "18600640176"), ("name", "surq"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("TELNo", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "30"), ("fee", "20"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "40"), ("fee", "31"))))

      val expectedOutput = Seq(
          Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq"))),
          Seq(Array.apply(("TELNo", "18600640176"), ("name", "surq"))),
          Seq(Array.apply(("TELNo", "18600640177"), ("name", "asia1"))))

      val filter = new AggregateStep();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      testOperation(input, operation, expectedOutput, true)

      logInfo("AggregateStepSuiteStep-" + caseNmae + " test example completion!")
    }
  }

  /**
   * 把输入的全部字段输出(附带累加计算)
   */
  def step1(stepNum: Int, caseNmae: String) {

    test(caseNmae) {

      logInfo("AggregateStepSuiteStep-" + caseNmae + " test example start... ")

      val xmlFile = XML.load("src/test/resources/polymerization-test.xml")
      val step = (xmlFile \ "step")(stepNum)

      val input = Seq(
        Seq(Array(("TELNo", "18600640175"), ("name", "surq1"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("TELNo", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("TELNo", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "30"), ("fee", "20"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "40"), ("fee", "31"))))


      val filter = new AggregateStep();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)
      val expectedOutput = Seq(Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq1"), ("netcount", "20"), ("fee", "10"))), 
          Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq"), ("netcount", "70"), ("fee", "51"))), 
          Seq(Array.apply(("TELNo", "18600640176"), ("name", "asia"), ("netcount", "33"), ("fee", "11"))), 
          Seq(Array.apply(("TELNo", "18600640177"), ("name", "asia1"), ("netcount", "31"), ("fee", "1"))))
      // 输入字段全部输出
      testOperation(input, operation, expectedOutput, true)

      logInfo("AggregateStepSuiteStep-" + caseNmae + " test example completion!")
    }
  }
  
  /**
   * 把流中的记录的个别字段更新到hbase
   */
  def step2(stepNum: Int, caseNmae: String) {

    test(caseNmae) {

      logInfo("AggregateStepSuite-" + caseNmae + " test example start... ")

      val xmlFile = XML.load("src/test/resources/polymerization-test.xml")
      val step = (xmlFile \ "step")(stepNum)

      val input = Seq(
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "20"), ("fee", "10"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "30"), ("fee", "20"))),
        Seq(Array(("TELNo", "18600640175"), ("name", "surq"), ("sex", "man"), ("netcount", "40"), ("fee", "30"))),
        Seq(Array(("TELNo", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "33"), ("fee", "11"))),
        Seq(Array(("TELNo", "18600640176"), ("name", "asia"), ("sex", "man"), ("netcount", "23"), ("fee", "11"))),
        Seq(Array(("TELNo", "18600640177"), ("name", "asia1"), ("sex", "man"), ("netcount", "31"), ("fee", "1"))))
        
      val expectedOutput = Seq(
          Seq(Array.apply(("TELNo", "18600640175"), ("name", "surq"), ("[count]", "3"))), 
          Seq(Array.apply(("TELNo", "18600640176"), ("name", "asia"), ("[count]", "2"))), 
          Seq(Array.apply(("TELNo", "18600640177"), ("name", "asia1"), ("[count]", "1"))))
          
      val filter = new AggregateStep();
      val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step, s)

      // 输入字段全部输出
      testOperation(input, operation, expectedOutput, true)
      logInfo("AggregateStepSuite-" + caseNmae + " test example completion!")
    }
  }
  
}
