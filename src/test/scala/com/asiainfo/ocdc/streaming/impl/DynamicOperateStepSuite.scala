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
 * Created by liuhao8 on 14-6-16.
 */
class DynamicOperateStepSuite extends TestSuitBase with Logging {

  test("DynamicOperate test example") {
    logInfo("DynamicOperateStep test example started ")

    val xmlFile = XML.load("src/test/resources/DynamicOperate-test.xml")
    val step = xmlFile \ "step"
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


    val expectedOutput = Seq(Seq(Array.apply(("NTcount","190"),("Fee","1061"))), Seq(Array.apply(("NTcount","133"),("Fee","1011"))), Seq(Array.apply(("NTcount","131"),("Fee","1001"))))
    val filter = new DynamicOperate();
    val operation = (s: DStream[Array[(String, String)]]) => filter.onStep(step(0), s)
    
    testOperation(input, operation, input, true)
    
    val result75 = getValue(table, "18600640175", family, Array.apply("NTcount", "Fee"))
    val result76 = getValue(table, "18600640176", family, Array.apply("NTcount", "Fee"))
    val result77 = getValue(table, "18600640177", family, Array.apply("NTcount", "Fee"))
    val resultset = Seq(Seq(result75), Seq(result76), Seq(result77))
    
      resultset.foreach(f=>{
        f.foreach(x=>{
          x.foreach(k=>println("=====result============"+ k._1+":"+k._2))
        })
      })
      
    verifyOutput(resultset,expectedOutput, true)

    logInfo("DynamicOperate test example finished ")
  }

}
