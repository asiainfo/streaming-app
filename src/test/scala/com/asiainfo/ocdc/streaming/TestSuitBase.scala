package com.asiainfo.ocdc.streaming

import org.apache.spark.Logging
import scala.reflect.ClassTag
import org.apache.hadoop.hbase.HBaseTestingUtility
import com.asiainfo.ocdc.streaming.tools.HbaseTool
import scala.Tuple2

trait TestSuitBase extends org.apache.spark.streaming.TestSuiteBase with Logging{

  val testUtil = MiniHbase.testUtil


  override def afterFunction{
    HbaseTool.table.toArray.foreach(x=>testUtil.deleteTable(x._1))
    HbaseTool.table.clear()
  }

  override def verifyOutput[V: ClassTag](
                                          output: Seq[Seq[V]],
                                          expectedOutput: Seq[Seq[V]],
                                          useSet: Boolean
                                          ) {

    logInfo("output.size = " + output.size)
    logInfo("output")
    output.asInstanceOf[Seq[Seq[Array[Tuple2[String,String]]]]](0)(0)(0) match {
      case (_, _) =>
        output.asInstanceOf[Seq[Seq[Array[Tuple2[String,String]]]]].foreach {
          //Seq[Array[Tuple2[String,String]]]
          x => x.foreach {
            //Array[Tuple2[String,String]]
            y =>
              for (t <- y) {
                //Tuple2[String,String]
                logInfo("1: "+t._1+" 2: "+t._2)
              }
          }
        }
    }
    logInfo("expected output.size = " + expectedOutput.size)
    logInfo("expected output")
    expectedOutput.asInstanceOf[Seq[Seq[Array[Tuple2[String,String]]]]](0)(0)(0) match {
      case (_, _) =>
        expectedOutput.asInstanceOf[Seq[Seq[Array[Tuple2[String,String]]]]].foreach {
          //Seq[Array[Tuple2[String,String]]]
          x => x.foreach {
            //Array[Tuple2[String,String]]
            y =>
              for (t <- y) {
                //Tuple2[String,String]
                logInfo("1: "+t._1+" 2: "+t._2)
              }
          }
        }
    }

    // Match the output with the expected output
    assert(output.size === expectedOutput.size, "Number of outputs do not match")

    val lv1Size = expectedOutput.size
    val lv2Size = expectedOutput(0).size
    var isFound = false
    for (i <- 0 until lv1Size) {
      for (j <- 0 until lv2Size) {
        val outputArr = output.asInstanceOf[Seq[Seq[Array[Tuple2[String,String]]]]](i)(j)
        val expectArr = expectedOutput.asInstanceOf[Seq[Seq[Array[Tuple2[String,String]]]]](i)(j)
        for (e <- expectArr) {
          isFound = false
          for (o <- outputArr) {
            if(o._1.equals(e._1) && o._2.equals(e._2)) {
              isFound = true
            }
          }
          assert(isFound, "can't find line \nexpact["+e._1+","+e._2+"]")
        }
      }
    }

  }
}

object MiniHbase{
  val testUtil = {
    val testUtil = HBaseTestingUtility.createLocalHTU()
    HbaseTool.setConf(testUtil.getConfiguration)
    testUtil.startMiniCluster(1)
    testUtil
  }
}
