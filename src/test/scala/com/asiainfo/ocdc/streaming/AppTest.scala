package com.asiainfo.ocdc.streaming

import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.SparkContext
import org.scalatest.{FunSuite, BeforeAndAfter}

/**
 * Created by luogk on 14-6-11.
 */
class AppTest extends FunSuite with BeforeAndAfter{
  val master = "local[2]"
  val appName = this.getClass.getSimpleName
  val batchDuration = Milliseconds(500)
  val sparkHome = "someDir"
  val envPair = "key" -> "value"

  var sc: SparkContext = null
  var ssc: StreamingContext = null

  after {
    if (ssc != null) {
      ssc.stop()
      ssc = null
    }
    if (sc != null) {
      sc.stop()
      sc = null
    }
  }

  before{
    ssc = new StreamingContext(master, appName, batchDuration, sparkHome, Nil)
  }

  test("from no conf + spark home") {
    assert(ssc.sparkContext.getConf.get("spark.home") === sparkHome)
  }


  test("from no conf constructor") {

  }

}
