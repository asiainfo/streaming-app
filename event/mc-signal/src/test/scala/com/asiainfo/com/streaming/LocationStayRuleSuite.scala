package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat
import org.scalatest.{BeforeAndAfter, FunSuite}


/**
 * Created by yfq on 15/4/2.
 */
class LocationStayRuleSuite extends FunSuite with BeforeAndAfter {

  // parttern="yyyyMMdd HH:mm:ss.SSSZ" in java1.7 OK, in java1.6 报错
//  val sdf=new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSSZ")
  val sdf=new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")

  test("1 test with sample cases") {

    val mc1_out = MCSourceObject(9, sdf.parse("20150401 08:20:00.000").getTime, 111, 1, 13900000000L ,13900000000L)
    val mc2=MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 112, 1, 13900000000L ,13900000000L)
    val mc3=MCSourceObject(1, sdf.parse("20150401 08:21:00.000").getTime, 123, 1, 13900000000L ,13900000000L)
    val mc4=MCSourceObject(1, sdf.parse("20150401 08:22:00.000").getTime, 333, 1, 13900000000L ,13900000000L)
    val mc5=MCSourceObject(1, sdf.parse("20150401 08:42:00.000").getTime, 123, 1, 13900000000L ,13900000000L)

    val cache = new StreamingCache
    val rule=new LocationStayRule()

    rule.attachMCLabel(mc1_out, cache)
    assert(mc1_out.getLabel(Constant.LABEL_ONSITE).size==0)
    assert(mc1_out.getLabel(Constant.STAY_TIME).size==0)

    rule.attachMCLabel(mc2,cache)
    //    val tmp1=mc2.getLabel(Constant.STAY_TIME)
    assert(mc2.getLabel(Constant.LABEL_ONSITE).size==2)
    assert(mc2.getLabel(Constant.STAY_TIME).size==0)

    rule.attachMCLabel(mc3,cache)
    assert(mc3.getLabel(Constant.LABEL_ONSITE).size==3)
    assert(mc3.getLabel(Constant.STAY_TIME).size==2)
    assert(mc3.getLabel(Constant.STAY_TIME).get("1")=="20")
    assert(mc3.getLabel(Constant.STAY_TIME).get("2")=="20")

    rule.attachMCLabel(mc4,cache)
    assert(mc4.getLabel(Constant.LABEL_ONSITE).size==1)
    assert(mc4.getLabel(Constant.STAY_TIME).size==1)
    assert(mc3.getLabel(Constant.STAY_TIME).get("3")=="20")

    rule.attachMCLabel(mc5,cache)
    assert(mc5.getLabel(Constant.LABEL_ONSITE).size==3)
    assert(mc5.getLabel(Constant.STAY_TIME).size==3)
    assert(mc5.getLabel(Constant.STAY_TIME).get("1")=="20")
    assert(mc5.getLabel(Constant.STAY_TIME).get("2")=="20")
    assert(mc5.getLabel(Constant.STAY_TIME).get("3")=="20")

  }
}
