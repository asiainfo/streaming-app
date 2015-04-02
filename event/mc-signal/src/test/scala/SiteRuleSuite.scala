package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat

import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by yfq on 15/4/2.
 */
class SiteRuleSuite extends FunSuite with BeforeAndAfter{

  val sdf=new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSSZ")
  val timeout=300

  test("1 "){
    val mc1_out = MCSourceObject(9, sdf.parse("20150401 08:20:00.000").getTime, 11, 1, 13900000000L ,13900000000L)
    val mc2=MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 11, 1, 13900000000L ,13900000000L)
    val mc3=MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 11, 1, 13900000000L ,13900000000L)

    val cache = new StreamingCache
    val rule=new SiteRule()

    rule.attachMCLabel(mc1_out, cache)
    assert(mc2.getLabel(Constant.LABEL_ONSITE).keySet==Set(1,3,9))
  }
}
