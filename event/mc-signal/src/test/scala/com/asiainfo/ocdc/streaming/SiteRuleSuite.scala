package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat
import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by yfq on 15 / 4 / 2.
 */
class SiteRuleSuite extends FunSuite with BeforeAndAfter {

  val sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")

  var cacher: CacheManager = _
  var cache: StreamingCache = _
  var rule: SiteRule = _
  var lrConf: LabelRuleConf = _

  before {
    cache = new StreamingCache
    rule = new SiteRule
    lrConf = new LabelRuleConf()
    rule.init(lrConf)
  }

  after {
    cacher = null
  }

/*
  //TODO: not supported CacheFactory.getManager 如何做到并存
  test("测试 CacherManager") {
    MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
    cacher = CacheFactory.getManager.asInstanceOf[TextCacheManager]
    assert(cacher.getClass.getName == "com.asiainfo.ocdc.streaming.TextCacheManager")

    MainFrameConf.set("DefaultCacheManager", "CodisCacheManager")
    cacher = CacheFactory.getManager.asInstanceOf[CodisCacheManager]
    assert(cacher.getClass.getName == "com.asiainfo.ocdc.streaming.CodisCacheManager")
  }
*/

  test("1 test SiteRule with textCacheManager") {

    MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
    cacher = CacheFactory.getManager

    cacher.setCommonCacheValue("lacci2area", "111:1", "area1")
    cacher.setCommonCacheValue("lacci2area", "112:1", "area1,area2")
    cacher.setCommonCacheValue("lacci2area", "123:1", "area1,area2,area3")

    val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, "111", "1", "13900000001", "13900000001")
    val mc2 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, "112", "1", "13900000002", "13910000002")
    val mc3 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, "123", "1", "13900000003", "13910000003")

    rule.attachMCLabel(mc1, cache)
    assert(mc1.getLabels(Constant.LABEL_ONSITE).size == 1)
    assert(mc1.getLabels(Constant.LABEL_ONSITE).get("area1").get == "true")

    rule.attachMCLabel(mc2, cache)
    assert(mc2.getLabels(Constant.LABEL_ONSITE).size == 2)
    assert(mc2.getLabels(Constant.LABEL_ONSITE).get("area1").get == "true")
    assert(mc2.getLabels(Constant.LABEL_ONSITE).get("area2").get == "true")

    rule.attachMCLabel(mc3, cache)
    assert(mc3.getLabels(Constant.LABEL_ONSITE).size == 3)
    assert(mc3.getLabels(Constant.LABEL_ONSITE).get("area1").get == "true")
    assert(mc3.getLabels(Constant.LABEL_ONSITE).get("area2").get == "true")
    assert(mc3.getLabels(Constant.LABEL_ONSITE).get("area3").get == "true")

  }

/*
  /**
   * TODO: can not test
   */
  test("2 test SiteRule with CodisCacheManager") {

    MainFrameConf.set("DefaultCacheManager", "CodisCacheManager")
    MainFrameConf.set("CodisProxy", "redis1:6379")
    MainFrameConf.set("JedisMEM", "10000")
    MainFrameConf.set("JedisMaxActive", "100")
    MainFrameConf.set("JedisMaxActive", "15")

    cacher = CacheFactory.getManager

    cacher.setCommonCacheValue("lacci2area", "111:1", "area1")
    cacher.setCommonCacheValue("lacci2area", "112:1", "area1,area2")
    cacher.setCommonCacheValue("lacci2area", "123:1", "area1,area2,area3")

    val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, "111", "1", "13900000001", "13900000001")
    val mc2 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, "112", "1", "13900000002", "13910000002")
    val mc3 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, "123", "1", "13900000003", "13910000003")

    rule.attachMCLabel(mc1, cache)
    assert(mc1.getLabels(Constant.LABEL_ONSITE).size == 1)
    assert(mc1.getLabels(Constant.LABEL_ONSITE).get("area1").get == "true")

    rule.attachMCLabel(mc2, cache)
    assert(mc2.getLabels(Constant.LABEL_ONSITE).size == 2)
    assert(mc2.getLabels(Constant.LABEL_ONSITE).get("area1").get == "true")
    assert(mc2.getLabels(Constant.LABEL_ONSITE).get("area2").get == "true")

    rule.attachMCLabel(mc3, cache)
    assert(mc3.getLabels(Constant.LABEL_ONSITE).size == 3)
    assert(mc3.getLabels(Constant.LABEL_ONSITE).get("area1").get == "true")
    assert(mc3.getLabels(Constant.LABEL_ONSITE).get("area2").get == "true")
    assert(mc3.getLabels(Constant.LABEL_ONSITE).get("area3").get == "true")
  }
  */
}
