package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat
import org.scalatest.{BeforeAndAfter, FunSuite}
import scala.collection.{immutable, mutable}

/**
 * Created by yfq on 15/4/2.
 */
class SiteRuleSuite extends FunSuite with BeforeAndAfter {

	val sdf=new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")
	val map=immutable.Map[String,String]()

	var cache:StreamingCache = _
	var rule:SiteRule = _
	var lrConf:LabelRuleConf = _

	before{
		cache = new LabelProps
		rule = new SiteRule
		lrConf = new LabelRuleConf(map)
		lrConf.setAll(map)
		rule.init(lrConf)
	}

	test("1 test SiteRule"){

		MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
		TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")
		TextCacheManager.setCommonCacheValue("lacci2area", "112:1", "area1,area2")
		TextCacheManager.setCommonCacheValue("lacci2area", "123:1", "area1,area2,area3")

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 112, 1, 13900000002L ,13910000002L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 123, 1, 13900000003L ,13910000003L)


		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_ONSITE).size==1)
		assert(mc1.getLabel(Constant.LABEL_ONSITE).get("area1").get=="true")

		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_ONSITE).size==2)
		assert(mc2.getLabel(Constant.LABEL_ONSITE).get("area1").get=="true")
		assert(mc2.getLabel(Constant.LABEL_ONSITE).get("area2").get=="true")

		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_ONSITE).size==3)
		assert(mc3.getLabel(Constant.LABEL_ONSITE).get("area1").get=="true")
		assert(mc3.getLabel(Constant.LABEL_ONSITE).get("area2").get=="true")
		assert(mc3.getLabel(Constant.LABEL_ONSITE).get("area3").get=="true")

	}
}
