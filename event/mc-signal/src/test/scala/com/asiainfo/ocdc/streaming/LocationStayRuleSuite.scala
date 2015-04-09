/*
package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.mutable

/**
 * Created by yfq on 15/4/2.
 */
class LocationStayRuleSuite extends FunSuite with BeforeAndAfter {

	val sdf = new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")

	MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
	TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")
	TextCacheManager.setCommonCacheValue("lacci2area", "222:1", "area2")
	TextCacheManager.setCommonCacheValue("lacci2area", "333:1", "area3")

	TextCacheManager.setCommonCacheValue("lacci2area", "112:1", "area1,area2")
	TextCacheManager.setCommonCacheValue("lacci2area", "122:1", "area1,area2")
	TextCacheManager.setCommonCacheValue("lacci2area", "121:1", "area1,area2")

	TextCacheManager.setCommonCacheValue("lacci2area", "223:1", "area2,area3")
	TextCacheManager.setCommonCacheValue("lacci2area", "123:1", "area1,area2,area3")

	var areaLabelMap = mutable.Map[String, String]()
	var cache: StreamingCache = _
	var rule: LocationStayRule = _
	var lrConf: LabelRuleConf = _

	before {
		cache = new LabelProps
		rule = new LocationStayRule()
		lrConf = new LabelRuleConf()
		lrConf.set("classname", "com.asiainfo.ocdc.streaming.LocationStayRule")
		lrConf.set("stay.limits", (20 * 60 * 1000).toString)
		lrConf.set("stay.matchMax", "true")
		lrConf.set("stay.outputThreshold", "true")
		lrConf.set("stay.timeout", (30 * 60 * 1000).toString)
		rule.init(lrConf)
	}

	after {
		areaLabelMap = mutable.Map[String, String]()
	}

	test("1 测试新用户首次MC的处理(cache没有缓存)") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 112, 1, 13900000002L, 13910000002L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 123, 1, 13900000003L, 13910000003L)

		assert(lrConf.get("classname") == "com.asiainfo.ocdc.streaming.LocationStayRule")

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		areaLabelMap.put("area2", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		areaLabelMap.put("area3", "true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 3)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")
	}

	test("2 测试已有用户的MC的处理(cache中有缓存信息)") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:37:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:57:00.000").getTime, 111, 1, 13900000001L, 13910000001L)

		//首次
		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		//没有达到时间
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		//达到时间
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")

		//长时间停留在一个地方，超过了一定时间
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 1)
		//TODO: 确认已超过指定时长后，是否不再打标签
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
//		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 1)
		//TODO: 确认已超过指定时长后，是否不再打标签
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")

	}

	test("3 测试某用户从区域A到区域B再到区域C，超过一定时间回到D") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 222, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 333, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:37:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:57:00.000").getTime, 111, 1, 13900000001L, 13910000001L)

		//首次到区域area1
		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		//首次到区域area2
		areaLabelMap.remove("area1")
		areaLabelMap.put("area2", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		//首次到区域area3
		areaLabelMap.remove("area2")
		areaLabelMap.put("area3", "true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		//超过cache timeout，再次到区域area1
		areaLabelMap.remove("area3")
		areaLabelMap.put("area1", "true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		//待在区域area1，达到时长
		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
	}

	test("3-2 测试某用户从区域A到区域B再到区域C，在一定时间内回到A") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:07:00.000").getTime, 222, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 333, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:57:00.000").getTime, 111, 1, 13900000001L, 13910000001L)

		val mc6 = MCSourceObject(1, sdf.parse("20150401 09:00:00.000").getTime, 111, 1, 13900000001L, 13910000001L)

		//首次到区域area1
		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		//首次到区域area2
		areaLabelMap.remove("area1")
		areaLabelMap.put("area2", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		//首次到区域area3
		areaLabelMap.remove("area2")
		areaLabelMap.put("area3", "true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		//在一定时间内，再次到区域area1
		areaLabelMap.remove("area3")
		areaLabelMap.put("area1", "true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 1)
		//TODO: 确认是否需要更精确的判断
//		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")

		//待在区域area1，达到时长
		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 1)
		//TODO: 已满足时长条件，确认是否继续打标签
//		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")


		mc6.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc6, cache)
		assert(mc6.getLabel(Constant.LABEL_STAY).size == 1)
		//TODO: 已满足时长条件，确认是否继续打标签
		//		assert(mc6.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
	}

	test("4 测试某用户从区域A到区域A/B再到区域B/C") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 122, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 122, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:37:00.000").getTime, 223, 1, 13900000001L, 13910000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:57:00.000").getTime, 223, 1, 13900000001L, 13910000001L)
		val mc6 = MCSourceObject(1, sdf.parse("20150401 08:58:00.000").getTime, 223, 1, 13900000001L, 13910000001L)
		val mc7 = MCSourceObject(1, sdf.parse("20150401 09:28:00.000").getTime, 223, 1, 13900000001L, 13910000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		areaLabelMap.put("area2", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		areaLabelMap.remove("area1")
		areaLabelMap.put("area3", "true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 2)
		//TODO: 在 area2 连续停留超过20分钟，这次都打标签，和 test3 不一样
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area3").get == 20 * 60 * 1000 + "")

		mc6.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc6, cache)
		assert(mc6.getLabel(Constant.LABEL_STAY).size == 2)
		//TODO: 在 area2 连续停留超过20分钟，这次不打标签，和 上一次处理不一样； 在 area3连续停留超过20分钟，这次打标签
//		assert(mc6.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area3").get == 20 * 60 * 1000 + "")

		mc7.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc7, cache)
		assert(mc7.getLabel(Constant.LABEL_STAY).size == 2)
		//TODO: 在 area2 连续停留超过20分钟，这次不打标签； 在 area3连续停留超过20分钟，这次打标签
//				assert(mc7.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		assert(mc7.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
//		assert(mc7.getLabel(Constant.LABEL_STAY).get("area3").get == 20 * 60 * 1000 + "")
		assert(mc7.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

	}

	test("4-2 测试多区域标签连续停留某区域的处理") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 122, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:40:00.000").getTime, 122, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 09:00:00.000").getTime, 223, 1, 13900000001L, 13910000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 09:21:00.000").getTime, 123, 1, 13900000001L, 13910000001L)
		val mc6 = MCSourceObject(1, sdf.parse("20150401 09:41:00.000").getTime, 123, 1, 13900000001L, 13910000001L)
		val mc7 = MCSourceObject(1, sdf.parse("20150401 10:12:00.000").getTime, 123, 1, 13900000001L, 13910000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		areaLabelMap.put("area2", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")

		areaLabelMap.remove("area1")
		areaLabelMap.put("area3", "true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		areaLabelMap.put("area1", "true")
		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 3)
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area3").get == 20 * 60 * 1000 + "")

		mc6.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc6, cache)
		assert(mc6.getLabel(Constant.LABEL_STAY).size == 3)
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		mc7.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc7, cache)
		assert(mc7.getLabel(Constant.LABEL_STAY).size == 3)
		assert(mc7.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc7.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
		assert(mc7.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

	}


	test("5 测试用户在一个区域A，mc短时间乱序的问题") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:13:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 111, 1, 13900000001L, 13910000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")

	}

	//TODO: 确认乱序的mc是否满足打标签
	test("5-2 测试用户在一个区域A,mc短时间乱序的问题, 乱序的mc满足打标签的情况") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:13:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:41:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:14:00.000").getTime, 111, 1, 13900000001L, 13910000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")

		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 1)
		//TODO: 连续满足时长条件，确认是否继续打标签
//		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

	}

	test("6 测试用户在区域A，mc长时间乱序的问题") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:03:00.000").getTime, 111, 1, 13900000001L, 13900000001L)

		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:29:00.000").getTime, 111, 1, 13900000001L, 13900000001L)

		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:11:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 111, 1, 13900000001L, 13900000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		//超过一定时长（30分钟），忽略
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")

	}

	test("7 测试用户在多个区域,mc短时间乱序的问题") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 121, 1, 13900000001L, 13900000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 123, 1, 13900000001L, 13900000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 123, 1, 13900000001L, 13900000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 123, 1, 13900000001L, 13900000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		areaLabelMap.put("area2", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		areaLabelMap.put("area3", "true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 3)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 3)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		//TODO: 是否需要需要更精确的判断，
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "") //业务逻辑暂定
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 3)
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "") //业务逻辑暂定
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")
	}

	//TODO: 确认乱序的mc是否满足打标签
	test("7-2 测试用户在多个区域,mc短时间乱序的问题，乱序的mc满足打标签的情况") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:40:00.000").getTime, 121, 1, 13900000001L, 13900000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 123, 1, 13900000001L, 13900000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 123, 1, 13900000001L, 13900000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:37:00.000").getTime, 123, 1, 13900000001L, 13900000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		areaLabelMap.put("area2", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		areaLabelMap.put("area3", "true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 3)
//		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		assert(mc3.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 3)
//		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
//		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 3)
//		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
//		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area3").get == 20 * 60 * 1000 + "")
	}

	test("8 测试用户在多个区域，mc长时间乱序的问题") {

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:05:00.000").getTime, 121, 1, 13900000001L, 13900000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:34:00.000").getTime, 121, 1, 13900000001L, 13900000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:25:00.000").getTime, 123, 1, 13900000001L, 13900000001L)


		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		areaLabelMap.put("area1", "true")
		areaLabelMap.put("area2", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "") //如果一条记录无效，记录所在区域停留时间为0，不更新cache

		areaLabelMap.put("area3", "true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 3)
//		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")

	}

	test("9 测试某用户不连续区域问题"){

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:01:00.000").getTime, 223, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:30:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:31:00.000").getTime, 121, 1, 13900000001L, 13910000001L)
		val mc6 = MCSourceObject(1, sdf.parse("20150401 08:32:00.000").getTime, 333, 1, 13900000001L, 13910000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		areaLabelMap.remove("area1")
		areaLabelMap.put("area2", "true")
		areaLabelMap.put("area3", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		areaLabelMap.remove("area2")
		areaLabelMap.remove("area3")
		areaLabelMap.put("area1", "true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")

		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")

		areaLabelMap.put("area2", "true")
		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 2)
		//TODO: 连续满足时长条件，确认是否继续打标签
//		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		assert(mc5.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")

		areaLabelMap.remove("area1")
		areaLabelMap.remove("area2")
		areaLabelMap.put("area3", "true")
		mc6.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc6, cache)
		assert(mc6.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

	}

	test("10 测试某用户在区域A、多级停留时间标签设置的处理") {

		lrConf.set("stay.limits", (20 * 60 * 1000).toString + "," + (10 * 60 * 1000).toString + "," + (5 * 60 * 1000).toString)

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:03:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:05:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:08:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc6 = MCSourceObject(1, sdf.parse("20150401 08:13:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc7 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 111, 1, 13900000001L, 13910000001L)
		val mc8 = MCSourceObject(1, sdf.parse("20150401 08:23:00.000").getTime, 111, 1, 13900000001L, 13910000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 5 * 60 * 1000 + "")

		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get == 5 * 60 * 1000 + "")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get == 10 * 60 * 1000 + "")

		mc6.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc6, cache)
		assert(mc6.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area1").get == 10 * 60 * 1000 + "")

		mc7.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc7, cache)
		assert(mc7.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc7.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")


		mc8.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc8, cache)
		assert(mc8.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc8.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")

	}

	test("11 测试某用户在多个区域、多级停留时间标签设置的处理") {

		lrConf.set("stay.limits", (20 * 60 * 1000).toString + "," + (10 * 60 * 1000).toString + "," + (5 * 60 * 1000).toString)

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L, 13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:03:00.000").getTime, 121, 1, 13900000001L, 13910000001L)
		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:05:00.000").getTime, 121, 1, 13900000001L, 13910000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:08:00.000").getTime, 223, 1, 13900000001L, 13910000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 223, 1, 13900000001L, 13910000001L)
		val mc6 = MCSourceObject(1, sdf.parse("20150401 08:13:00.000").getTime, 223, 1, 13900000001L, 13910000001L)
		val mc7 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 123, 1, 13900000001L, 13910000001L)
		val mc8 = MCSourceObject(1, sdf.parse("20150401 08:23:00.000").getTime, 123, 1, 13900000001L, 13910000001L)
		val mc9 = MCSourceObject(1, sdf.parse("20150401 08:28:00.000").getTime, 123, 1, 13900000001L, 13910000001L)

		areaLabelMap.put("area1", "true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size == 1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")

		areaLabelMap.put("area2", "true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get == 5 * 60 * 1000 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get == 0 + "")

		areaLabelMap.remove("area1")
		areaLabelMap.put("area3", "true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get == 5 * 60 * 1000 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area2").get == 5 * 60 * 1000 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		mc6.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc6, cache)
		assert(mc6.getLabel(Constant.LABEL_STAY).size == 2)
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area2").get == 10 * 60 * 1000 + "")
		assert(mc6.getLabel(Constant.LABEL_STAY).get("area3").get == 5 * 60 * 1000 + "")

		areaLabelMap.put("area1", "true")
		mc7.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc7, cache)
		assert(mc7.getLabel(Constant.LABEL_STAY).size == 3)
		assert(mc7.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc7.getLabel(Constant.LABEL_STAY).get("area2").get == 10 * 60 * 1000 + "")
		assert(mc7.getLabel(Constant.LABEL_STAY).get("area3").get == 10 * 60 * 1000 + "")


		mc8.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc8, cache)
		assert(mc8.getLabel(Constant.LABEL_STAY).size == 3)
		assert(mc8.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc8.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		//TODO: 连续满足时长条件，确认是否继续打标签
//		assert(mc8.getLabel(Constant.LABEL_STAY).get("area3").get == 10 * 60 * 1000 + "")
		assert(mc8.getLabel(Constant.LABEL_STAY).get("area3").get == 0 + "")

		mc9.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc9, cache)
		assert(mc9.getLabel(Constant.LABEL_STAY).size == 3)
		//TODO: 连续满足时长条件，确认是否继续打标签
//		assert(mc9.getLabel(Constant.LABEL_STAY).get("area1").get == 20 * 60 * 1000 + "")
		assert(mc9.getLabel(Constant.LABEL_STAY).get("area1").get == 0 + "")
		assert(mc9.getLabel(Constant.LABEL_STAY).get("area2").get == 20 * 60 * 1000 + "")
		assert(mc9.getLabel(Constant.LABEL_STAY).get("area3").get == 20 * 60 * 1000 + "")

	}
}
*/
