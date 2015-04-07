package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat
import org.scalatest.{BeforeAndAfter, FunSuite}

import scala.collection.immutable
import scala.collection.mutable

/**
 * Created by yfq on 15/4/2.
 */
class LocationStayRuleSuite extends FunSuite with BeforeAndAfter {

	//in SourceObject, val labels = new java.util.HashMap[String, Map[String,String]]()
	//  map=immutable.Map[String,String]
	//  areaLabelMap=mutable.Map[String,String]()

	val sdf=new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")
	val map=immutable.Map[String,String]()

//  val areaLabelMap=immutable.Map[String,String]()	//报错
	var areaLabelMap=mutable.Map[String,String]()
	var cache:StreamingCache = _
	var rule:LocationStayRule = _
	var lrConf:LabelRuleConf = _

	before{
		cache = new LabelProps
		rule = new LocationStayRule()
		lrConf = new LabelRuleConf(map)
		lrConf.setAll(map)
		lrConf.set("classname","com.asiainfo.ocdc.streaming.LocationStayRule")
		lrConf.set("stay.limits", (20 * 60 * 1000).toString)
		lrConf.set("stay.matchMax", "true")
		lrConf.set("stay.outputThreshold", "true")
		lrConf.set("stay.timeout", (30 * 60 * 1000).toString)
		rule.init(lrConf)
	}

	after{
		areaLabelMap=mutable.Map[String,String]()
	}

	test("1 测试新用户首次MC的处理(cache没有缓存)") {
		MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
		TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")
		TextCacheManager.setCommonCacheValue("lacci2area", "112:1", "area1,area2")
		TextCacheManager.setCommonCacheValue("lacci2area", "123:1", "area1,area2,area3")

		assert(TextCacheManager.getCommonCacheValue("lacci2area","123:1")=="area1,area2,area3")

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 112, 1, 13900000002L ,13910000002L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 123, 1, 13900000003L ,13910000003L)
		
		assert(lrConf.get("classname")=="com.asiainfo.ocdc.streaming.LocationStayRule")

		areaLabelMap.put("area1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get==0 +"")

		areaLabelMap.put("area2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size==2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get==0 +"")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get==0 +"")

		areaLabelMap.put("area3","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size==3)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get==0 +"")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area2").get==0 +"")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area3").get==0 +"")
	}

	test("2 测试已有用户的MC的处理(cache中有缓存信息)"){
		MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
		TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 111, 1, 13900000001L ,13910000001L)

		

		areaLabelMap.put("area1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get==0 +"")

		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2,cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get==0 +"")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3,cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get==20 * 60 * 1000 +"")
	}

	test("3 测试某用户从区域A到区域B再到区域C，在回到A"){
		MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
		TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")
		TextCacheManager.setCommonCacheValue("lacci2area", "222:1", "area2")
		TextCacheManager.setCommonCacheValue("lacci2area", "333:1", "area3")

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 222, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 333, 1, 13900000001L ,13910000001L)
		val mc4=MCSourceObject(1, sdf.parse("20150401 08:37:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc5=MCSourceObject(1, sdf.parse("20150401 08:57:00.000").getTime, 111, 1, 13900000001L ,13910000001L)


		areaLabelMap.put("area1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get==0 +"")

		areaLabelMap.remove("area1")
		areaLabelMap.put("area2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get==0 +"")

		areaLabelMap.remove("area2")
		areaLabelMap.put("area3","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3,cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area3").get==0 +"")

		areaLabelMap.remove("area3")
		areaLabelMap.put("area1","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4,cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get==0 +"")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5,cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get==20 * 60 * 1000 +"")

	}

	test("4 测试某用户从区域A到区域A/B再到区域B/C"){

		MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
		TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")
		TextCacheManager.setCommonCacheValue("lacci2area", "122:1", "area1,area2")
		TextCacheManager.setCommonCacheValue("lacci2area", "223:1", "area2,area3")

		val mc1=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 122, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 122, 1, 13900000001L ,13910000001L)
		val mc4=MCSourceObject(1, sdf.parse("20150401 08:37:00.000").getTime, 223, 1, 13900000001L ,13910000001L)
		val mc5=MCSourceObject(1, sdf.parse("20150401 08:57:00.000").getTime, 223, 1, 13900000001L ,13910000001L)


		areaLabelMap.put("area1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get==0 +"")

		areaLabelMap.put("area2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size==2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get==0 + "")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3,cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size==2)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get==20 * 60 * 1000 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area2").get==0 + "")

		areaLabelMap.remove("area1")
		areaLabelMap.put("area3","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4,cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size==2)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get==20 * 60 * 1000 +"")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area3").get==0 +"")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5,cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size==2)
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area2").get==20 * 60 * 1000+"")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area3").get==20 * 60 * 1000+"")
	}

	test("5 测试用户在一个区域mc短时间乱序的问题"){
		MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
		TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc4=MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 111, 1, 13900000001L ,13910000001L)


		areaLabelMap.put("area1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get==0+"")

		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")


		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")


		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get==20 * 60 * 1000 + "")

	}

	test("6 测试用户在多个区域mc短时间乱序的问题"){
		MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
		TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")
		TextCacheManager.setCommonCacheValue("lacci2area", "121:1", "area1,area2")
		TextCacheManager.setCommonCacheValue("lacci2area", "123:1", "area1,area2,area3")

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 121, 1, 13900000001L ,13900000001L)

		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 123, 1, 13900000001L ,13900000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 123, 1, 13900000001L ,13900000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 123, 1, 13900000001L ,13900000001L)

		areaLabelMap.put("area1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")

		areaLabelMap.put("area2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size==2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get==0 + "")

		areaLabelMap.put("area3","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size==3)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area2").get==0 + "")
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area3").get==0 + "")

		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size==3)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get==20 * 60 * 1000 +"")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get==20 * 60 * 1000 +"")		//业务逻辑暂定
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area3").get==0 +"")

		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5, cache)
		assert(mc5.getLabel(Constant.LABEL_STAY).size==3)
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area1").get==20 * 60 * 1000 + "")
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area2").get==20 * 60 * 1000 + "")	//业务逻辑暂定
		assert(mc5.getLabel(Constant.LABEL_STAY).get("area3").get==0 + "")
	}

	test("7 测试用户在某区域mc长时间乱序的问题"){
		MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
		TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:03:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:20:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:23:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		areaLabelMap.put("area1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")

		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")

		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")

		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get==20 * 60 * 1000 + "")

	}

	test("8 测试用户在多个区域mc长时间乱序的问题"){
		MainFrameConf.set("DefaultCacheManager", "TextCacheManager")
		TextCacheManager.setCommonCacheValue("lacci2area", "111:1", "area1")
		TextCacheManager.setCommonCacheValue("lacci2area", "121:1", "area1,area2")
		TextCacheManager.setCommonCacheValue("lacci2area", "123:1", "area1,area2,area3")

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 121, 1, 13900000001L ,13900000001L)

		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:20:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		//		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:20:00.000").getTime, 121, 1, 13900000001L ,13900000001L)

		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:23:00.000").getTime, 123, 1, 13900000001L ,13900000001L)


		areaLabelMap.put("area1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc1.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")

		areaLabelMap.put("area1","true")
		areaLabelMap.put("area2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_STAY).size==2)
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area1").get==20 * 60 * 1000 + "")
		assert(mc2.getLabel(Constant.LABEL_STAY).get("area2").get==0 + "")

		areaLabelMap.remove("area2")
		areaLabelMap.put("area1","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_STAY).size==1)
		assert(mc3.getLabel(Constant.LABEL_STAY).get("area1").get==0 + "")	//如果一条记录无效，记录所在区域停留时间为0，不更新cache

		areaLabelMap.put("area2","true")
		areaLabelMap.put("area3","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.LABEL_STAY).size==3)
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area1").get==20 * 60 * 1000 + "")
		assert(mc4.getLabel(Constant.LABEL_STAY).get("area2").get==0 + "")

	}

}
