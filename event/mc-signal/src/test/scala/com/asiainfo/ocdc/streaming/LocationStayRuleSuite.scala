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
	//  stayLabelMap=mutable.Map[String,String]()

	val sdf=new SimpleDateFormat("yyyyMMdd HH:mm:ss.SSS")
	val map=immutable.Map[String,String]()
	map + ("classname"->"com.asiainfo.ocdc.streaming.LocationStayRule")
	map + ("labelrule.StayTime"->"20")


	test("1 测试新用户首次MC的处理(cache没有缓存)") {

		//    val stayLabelMap=immutable.Map[String,String]()	//报错
		val stayLabelMap=mutable.Map[String,String]()
		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 112, 1, 13900000002L ,13910000002L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 123, 1, 13900000003L ,13910000003L)

		val cache = new StreamingCache
		val rule=new LocationStayRule()
		val lconf=new LabelRuleConf(map)


		rule.init(lconf)
		assert(lconf.get("labelrule.StayTime")=="20")


		stayLabelMap.put("1","true")
		mc3.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)


		stayLabelMap.put("2","true")
		mc3.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)


		stayLabelMap.put("3","true")
		mc3.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==0)
	}

	test("2 测试已有用户的MC的处理(cache中有缓存信息)"){
		val stayLabelMap=mutable.Map[String,String]()

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 111, 1, 13900000001L ,13910000001L)

		val cache = new StreamingCache
		val rule=new LocationStayRule()
		val lconf=new LabelRuleConf(map)

		rule.init(lconf)
		assert(lconf.get("labelrule.StayTime")=="20")

		stayLabelMap.put("1","true")
		mc1.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)

		stayLabelMap.put("1","true")
		mc2.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc2,cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		stayLabelMap.put("1","true")
		mc3.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc3,cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==1)
		assert(mc3.getLabel(Constant.STAY_TIME).get("1")=="20")

	}

	test("3 测试某用户从区域A到区域B再到区域C"){
		val stayLabelMap=mutable.Map[String,String]()

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 222, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 333, 1, 13900000001L ,13910000001L)

		val cache = new StreamingCache
		val rule=new LocationStayRule()
		val lconf=new LabelRuleConf(map)

		rule.init(lconf)
		assert(lconf.get("labelrule.StayTime")=="20")

		stayLabelMap.put("1","true")
		mc1.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)


		stayLabelMap.put("2","true")
		mc2.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		stayLabelMap.put("3","true")
		mc3.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc3,cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==0)

	}

	test("4 测试某用户从区域A到区域A/B再到区域B/C"){
		val stayLabelMap=mutable.Map[String,String]()

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 122, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 122, 1, 13900000001L ,13910000001L)
		val mc4=MCSourceObject(1, sdf.parse("20150401 08:37:00.000").getTime, 223, 1, 13900000001L ,13910000001L)
		val mc5=MCSourceObject(1, sdf.parse("20150401 08:57:00.000").getTime, 223, 1, 13900000001L ,13910000001L)

		val cache = new StreamingCache
		val rule=new LocationStayRule()
		val lconf=new LabelRuleConf(map)

		rule.init(lconf)
		assert(lconf.get("labelrule.StayTime")=="20")

		stayLabelMap.put("1","true")
		mc1.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)

		//		stayLabelMap.put("1","true")
		stayLabelMap.put("2","true")
		mc2.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		//		stayLabelMap.put("1","true")
		//		stayLabelMap.put("2","true")
		mc3.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc3,cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==1)
		assert(mc3.getLabel(Constant.STAY_TIME).get("1")=="20")

		stayLabelMap.remove("1")
		//		stayLabelMap.put("2","true")
		stayLabelMap.put("3","true")
		mc4.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc4,cache)
		assert(mc4.getLabel(Constant.STAY_TIME).size==1)
		assert(mc4.getLabel(Constant.STAY_TIME).get("2")=="20")

		stayLabelMap.put("2","true")
		stayLabelMap.put("3","true")
		mc5.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc5,cache)
		assert(mc5.getLabel(Constant.STAY_TIME).size==2)
		assert(mc5.getLabel(Constant.STAY_TIME).get("2")=="20")
		assert(mc5.getLabel(Constant.STAY_TIME).get("3")=="20")
	}

	test("5 测试用户在一个区域mc短时间乱序的问题"){
		val stayLabelMap=mutable.Map[String,String]()

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc4=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 111, 1, 13900000001L ,13910000001L)

		val cache = new StreamingCache
		val rule=new LocationStayRule()
		val lconf=new LabelRuleConf(map)

		rule.init(lconf)
		assert(lconf.get("labelrule.StayTime")=="20")

		stayLabelMap.put("1","true")
		mc1.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)


		//		stayLabelMap.put("1","true")
		mc2.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		//		stayLabelMap.put("1","true")
		mc3.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==0)

		//		stayLabelMap.put("1","true")
		mc4.setLabel(Constant.STAY_TIME, stayLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.STAY_TIME).size==1)
		assert(mc4.getLabel(Constant.STAY_TIME).get("1")=="20")

	}

	test("6 测试用户在多个区域mc短时间乱序的问题"){
		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		mc1.getLabel(Constant.STAY_TIME).put("1","true");

		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 121, 1, 13900000001L ,13900000001L)
		mc2.getLabel(Constant.STAY_TIME).put("1","true");
		mc2.getLabel(Constant.STAY_TIME).put("2","true");

		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:23:00.000").getTime, 123, 1, 13900000001L ,13900000001L)
		mc3.getLabel(Constant.STAY_TIME).put("1","true");
		mc3.getLabel(Constant.STAY_TIME).put("2","true");
		mc3.getLabel(Constant.STAY_TIME).put("3","true");

	}

	test("7 测试用户在某区域mc长时间乱序的问题"){
		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:03:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:20:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:23:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

	}

	test("8 测试用户在多个区域mc长时间乱序的问题"){
		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 121, 1, 13900000001L ,13900000001L)

		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:07:00.000").getTime, 223, 1, 13900000001L ,13900000001L)

		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 121, 1, 13900000001L ,13900000001L)

	}

}
