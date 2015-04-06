package com.asiainfo.ocdc.streaming

import java.text.SimpleDateFormat
import com.asiainfo.ocdc.save.{LabelProps, MCStatus}
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
	map + ("classname"->"com.asiainfo.ocdc.streaming.LocationStayRule")
	map + ("labelrule.StayTime"->"20")

//  val areaLabelMap=immutable.Map[String,String]()	//报错
	val areaLabelMap=mutable.Map[String,String]()
	var cache:StreamingCache = _
	var rule:LocationStayRule = _
	var lrConf:LabelRuleConf = _
	before{
		cache = new LabelProps
		cache
		rule = new LocationStayRule()
		lrConf = new LabelRuleConf(map)
		lrConf.setAll(map)
		lrConf.set("classname","com.asiainfo.ocdc.streaming.LocationStayRule")
		lrConf.set("labelrule.StayTime", "20")
		rule.init(lrConf)
	}


	test("1 测试新用户首次MC的处理(cache没有缓存)") {

		
		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 112, 1, 13900000002L ,13910000002L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 123, 1, 13900000003L ,13910000003L)
		
		assert(lrConf.get("classname")=="com.asiainfo.ocdc.streaming.LocationStayRule")
		assert(lrConf.get("labelrule.StayTime")=="20")


		areaLabelMap.put("1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==1)
		assert(mc1.getLabel(Constant.STAY_TIME).get("1")==0)

		//		areaLabelMap.put("1","true")
		areaLabelMap.put("2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		//		areaLabelMap.put("2","true")
		areaLabelMap.put("3","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==0)
	}

	test("2 测试已有用户的MC的处理(cache中有缓存信息)"){

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 111, 1, 13900000001L ,13910000001L)

		assert(lrConf.get("labelrule.StayTime")=="20")

		areaLabelMap.put("1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)

		areaLabelMap.put("1","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2,cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		areaLabelMap.put("1","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3,cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==1)
		assert(mc3.getLabel(Constant.STAY_TIME).get("1")=="20")

	}

	test("3 测试某用户从区域A到区域B再到区域C，在回到A"){

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 222, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 333, 1, 13900000001L ,13910000001L)
		val mc4=MCSourceObject(1, sdf.parse("20150401 08:37:00.000").getTime, 111, 1, 13900000001L ,13910000001L)

		assert(lrConf.get("labelrule.StayTime")=="20")

		areaLabelMap.put("1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)


		areaLabelMap.remove("1")
		areaLabelMap.put("2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		areaLabelMap.remove("2")
		areaLabelMap.put("3","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3,cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==0)

		areaLabelMap.remove("3")
		areaLabelMap.put("1","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4,cache)
		assert(mc4.getLabel(Constant.STAY_TIME).size==0)

	}

	test("4 测试某用户从区域A到区域A/B再到区域B/C"){

		val mc1=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 122, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 122, 1, 13900000001L ,13910000001L)
		val mc4=MCSourceObject(1, sdf.parse("20150401 08:37:00.000").getTime, 223, 1, 13900000001L ,13910000001L)
		val mc5=MCSourceObject(1, sdf.parse("20150401 08:57:00.000").getTime, 223, 1, 13900000001L ,13910000001L)

		assert(lrConf.get("labelrule.StayTime")=="20")

		areaLabelMap.put("1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		areaLabelMap.put("2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		//		areaLabelMap.put("2","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3,cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==1)
		assert(mc3.getLabel(Constant.STAY_TIME).get("1")=="20")

		areaLabelMap.remove("1")
		//		areaLabelMap.put("2","true")
		areaLabelMap.put("3","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4,cache)
		assert(mc4.getLabel(Constant.STAY_TIME).size==1)
		assert(mc4.getLabel(Constant.STAY_TIME).get("2")=="20")

		//		areaLabelMap.put("2","true")
		//		areaLabelMap.put("3","true")
		mc5.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc5,cache)
		assert(mc5.getLabel(Constant.STAY_TIME).size==2)
		assert(mc5.getLabel(Constant.STAY_TIME).get("2")=="20")
		assert(mc5.getLabel(Constant.STAY_TIME).get("3")=="20")
	}

	test("5 测试用户在一个区域mc短时间乱序的问题"){

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 111, 1, 13900000001L ,13910000001L)
		val mc4=MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 111, 1, 13900000001L ,13910000001L)

		assert(lrConf.get("labelrule.StayTime")=="20")

		areaLabelMap.put("1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)


		//		areaLabelMap.put("1","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.STAY_TIME).size==1)
		assert(mc4.getLabel(Constant.STAY_TIME).get("1")=="20")

	}

	test("6 测试用户在多个区域mc短时间乱序的问题"){

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:10:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 121, 1, 13900000001L ,13900000001L)

		val mc3 = MCSourceObject(1, sdf.parse("20150401 08:17:00.000").getTime, 123, 1, 13900000001L ,13900000001L)
		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 123, 1, 13900000001L ,13900000001L)
		val mc5 = MCSourceObject(1, sdf.parse("20150401 08:27:00.000").getTime, 123, 1, 13900000001L ,13900000001L)

		assert(lrConf.get("labelrule.StayTime")=="20")

		areaLabelMap.put("1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		areaLabelMap.put("2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		areaLabelMap.put("2","true")
		areaLabelMap.put("3","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		//		areaLabelMap.put("2","true")
		//		areaLabelMap.put("3","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.STAY_TIME).size==1)
		assert(mc4.getLabel(Constant.STAY_TIME).get("1")=="true")

		//		areaLabelMap.put("1","true")
		//		areaLabelMap.put("2","true")
		//		areaLabelMap.put("3","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.STAY_TIME).size==2)
		assert(mc4.getLabel(Constant.STAY_TIME).get("1")=="true")
		assert(mc4.getLabel(Constant.STAY_TIME).get("2")=="true")

	}

	test("7 测试用户在某区域mc长时间乱序的问题"){

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:03:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:20:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:23:00.000").getTime, 111, 1, 13900000001L ,13900000001L)

		assert(lrConf.get("labelrule.StayTime")=="20")

		areaLabelMap.put("1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==0)

		//		areaLabelMap.put("1","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==0)
		//		assert(mc3.getLabel(Constant.STAY_TIME).get("1")=="true")

		//		areaLabelMap.put("1","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.STAY_TIME).size==1)
		assert(mc4.getLabel(Constant.STAY_TIME).get("1")=="true")

	}

	test("8 测试用户在多个区域mc长时间乱序的问题"){

		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2 = MCSourceObject(1, sdf.parse("20150401 08:20:00.000").getTime, 121, 1, 13900000001L ,13900000001L)

		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:20:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		//		val mc3 = MCSourceObject(1, sdf.parse("20150401 07:20:00.000").getTime, 121, 1, 13900000001L ,13900000001L)

		val mc4 = MCSourceObject(1, sdf.parse("20150401 08:23:00.000").getTime, 123, 1, 13900000001L ,13900000001L)

		assert(lrConf.get("labelrule.StayTime")=="20")

		areaLabelMap.put("1","true")
		mc1.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.STAY_TIME).size==0)

		areaLabelMap.put("1","true")
		areaLabelMap.put("2","true")
		mc2.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.STAY_TIME).size==1)
		assert(mc2.getLabel(Constant.STAY_TIME).get("1")=="true")

		areaLabelMap.remove("2")
		areaLabelMap.put("1","true")
		mc3.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.STAY_TIME).size==0)

		areaLabelMap.put("1","true")
		areaLabelMap.put("2","true")
		areaLabelMap.put("3","true")
		mc4.setLabel(Constant.LABEL_ONSITE, areaLabelMap)
		rule.attachMCLabel(mc4, cache)
		assert(mc4.getLabel(Constant.STAY_TIME).size==2)
		assert(mc4.getLabel(Constant.STAY_TIME).get("1")=="true")
		assert(mc4.getLabel(Constant.STAY_TIME).get("2")=="true")

	}

}
