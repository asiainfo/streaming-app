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
	map + ("labelrule.classname"->"com.asiainfo.ocdc.streaming.LocationStayRule")
	map + ("labelrule.StayTime"->"20")

	test("1 test SiteRule"){
		//    val stayLabelMap=immutable.Map[String,String]()	//报错
		val stayLabelMap=mutable.Map[String,String]()
		val mc1 = MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 111, 1, 13900000001L ,13900000001L)
		val mc2=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 112, 1, 13900000002L ,13910000002L)
		val mc3=MCSourceObject(1, sdf.parse("20150401 08:00:00.000").getTime, 123, 1, 13900000003L ,13910000003L)

		val cache = new StreamingCache
		val rule=new SiteRule()
		val lconf=new LabelRuleConf(map)


		rule.init(lconf)
		assert(lconf.get("labelrule.StayTime")=="20")


		rule.attachMCLabel(mc1, cache)
		assert(mc1.getLabel(Constant.LABEL_ONSITE).size==1)
		assert(mc1.getLabel(Constant.LABEL_ONSITE).get("1")=="true")

		rule.attachMCLabel(mc2, cache)
		assert(mc2.getLabel(Constant.LABEL_ONSITE).size==2)
		assert(mc2.getLabel(Constant.LABEL_ONSITE).get("1")=="true")
		assert(mc2.getLabel(Constant.LABEL_ONSITE).get("2")=="true")

		rule.attachMCLabel(mc3, cache)
		assert(mc3.getLabel(Constant.LABEL_ONSITE).size==3)
		assert(mc3.getLabel(Constant.LABEL_ONSITE).get("1")=="true")
		assert(mc3.getLabel(Constant.LABEL_ONSITE).get("2")=="true")
		assert(mc3.getLabel(Constant.LABEL_ONSITE).get("3")=="true")

	}
}
