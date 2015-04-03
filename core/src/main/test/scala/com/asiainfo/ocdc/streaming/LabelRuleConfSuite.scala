package com.asiainfo.ocdc.streaming

import org.scalatest.{BeforeAndAfter, FunSuite}

/**
 * Created by yfq on 15/4/3.
 */
class LabelRuleConfSuite extends FunSuite with BeforeAndAfter {

	test("test read configuration"){
		val map=Map("classname"->"com.asiainfo.ocdc.streaming.LocationStayRule",
			"labelrule.StayTime"->"10"
		)

		val lconf=new LabelRuleConf(map)
		assert(lconf.getClassName=="com.asiainfo.ocdc.streaming.LocationStayRule")
		assert(lconf.get("labelrule.StayTime")=="10")
	}
}
