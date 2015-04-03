package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */

import com.asiainfo.ocdc.streaming.constant.TableNameConstants

import scala.collection.mutable.Map

object MainFrameConf extends BaseConf {

  def getEventRulesBySource(value: String) = sourceEventRules.get(value).get

  def getLabelRulesBySource(value: String) = sourceLabelRules.get(value).get

  def getInternal: Long = getLong("internal", 1)

  var sourceLabelRules = Map[String, Seq[LabelRuleConf]]()
  var sourceEventRules = Map[String, Seq[EventRuleConf]]()

  var sources: Array[EventSourceConf] = null


  def init(): Unit = {
    // read main frame config
    var sql = "select name,pvalue from " + TableNameConstants.MainFramePropTableName
    val mainframedata = JDBCUtils.query(sql)
    mainframedata.foreach(x => {
      set(x.get("name").get, x.get("pvalue").get)
    })

    // read event source list and config
    sql = "select id,type,sourceid,delim,formatlength,classname from " + TableNameConstants.EventSourceTableName
    val events = JDBCUtils.query(sql)
    sources = events.map(x => {
      new EventSourceConf(x)
    })

    // read label rule list and config
    sql = "select lrp.name,lrp.pvalue,lr.classname,lr.id as lrid,es.id as esid from LabelRulesProp lrp join LabelRules lr on lrp.lrid = lr.id join EventSource es on lr.esourceid = es.id"
    val labrules = JDBCUtils.query(sql)
    val midmap = Map[String, Map[String, LabelRuleConf]]()
    labrules.foreach(x => {
      val esid = x.get("esid").get
      val lrid = x.get("lrid").get
      val name = x.get("name").get
      val pvalue = x.get("pvalue").get
      val classname = x.get("classname").get
      if (midmap.contains(esid)) {
        if (midmap.get(esid).get.contains(lrid)) {
          midmap.get(esid).get.get(lrid).get.set(name, pvalue)
        } else {
          val lrconf = new LabelRuleConf()
          lrconf.set(name, pvalue)
          lrconf.set("labelrule.classname", classname)
          midmap.get(esid).get += (lrid -> lrconf)
        }
      } else {
        val lrconf = new LabelRuleConf()
        lrconf.set(name, pvalue)
        lrconf.set("labelrule.classname", classname)
        midmap += (esid -> Map(lrid -> lrconf))
      }
    })

    sourceLabelRules = midmap.map(x => {
      x._1 -> x._2.map(y => {
        y._2
      }).toSeq
    })

    // read event rule list and config
    sql = "select erp.name,erp.pvalue,er.classname,er.id as erid,es.id as esid from EventRulesProp erp join EventRules er on erp.erid = er.id join EventSource es on er.esourceid = es.id"
    val eventrules = JDBCUtils.query(sql)
    val midmap2 = Map[String, Map[String, EventRuleConf]]()
    eventrules.foreach(x => {
      val esid = x.get("esid").get
      val erid = x.get("erid").get
      val name = x.get("name").get
      val pvalue = x.get("pvalue").get
      val classname = x.get("classname").get
      if (midmap2.contains(esid)) {
        if (midmap2.get(esid).get.contains(erid)) {
          midmap2.get(esid).get.get(erid).get.set(name, pvalue)
        } else {
          val erconf = new EventRuleConf()
          erconf.set(name, pvalue)
          erconf.set("eventrule.classname", classname)
          midmap2.get(esid).get += (erid -> erconf)
        }
      } else {
        val erconf = new EventRuleConf()
        erconf.set(name, pvalue)
        erconf.set("eventrule.classname", classname)
        midmap2 += (esid -> Map(erid -> erconf))
      }
    })
    sourceEventRules = midmap2.map(x => {
      x._1 -> x._2.map(y => {
        y._2
      }).toSeq
    })

  }
}
