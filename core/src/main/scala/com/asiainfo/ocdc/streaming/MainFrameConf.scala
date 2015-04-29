package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */

import com.asiainfo.ocdc.streaming.constant.TableNameConstants

import scala.collection.mutable.Map

object MainFrameConf extends BaseConf {

  var sources: Array[EventSourceConf] = null
  var sourceLabelRules = Map[String, Seq[LabelRuleConf]]()
  var sourceEventRules = Map[String, Seq[EventRuleConf]]()

  def getEventRulesBySource(value: String) = sourceEventRules.getOrElse(value, Seq())

  def getLabelRulesBySource(value: String) = sourceLabelRules.getOrElse(value, Seq())

  def getInternal: Long = getLong("internal", 1)

  def init(): Unit = {
    initMainFrameConf

    initEventSourceConf

    initLabelRuleConf

    initEventRuleConf
  }

  init()

  /**
   * read main frame config
   */
  def initMainFrameConf {
    val sql = "select name,pvalue from " + TableNameConstants.MainFramePropTableName
    val mainframedata = JDBCUtils.query(sql)
    mainframedata.foreach(x => {
      set(x.get("name").get, x.get("pvalue").get)
    })
  }

  /**
   * read event source list and config
   */
  def initEventSourceConf {

    val sql = "select es.id,es.name as sourcename,es.type,es.delim,es.formatlength,es.classname,es.batchsize,es.enabled,esp.name as pname,esp.pvalue from " + TableNameConstants.EventSourceTableName + " es left join " + TableNameConstants.EventSourcePropTableName + " esp on es.id = esp.esourceid where es.enabled=1 "
    val events = JDBCUtils.query(sql)
    val sourcemap = Map[String, EventSourceConf]()
    events.map(x => {
      val sourceId = x.get("id").get
      if (sourcemap.contains(sourceId)) {
        if (x.get("pname").get != null) sourcemap.get(sourceId).get.set(x.get("pname").get, x.get("pvalue").get)
      } else {
        val esconf = new EventSourceConf()
        esconf.set("id", x.get("id").get)
        esconf.set("sourcename", x.get("sourcename").get)
        esconf.set("type", x.get("type").get)
        esconf.set("delim", x.get("delim").get)
        esconf.set("formatlength", x.get("formatlength").get)
        esconf.set("classname", x.get("classname").get)
        esconf.set("batchsize", x.get("batchsize").get)
        esconf.set("enabled", x.get("enabled").get)
        if (x.get("pname").get != null) esconf.set(x.get("pname").get, x.get("pvalue").get)
        sourcemap += (sourceId -> esconf)
      }
    })

    sources = sourcemap.map(_._2).toArray
  }

  /**
   * read label rule list and config
   */
  def initLabelRuleConf {
    val sql = "select lrp.name,lrp.pvalue,lr.classname,lr.id as lrid,es.id as esid from LabelRulesProp lrp right join LabelRules lr on lrp.lrid = lr.id join EventSource es on lr.esourceid = es.id where es.enabled=1 "
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
          if (name != null) midmap.get(esid).get.get(lrid).get.set(name, pvalue)
        } else {
          val lrconf = new LabelRuleConf()
          lrconf.set("id", lrid)
          lrconf.set("classname", classname)
          if (name != null) lrconf.set(name, pvalue)
          midmap.get(esid).get += (lrid -> lrconf)
        }
      } else {
        val lrconf = new LabelRuleConf()
        lrconf.set("id", lrid)
        lrconf.set("classname", classname)
        if (name != null) lrconf.set(name, pvalue)
        midmap += (esid -> Map(lrid -> lrconf))
      }
    })

    sourceLabelRules = midmap.map(x => {
      x._1 -> x._2.toList.sortBy(_._1).map(y => {
        y._2
      })
    })
  }

  /**
   * read event rule list and config
   */
  def initEventRuleConf {
    val sql = "select erp.name,erp.pvalue,er.classname,er.id as erid,es.id as esid from EventRulesProp erp right join EventRules er on erp.erid = er.id join EventSource es on er.esourceid = es.id where es.enabled=1 "
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
          erconf.set("classname", classname)
          midmap2.get(esid).get += (erid -> erconf)
        }
      } else {
        val erconf = new EventRuleConf()
        erconf.set(name, pvalue)
        erconf.set("classname", classname)
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
