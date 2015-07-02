package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */

import com.asiainfo.ocdc.streaming.constant.TableNameConstants
import com.asiainfo.ocdc.streaming.eventrule.EventRuleConf
import com.asiainfo.ocdc.streaming.eventsource.EventSourceConf
import com.asiainfo.ocdc.streaming.labelrule.LabelRuleConf
import com.asiainfo.ocdc.streaming.subscribe.BusinessEventConf
import com.asiainfo.ocdc.streaming.tool.JDBCUtils
import scala.collection.mutable.Map

object MainFrameConf extends BaseConf {

  var updateTime: Long = 0L

  var sources: Array[EventSourceConf] = null
  var sourceLabelRules = Map[String, Seq[LabelRuleConf]]()
  var sourceEventRules = Map[String, Seq[EventRuleConf]]()
  var businessEvents = Map[String, Seq[BusinessEventConf]]()

  val bsevent2eventrules = Map[String, Seq[String]]()
  val bsevent2eventsources = Map[String, Seq[String]]()

  def getEventRulesBySource(value: String) = sourceEventRules.getOrElse(value, Seq())

  def getLabelRulesBySource(value: String) = sourceLabelRules.getOrElse(value, Seq())

  def getBsEventsBySource(value: String) = businessEvents.getOrElse(value, Seq())

  def getEventRulesByBsEvent(value: String) = bsevent2eventrules.getOrElse(value, Seq())

  def getEventSourcesByBsEvent(value: String) = bsevent2eventsources.getOrElse(value, Seq())

  def getInternal: Long = getLong("internal", 1)

  def init(): Unit = {
    initMainFrameConf

    initEventSourceConf

    initLabelRuleConf

    initEventRuleConf

    initBusinessEventConf

    initBsEvent2EventRules
  }

  def update(updateTime: Long){
    init()
    this.updateTime = updateTime
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
          erconf.set("id", erid)
          erconf.set("classname", classname)
          midmap2.get(esid).get += (erid -> erconf)
        }
      } else {
        val erconf = new EventRuleConf()
        erconf.set(name, pvalue)
        erconf.set("id", erid)
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

  /**
   * read business event list and config
   */
  def initBusinessEventConf {
    val sql = "select bep.name,bep.pvalue,be.classname,be.id as beid,es.esid as esid from BusenessEventsProp bep join BusenessEvents be on bep.beid=be.id join BusenessEventsMapEventSources es on be.id = es.beid"
    val busievents = JDBCUtils.query(sql)
    val midmap2 = Map[String, Map[String, BusinessEventConf]]()
    busievents.foreach(x => {
      val esid = x.get("esid").get
      val beid = x.get("beid").get
      val name = x.get("name").get
      val pvalue = x.get("pvalue").get
      val classname = x.get("classname").get
      if (midmap2.contains(esid)) {
        if (midmap2.get(esid).get.contains(beid)) {
          midmap2.get(esid).get.get(beid).get.set(name, pvalue)
        } else {
          val beconf = new BusinessEventConf()
          beconf.set(name, pvalue)
          beconf.set("beid", beid)
          beconf.set("classname", classname)
          midmap2.get(esid).get += (beid -> beconf)
        }
      } else {
        val beconf = new BusinessEventConf()
        beconf.set(name, pvalue)
        beconf.set("beid", beid)
        beconf.set("classname", classname)
        midmap2 += (esid -> Map(beid -> beconf))
      }
    })
    businessEvents = midmap2.map(x => {
      x._1 -> x._2.map(y => {
        y._2
      }).toSeq
    })
  }


  /**
   * read business event map to event rules
   */
  def initBsEvent2EventRules {
    val sql = "select be.id as beid,er.id as erid from BusenessEvents be join BusenessEventsMapEventRules map join EventRules er on be.id = map.beid and er.id=map.erid "
    val be2er = JDBCUtils.query(sql)

    be2er.foreach(x => {
      val beid = x.get("beid").get
      val erid = x.get("erid").get

      if (bsevent2eventrules.contains(beid)) {
        val newseq = bsevent2eventrules.get(beid).get ++ (erid)
        bsevent2eventrules.update(beid, newseq.asInstanceOf[Seq[String]])
      } else {
        val ers = Seq(erid)
        bsevent2eventrules += (beid -> ers)
      }
    })
  }

  /**
   * read business event map to event sources
   */
  def initBsEvent2EventSources {
    val sql = "select be.id as beid,es.id as esid from BusenessEvents be join BusenessEventsMapEventSources map join EventSources es on be.id = map.beid and es.id=map.esid "
    val be2es = JDBCUtils.query(sql)

    be2es.foreach(x => {
      val beid = x.get("beid").get
      val esid = x.get("esid").get

      if (bsevent2eventsources.contains(beid)) {
        val newseq = bsevent2eventsources.get(beid).get ++ (esid)
        bsevent2eventsources.update(beid, newseq.asInstanceOf[Seq[String]])
      } else {
        val ess = Seq(esid)
        bsevent2eventsources += (beid -> ess)
      }
    })
  }


}
