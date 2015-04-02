package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.save.MCStatus
import scala.collection.mutable.Map
import com.asiainfo.ocdc.save.LabelProps
import scala.collection.mutable.ArrayBuffer

/**
 * @author surq
 * @since 2015.4.2
 * @comment 给mc信令标记连续停留时间标签
 */
class LocationStayRule extends MCLabelRule {
  // TODO 配置文件读入的，
  val selfDefStayTimeList = Array(10 * 60 * 1000, 5 * 60 * 1000, 3 * 60 * 1000).sorted
  val thresholdValue = 20 * 60 * 1000
  def attachMCLabel(mcSourceObj: MCSourceObject, cache: StreamingCache) {
    val cacheInstance = cache.asInstanceOf[MCStatus]

    // 取在siteRule（区域规则）中所打的标签对像
    val MCLabelonsite = (mcSourceObj.getLabel(Constant.LABEL_ONSITE)).asInstanceOf[LabelProps]

    // 获取基于区域标签之上的处理预订obj
    val MCPropList = MCLabelonsite.getLabelsPropList

    val mcCache = cacheInstance.getUpdateStatus
    // 数据库中无此用户记录
    if (mcCache == null) {
      //新处理的手机号（无记录数据） [遍历所有的区域，并对其做停留时间处理]
      // TODO 改变了mcsource Labels的内容，此处打上标签了!!!!
      val updatePropList = MCPropList.map(prop => {
        val propMap = prop._2
        propMap += (Constant.LABEL_STAY_FIRSTTIME -> (mcSourceObj.time).toString)
        propMap += (Constant.LABEL_STAY_LASTTIME -> (mcSourceObj.time).toString)
        (prop._1, propMap)
      })
      MCLabelonsite.setLabelsPropList(updatePropList)
      // 更新k/v库数据内容
      cacheInstance.setUpdateStatus((Constant.LABEL_ONSITE, MCLabelonsite))
    } else {
      val thistime = mcSourceObj.time.toLong
      val event = mcSourceObj.eventID
      // 本记录的area标签集合
      val locationList = for (local <- MCPropList) yield local._1

      val baseLabel = mcCache._1
      val baseLabelProp = mcCache._2
      val areaListPropList = baseLabelProp.getLabelsPropList
      // 清除cache中过期或无效的数据
      // 1、根据本条记录的time与所有非本区域标签的数据的lastTime比兑若大于，则再与FirstTime做差值若小于最小的阈值则视为无效值
      // 2、通过event标签确定“切出”时间，清除本区域的数据
      // 3、设定阈值判断所有区域的lastTime加上阈值时间大于本记录time的视为过期数据清除
      // 4、lastTime最大的区域的lastTime与非此区域的firstTime做差值若小于最小的阈值则视为无效值
      // 数据更新
      // 1、根据本条记录的time与所有非本区域标签的数据的持续时间区间比兑若在其中则视为该区域数据的firstTime无效，
      // 把大于thisTime小于lastTime所有有效数据中最小的lastTime设为firstTime(此做法不严谨)
      // 2、正常情况的更新处理（比兑firstTime ,lastTime，设值）
      //创建临时cache区
      val tmpMap = Map[String, Map[String, String]]()
      for ((k, v) <- areaListPropList) tmpMap += (k -> v)

      val rmIterator = tmpMap.iterator
      // 记当有效区域数据的开始时间
      val firstTimeList = ArrayBuffer[Long]()
      val lastTimeList = ArrayBuffer[(String, Long)]()
      def delData(areaName: String) = {
        tmpMap.remove(areaName)
        firstTimeList.remove(0)
        lastTimeList.remove(0)
      }

      while (rmIterator.hasNext) {
        val locationNode = rmIterator.next
        val areaName = locationNode._1
        val areaProp = locationNode._2
        val firestTime = areaProp(Constant.LABEL_STAY_FIRSTTIME).toLong
        val lastTime = areaProp(Constant.LABEL_STAY_LASTTIME).toLong

        firstTimeList += firestTime
        lastTimeList += (areaName -> lastTime)

        if (!locationList.contains(areaName)) {
          // del:1、根据本条记录的time与所有非本区域标签的数据的lastTime比较若大于，则再与FirstTime做差值若小于最小的阈值则视为无效值
          if (thistime >= lastTime && (thistime - firestTime) < selfDefStayTimeList(0))
            delData(areaName)
        } // del:2、通过event标签确定“切出”时间，并且时间段小于业务所需最小时间，清除本区域的数据(8:切入;9:切出)
        else if (event == 9 && (thistime - firestTime) < selfDefStayTimeList(0))
          delData(areaName)
        // del:3、设定阈值判断所有区域的lastTime加上阈值时间大于本记录time的视为过期数据清除
        if ((thresholdValue + lastTime) > thistime) delData(areaName)
      }

      // 4、lastTime最大的区域的lastTime与非此区域的firstTime做差值若小于最小的阈值则视为无效值
      val rmit = tmpMap.iterator
      val maxLastTime = (lastTimeList.sortBy(_._2).reverse)(0)
      // 最大listTime的区域
      val masxtLTimeArea = maxLastTime._1
      val masxtLTime = maxLastTime._2
      // 刨除最大listTime的区域数据的区域list
      //      val areaList = lastTimeList.filter(area => area._1 != masxtLTimeArea).map(_._1)

      while (rmit.hasNext) {
        val locationNode = rmit.next
        val areaName = locationNode._1
        val areaProp = locationNode._2
        val firestTime = areaProp(Constant.LABEL_STAY_FIRSTTIME).toLong
        if (areaName != masxtLTimeArea && (masxtLTime - firestTime) < selfDefStayTimeList(0))
          delData(areaName)
      }

      // 更新cache操作
      val upIterator = tmpMap.iterator
      while (upIterator.hasNext) {
        val locationNode = upIterator.next
        val areaName = locationNode._1
        val areaProp = locationNode._2
        val firestTime = areaProp(Constant.LABEL_STAY_FIRSTTIME).toLong
        val lastTime = areaProp(Constant.LABEL_STAY_LASTTIME).toLong
        if (!locationList.contains(areaName)) {
          // update:1、根据本条记录的time与所有非本区域标签的数据的持续时间区间比兑
          //若在其间则视为该区域数据的firstTime无效，把大于thisTime中最小的lastTime设为firstTime
          if (thistime >= firestTime && firestTime <= lastTime) {
            // 把lastTime放入，在没有满足条件的时候，就会设此值（目的为了保证estimateList.size>0）
            firstTimeList += lastTime.toLong
            val estimateList = firstTimeList
              .filter(time => (time >= thistime && thistime <= lastTime)).sorted
            areaProp += (Constant.LABEL_STAY_FIRSTTIME -> estimateList(0).toString)
          }
        } else {
          // 正常情况的更新处理
          var newfirstTime = firestTime
          var newlastTime = lastTime
          if (thistime < firestTime) newfirstTime = thistime
          else if (thistime > lastTime) newlastTime = lastTime
          areaProp += (Constant.LABEL_STAY_FIRSTTIME -> newfirstTime.toString)
          areaProp += (Constant.LABEL_STAY_LASTTIME -> newlastTime.toString)
        }

        // 打标签
        // 计算停留时间
        val stayTime = areaProp(Constant.LABEL_STAY_LASTTIME).toLong -
          areaProp(Constant.LABEL_STAY_FIRSTTIME).toLong

          val st = for ((areaName,v)<-MCPropList)yield (v)
          val propMap = st(0)
          propMap += (Constant.STAY_TIME -> stayTime.toString)
          
          // TODO 如何只在达到阈值时推送？
          //        selfDefStayTimeList.map(defstaytime => {
//          if (stayTime >= defstaytime) {
//          }
//        })
      }
      // 重置cache
      baseLabelProp.setLabelsPropList(tmpMap.toList)
    }
  }
}