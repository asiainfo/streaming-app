package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.constant.LabelConstant
import com.asiainfo.ocdc.streaming.eventrule.StreamingCache

import scala.collection.mutable.Map
import scala.util.Sorting.quickSort

/**
 * @author surq
 * @since 2015.4.2
 * @comment 给mc信令标记连续停留时间标签
 */

class LabelProps extends StreamingCache with Serializable {
  import scala.collection.immutable.Map
  var labelsPropList = Map[String, Map[String, String]]()
}

class LocationStayRule extends MCLabelRule {
  // 从配置文件读入 用户设定的各业务连续停留的时间坎(排序升序)
  lazy val selfDefStayTimeList = conf.get(LabelConstant.STAY_LIMITS).split(LabelConstant.ITME_SPLIT_MARK).map(_.trim)
  lazy val stayTimeOrderList = selfDefStayTimeList.map(_.toLong).sorted
  // 推送满足设置的数据坎的最大值:true;最小值：false
  lazy val userDefPushOrde = conf.getBoolean(LabelConstant.STAY_MATCHMAX, true)
  // 推送满足设置的数据的限定值，还是真实的累计值.真实的累计值:false;限定值:true
  lazy val pushLimitValue = conf.getBoolean(LabelConstant.STAY_OUTPUTTHRESHOLD, true)
  // 无效数据阈值的设定
  lazy val thresholdValue = conf.getLong(LabelConstant.STAY_TIMEOUT, LabelConstant.DEFAULT_TIMEOUT_VALUE)

  /**
   * 根据本次以及前次的停留时间计算出标签停留时间的值
   */
  private def evaluateTime(oldStayTime: Long, newStayTime: Long): String = {
    // 新状态未达到最小坎时或旧状态超过最大坎值时返回黙认值“0”
    if (newStayTime < stayTimeOrderList(0) ||
      oldStayTime > stayTimeOrderList(stayTimeOrderList.size - 1)) {
      LabelConstant.LABEL_STAY_DEFAULT_TIME
    } else {
      val matchList = stayTimeOrderList.filter(limit => (oldStayTime <= limit && newStayTime >= limit))
      // 新旧停留时间在某坎区间内，返回黙认值“0”
      if (matchList.isEmpty) LabelConstant.LABEL_STAY_DEFAULT_TIME
      // 新旧停留时间为跨坎区域时间，推送设置的数据坎的值
      else if (pushLimitValue) {
        val result = if (userDefPushOrde) matchList.map(_.toLong).max else matchList.map(_.toLong).min
        result.toString
      } // 推送真实数据值
      else newStayTime.toString
    }
  }

  /**
   * 框架调用入口方法
   */
  def attachMCLabel(mc: MCSourceObject, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache = {
    //a. 获取locationStayRule的cache对象
    val cacheInstance = if (cache == null) new LabelProps
    else cache.asInstanceOf[LabelProps]
    cache.asInstanceOf[LabelProps]
    // cache中各区域的属性map
    val sourceLabelsProp = cacheInstance.labelsPropList
    // map属性转换
    val labelsPropMap = getCacheInfo(sourceLabelsProp)
    // mcsource 打标签用
    val mcStayLabelsMap = Map[String, String]()
    // cache中所有区域的最大lastTime
    val cacheMaxLastTime = getCacheMaxLastTime(labelsPropMap)
    // 取在siteRule（区域规则）中所打的area标签list
    val locationList = (mc.getLabels(LabelConstant.LABEL_ONSITE)).keys
    locationList.map(location => {
      // A.此用的所有区域在cache中的信息已经过期视为无效，标签打为“0”；重新设定cache;
      if (mc.time - cacheMaxLastTime > thresholdValue) {
        // 1. 连续停留标签置“0”
        mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_TIME_ZERO)
        // 2. 清除cache信息
        labelsPropMap.clear
        // 3. 重置cache信息
        val cacheStayLabelsMap = Map[String, String]()
        cacheStayLabelsMap += (LabelConstant.LABEL_STAY_FIRSTTIME -> mc.time.toString)
        cacheStayLabelsMap += (LabelConstant.LABEL_STAY_LASTTIME -> mc.time.toString)
        labelsPropMap += (location -> cacheStayLabelsMap)
      } else if (cacheMaxLastTime - mc.time > thresholdValue) {
        //B.此条数据为延迟到达的数据，已超过阈值视为无效，标签打为“0”；cache不做设定;
        // 1. 连续停留标签置“0”
        mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_TIME_ZERO)
      } else {
        //C.此条数据时间为[(maxLastTime-thresholdValue)~maxLastTime~(maxLastTime+thresholdValue)]之间
        labelAction(location, labelsPropMap, mcStayLabelsMap, mc.time)
      }
    })
    // c. 给mcsoruce设定连续停留[LABEL_STAY]标签
    mc.setLabel(LabelConstant.LABEL_STAY, mcStayLabelsMap)

    // map属性转换
    cacheInstance.labelsPropList = setCacheInfo(labelsPropMap)
    cacheInstance
  }

  /**
   * 把cache的数据转为可变map
   */
  private def getCacheInfo(
    cacheInfo: scala.collection.immutable.Map[String, scala.collection.immutable.Map[String, String]]) = {
    val labelsPropMap = Map[String, Map[String, String]]()
    cacheInfo.map(infoMap => {
      val copProp = Map[String, String]()
      infoMap._2.foreach(copProp += _)
      labelsPropMap += (infoMap._1 -> copProp)
    })
    labelsPropMap
  }

  /**
   * 编辑完chache中的内容后重新置为不可变类属
   */
  private def setCacheInfo(labelsPropMap: Map[String, Map[String, String]]) = {
    import scala.collection.immutable.Map
    if (labelsPropMap.isEmpty) Map[String, Map[String, String]]()
    else labelsPropMap.map(propSet => (propSet._1, propSet._2.toMap)).toMap
  }
  /**
   * 从cache的区域List中取出最大的lastTime<br>
   */

  private def getCacheMaxLastTime(labelsPropMap: Map[String, Map[String, String]]): Long = {
    val areaPropArray = labelsPropMap.toArray

    if (areaPropArray.isEmpty) 0L
    else {
      // 对用户cache中的区域列表按lastTime排序（升序）
      quickSort(areaPropArray)(Ordering.by(_._2.get(LabelConstant.LABEL_STAY_LASTTIME)))
      // 取用户cache区域列表中的最大lastTime
      areaPropArray.reverse(0)._2.get(LabelConstant.LABEL_STAY_LASTTIME).get.toLong
    }
  }

  /**
   * 打标签处理并且更新cache<br>
   */
  private def labelAction(
    location: String,
    labelsPropMap: Map[String, Map[String, String]],
    mcStayLabelsMap: Map[String, String],
    mcTime: Long) {
    // cache属性map
    //    lazy val labelsPropMap = cacheInstance.labelsPropList
    // b. 使用宽松的过滤策略，相同区域信令如果间隔超过${thresholdValue}，则判定为不连续
    val area = labelsPropMap.get(location)
    area match {
      case None => {
        mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_TIME_ZERO)
        addCacheAreaStayTime(labelsPropMap, location, mcTime, mcTime)
      }
      case Some(currentStatus) => {
        val first = getCacheStayTime(currentStatus).get("first").get
        val last = getCacheStayTime(currentStatus).get("last").get
        println("FIRST TIME : " + first + " , LAST TIME : " + last + " , MCTIME : " + mcTime)
        if (first > last) {
          // 无效数据，丢弃，本条视为first
          mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_DEFAULT_TIME)
          updateCacheStayTime(currentStatus, mcTime, mcTime)
        } else if (mcTime < first) {
          // 本条记录属于延迟到达，更新开始时间
          currentStatus.put(LabelConstant.LABEL_STAY_FIRSTTIME, mcTime.toString)
          mcStayLabelsMap.put(location, evaluateTime(last - first, last - mcTime))
          if (first - mcTime > thresholdValue) {
            // 本条记录无效，输出空标签，不更新cache
            mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_TIME_ZERO)
          } else {
            // 本条记录属于延迟到达，更新开始时间
            currentStatus.put(LabelConstant.LABEL_STAY_FIRSTTIME, mcTime.toString)
            mcStayLabelsMap.put(location, evaluateTime(last - first, last - mcTime))
          }
        } else if (mcTime <= last) {
          // 本条属于延迟到达，不处理
          mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_DEFAULT_TIME)
        } else if (mcTime - last > thresholdValue) {
          // 本条与上一条数据间隔过大，判定为不连续
          mcStayLabelsMap += (location -> LabelConstant.LABEL_STAY_DEFAULT_TIME)
          updateCacheStayTime(currentStatus, mcTime, mcTime)
        } else {
          // 本条为正常新数据，更新cache后判定
          currentStatus.put(LabelConstant.LABEL_STAY_LASTTIME, mcTime.toString)

          val newtime = evaluateTime(last - first, mcTime - first)
          mcStayLabelsMap.put(location, newtime)
        }
      }
    }
  }

  /**
   * 取cache中的firstTime,lastTime<br>
   * 返回结果map,key:"first"和"last"<br>
   */
  private def getCacheStayTime(currentStatus: Map[String, String]) = {
    val first = currentStatus.get(LabelConstant.LABEL_STAY_FIRSTTIME)
    val last = currentStatus.get(LabelConstant.LABEL_STAY_LASTTIME)
    if (first == None || last == None) Map("first" -> 0l, "last" -> 0l)
    else Map("first" -> first.get.toLong, "last" -> last.get.toLong)
  }

  /**
   * 在cache中追加新的区域属性map并设值<br>
   */
  private def addCacheAreaStayTime(
    labelsPropMap: Map[String, Map[String, String]],
    location: String,
    firstTime: Long,
    lastTime: Long) {
    val map = Map[String, String]()
    updateCacheStayTime(map, firstTime, lastTime)
    labelsPropMap += (location -> map)
  }

  /**
   * 更新cache中区域属性map的firstTime,lastTime值<br>
   */
  private def updateCacheStayTime(
    map: Map[String, String],
    firstTime: Long,
    lastTime: Long) {
    map += (LabelConstant.LABEL_STAY_FIRSTTIME -> (firstTime).toString)
    map += (LabelConstant.LABEL_STAY_LASTTIME -> (lastTime).toString)
  }
}