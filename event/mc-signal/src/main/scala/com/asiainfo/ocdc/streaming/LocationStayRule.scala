package com.asiainfo.ocdc.streaming

import scala.collection.mutable.Map
import scala.util.Sorting.quickSort

/**
 * @author surq
 * @since 2015.4.2
 * @comment 给mc信令标记连续停留时间标签
 */

class LabelProps extends StreamingCache with Serializable {
  val labelsPropList: Map[String, Map[String, String]] = Map[String, Map[String, String]]()
}

class LocationStayRule extends MCLabelRule {
  // 从配置文件读入 用户设定的各业务连续停留的时间坎(排序升序)
  lazy val selfDefStayTimeList = conf.get(Constant.STAY_LIMITS).split(Constant.ITME_SPLIT_MARK)
  lazy val stayTimeOrderList = selfDefStayTimeList.map(_.toLong).sorted
  // 推送满足设置的数据坎的最大值:true;最小值：false
  lazy val userDefPushOrde = conf.getBoolean(Constant.STAY_MATCHMAX, true)
  // 推送满足设置的数据的限定值，还是真实的累计值.真实的累计值:false;限定值:true
  lazy val pushLimitValue = conf.getBoolean(Constant.STAY_OUTPUTTHRESHOLD, true)
  // 无效数据阈值的设定
  lazy val thresholdValue = conf.getLong(Constant.STAY_TIMEOUT, Constant.DEFAULT_TIMEOUT_VALUE)

  /**
   * 根据本次以及前次的停留时间计算出标签停留时间的值
   */
  private def evaluateTime(oldStayTime: Long, newStayTime: Long): String = {
    // 新状态未达到最小坎时或旧状态超过最大坎值时返回黙认值“0”
    if (newStayTime < stayTimeOrderList(0) ||
      oldStayTime > stayTimeOrderList(stayTimeOrderList.size - 1)) {
      Constant.LABEL_STAY_DEFAULT_TIME
    } else {
      val matchList = stayTimeOrderList.filter(limit => (oldStayTime <= limit && newStayTime >= limit))
      // 新旧停留时间在某坎区间内，返回黙认值“0”
      if (matchList.isEmpty) Constant.LABEL_STAY_DEFAULT_TIME
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
  def attachMCLabel(mc: MCSourceObject, cache: StreamingCache): StreamingCache = {
    //a. 获取locationStayRule的cache对象
    val cacheInstance = if (cache == null) new LabelProps
    else cache.asInstanceOf[LabelProps]
    cache.asInstanceOf[LabelProps]
    // cache中各区域的属性map
    val labelsPropMap = cacheInstance.labelsPropList

    // mcsource 打标签用
    val mcStayLabelsMap = Map[String, String]()
    // cache中所有区域的最大lastTime
    val cacheMaxLastTime = getCacheMaxLastTime(labelsPropMap)
    // 取在siteRule（区域规则）中所打的area标签list
    val locationList = (mc.getLabels(Constant.LABEL_ONSITE)).keys
    locationList.map(location => {
      println(" Find Current site : " + location)
      // A.此用的所有区域在cache中的信息已经过期视为无效，标签打为“0”；重新设定cache;
      if (mc.time - cacheMaxLastTime > thresholdValue) {
        // 1. 连续停留标签置“0”
        mcStayLabelsMap += (location -> Constant.LABEL_STAY_TIME_ZERO)
        // 2. 清除cache信息
        labelsPropMap.clear
        // 3. 重置cache信息
        val cacheStayLabelsMap = Map[String, String]()
        cacheStayLabelsMap += (Constant.LABEL_STAY_FIRSTTIME -> mc.time.toString)
        cacheStayLabelsMap += (Constant.LABEL_STAY_LASTTIME -> mc.time.toString)
        labelsPropMap += (location -> cacheStayLabelsMap)
      } else if (cacheMaxLastTime - mc.time > thresholdValue) {
        //B.此条数据为延迟到达的数据，已超过阈值视为无效，标签打为“0”；cache不做设定;
        // 1. 连续停留标签置“0”
        mcStayLabelsMap += (location -> Constant.LABEL_STAY_TIME_ZERO)
      } else {
        //C.此条数据时间为[(maxLastTime-thresholdValue)~maxLastTime~(maxLastTime+thresholdValue)]之间
        labelAction(location, cacheInstance, mcStayLabelsMap, mc.time)
      }
    })
    // c. 给mcsoruce设定连续停留[LABEL_STAY]标签
    mc.setLabel(Constant.LABEL_STAY, mcStayLabelsMap)
    println(" Current site : " + mc.getLabels(Constant.LABEL_ONSITE))
    mcStayLabelsMap.iterator.foreach(x => {
      println("area : " + x._1 + " , stay : " + x._2)
    })

    cacheInstance
  }

  /**
   * 从cache的区域List中取出最大的lastTime<br>
   */
  private def getCacheMaxLastTime(labelsPropMap: Map[String, Map[String, String]]): Long = {
    val areaPropArray = labelsPropMap.toArray

    if (areaPropArray.isEmpty) 0L
    else {
      // 对用户cache中的区域列表按lastTime排序（升序）
      quickSort(areaPropArray)(Ordering.by(_._2.get(Constant.LABEL_STAY_LASTTIME)))
      // 取用户cache区域列表中的最大lastTime
      areaPropArray.reverse(0)._2.get(Constant.LABEL_STAY_LASTTIME).get.toLong
    }
  }

  /**
   * 打标签处理并且更新cache<br>
   */
  private def labelAction(
                           location: String,
                           cacheInstance: LabelProps,
                           mcStayLabelsMap: Map[String, String],
                           mcTime: Long) {
    // cache属性map
    lazy val labelsPropMap = cacheInstance.labelsPropList
    // b. 使用宽松的过滤策略，相同区域信令如果间隔超过${thresholdValue}，则判定为不连续
    val area = labelsPropMap.get(location)
    area match {
      case None => {
        mcStayLabelsMap += (location -> Constant.LABEL_STAY_TIME_ZERO)
        addCacheAreaStayTime(cacheInstance, location, mcTime, mcTime)
      }
      case Some(currentStatus) => {
        val first = getCacheStayTime(currentStatus).get("first").get
        val last = getCacheStayTime(currentStatus).get("last").get
        if (first > last) {
          // 无效数据，丢弃，本条视为first
          mcStayLabelsMap += (location -> Constant.LABEL_STAY_DEFAULT_TIME)
          updateCacheStayTime(currentStatus, mcTime, mcTime)
        } else if (mcTime < first) {
          // 本条记录属于延迟到达，更新开始时间
          currentStatus.put(Constant.LABEL_STAY_FIRSTTIME, mcTime.toString)
          mcStayLabelsMap.put(location, evaluateTime(last - first, last - mcTime))
          if (first - mcTime > thresholdValue) {
            // 本条记录无效，输出空标签，不更新cache
            mcStayLabelsMap += (location -> Constant.LABEL_STAY_TIME_ZERO)
          } else {
            // 本条记录属于延迟到达，更新开始时间
            currentStatus.put(Constant.LABEL_STAY_FIRSTTIME, mcTime.toString)
            mcStayLabelsMap.put(location, evaluateTime(last - first, last - mcTime))
          }
        } else if (mcTime <= last) {
          // 本条属于延迟到达，不处理
          mcStayLabelsMap += (location -> Constant.LABEL_STAY_DEFAULT_TIME)
        } else if (mcTime - last > thresholdValue) {
          // 本条与上一条数据间隔过大，判定为不连续
          mcStayLabelsMap += (location -> Constant.LABEL_STAY_DEFAULT_TIME)
          updateCacheStayTime(currentStatus, mcTime, mcTime)
        } else {
          // 本条为正常新数据，更新cache后判定
          currentStatus.put(Constant.LABEL_STAY_LASTTIME, mcTime.toString)
          mcStayLabelsMap.put(location, evaluateTime(last - first, mcTime - first))
        }
      }
    }
  }

  /**
   * 取cache中的firstTime,lastTime<br>
   * 返回结果map,key:"first"和"last"<br>
   */
  private def getCacheStayTime(currentStatus: Map[String, String]) = {
    val first = currentStatus.get(Constant.LABEL_STAY_FIRSTTIME)
    val last = currentStatus.get(Constant.LABEL_STAY_LASTTIME)
    if (first == None || last == None) Map("first" -> 0l, "last" -> 0l)
    else Map("first" -> first.get.toLong, "last" -> last.get.toLong)
  }

  /**
   * 在cache中追加新的区域属性map并设值<br>
   */
  private def addCacheAreaStayTime(
                                    cacheInstance: LabelProps,
                                    location: String,
                                    firstTime: Long,
                                    lastTime: Long) {
    val map = Map[String, String]()
    updateCacheStayTime(map, firstTime, lastTime)
    cacheInstance.labelsPropList.put(location, map)
  }

  /**
   * 更新cache中区域属性map的firstTime,lastTime值<br>
   */
  private def updateCacheStayTime(
                                   map: Map[String, String],
                                   firstTime: Long,
                                   lastTime: Long) {
    map += (Constant.LABEL_STAY_FIRSTTIME -> (firstTime).toString)
    map += (Constant.LABEL_STAY_LASTTIME -> (lastTime).toString)
  }
}