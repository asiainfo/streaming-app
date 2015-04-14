package com.asiainfo.ocdc.streaming

import scala.collection.mutable.{ Map, ArrayBuffer }
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
  // 从配置文件读入
  lazy val selfDefStayTimeList = conf.get("stay.limits").split(",")
  // 推送满足设置的数据坎的最大值:true;最小值：false
  lazy val userDefPushOrde = conf.getBoolean("stay.matchMax", true)
  // 推送满足设置的数据的限定值，还是真实的累计值.真实的累计值:false;限定值:true
  lazy val pushLimitValue = conf.getBoolean("stay.outputThreshold", true)
  // 无效数据阈值的设定
  lazy val thresholdValue = conf.getLong("stay.timeout", 30 * 60 * 1000)

  def evaluateTime(oldStayTime: Long, newStayTime: Long): Long = {

    val matchList = selfDefStayTimeList.filter(limit => (oldStayTime <= limit.toLong && newStayTime >= limit.toLong))
    if (matchList.isEmpty) 0
    // 推送设置的数据坎的值
    else if (pushLimitValue) (if (userDefPushOrde) matchList.map(_.toLong).max else matchList.map(_.toLong).min)
    // 推送真实数据值
    else newStayTime
  }

  def attachMCLabel(mc: MCSourceObject, cache: StreamingCache) {
    val cacheInstance = if (cache == null) new LabelProps
    else cache.asInstanceOf[LabelProps]

    // 取在siteRule（区域规则）中所打的area标签list
    val locationList = (mc.getLabels(Constant.LABEL_ONSITE)).keys

    // mcsource labels用
    val mcStayLabelsMap = Map[String, String]()

    // 使用宽松的过滤策略，相同区域信令如果间隔超过${thresholdValue}，则判定为不连续
    locationList.map(location => {
      val area = cacheInstance.labelsPropList.get(location)
      area match {
        case None => {
          mcStayLabelsMap += (location -> "0")
          cacheInstance.labelsPropList.put(location, Map(
            Constant.LABEL_STAY_FIRSTTIME -> (mc.time).toString,
            Constant.LABEL_STAY_LASTTIME -> (mc.time).toString))
        }
        case Some(currentStatus) => {
          val first = currentStatus.get(Constant.LABEL_STAY_FIRSTTIME).getOrElse("0").toLong
          val last = currentStatus.get(Constant.LABEL_STAY_LASTTIME).getOrElse("0").toLong

          if (first > last) {
            // 无效数据，丢弃，本条视为first
            mcStayLabelsMap += (location -> "0")
            cacheInstance.labelsPropList.put(location, Map(
              Constant.LABEL_STAY_FIRSTTIME -> (mc.time).toString,
              Constant.LABEL_STAY_LASTTIME -> (mc.time).toString))
          } else if (mc.time < first) {
            if (first - mc.time > thresholdValue) {
              // 本条记录无效，输出空标签，不更新cache
              mcStayLabelsMap += (location -> "0")
            } else {
              // 本条记录属于延迟到达，更新开始时间
              currentStatus.put(Constant.LABEL_STAY_FIRSTTIME, mc.time.toString)
              mcStayLabelsMap.put(location, evaluateTime(last - first, last - mc.time).toString)
            }
          } else if (mc.time <= last) {
            // 本条属于延迟到达，不处理
            mcStayLabelsMap += (location -> "0")
          } else if (mc.time - last > thresholdValue) {
            // 本条与上一条数据间隔过大，判定为不连续
            mcStayLabelsMap += (location -> "0")
            cacheInstance.labelsPropList.put(location, Map(
              Constant.LABEL_STAY_FIRSTTIME -> (mc.time).toString,
              Constant.LABEL_STAY_LASTTIME -> (mc.time).toString))
          } else {
            // 本条为正常新数据，更新cache后判定
            currentStatus.put(Constant.LABEL_STAY_LASTTIME, mc.time.toString)
            mcStayLabelsMap.put(location, evaluateTime(last - first, mc.time - first).toString)
          }
        }
      }
    })
    // 清除cache中过期的区域信息（判断标准：取本用户cache中所有区域中的
    // 最大lastTime向前推一个无效数据阈值时间［stay.timeout］，若小于此时刻则视为离开该区域）
    val areaPropList = cacheInstance.labelsPropList.toArray
    if (areaPropList.size > 1) {
      // 对用户cache中的区域列表按lastTime排序（升序）
      quickSort(areaPropList)(Ordering.by(_._2.getOrElse(Constant.LABEL_STAY_LASTTIME, "0")))
      // 取用户cache区域列表中的最大lastTime
      val maxLastTime = areaPropList(areaPropList.size - 1)._2.get(Constant.LABEL_STAY_LASTTIME).get.toLong
      // 遍历列表与最大时间想比较若时间超过用户定义的［无效数据阈值］则视为离开该区域
      val invalidAreaList = for {index <- 0 until areaPropList.size - 1;
        propLastTime = areaPropList(index)._2.getOrElse(Constant.LABEL_STAY_LASTTIME, "0").toLong;
        if (maxLastTime - propLastTime > thresholdValue)
      } yield areaPropList(index)._1
      // 删除无效区域属性
      invalidAreaList map cacheInstance.labelsPropList.remove
    }
    // 给mcsoruce设定连续停留[LABEL_STAY]标签
    mc.setLabel(Constant.LABEL_STAY, mcStayLabelsMap)
  }
}