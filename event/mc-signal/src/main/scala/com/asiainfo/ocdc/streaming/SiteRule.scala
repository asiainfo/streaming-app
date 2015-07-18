package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.constant.LabelConstant
import com.asiainfo.ocdc.streaming.eventrule.StreamingCache
import org.slf4j.LoggerFactory

import scala.collection.mutable.Map

/**
 * @author surq
 * @since 2015.4.2
 *  给mc信令标记区域标签
 */
class SiteRule extends MCLabelRule {

  val logger = LoggerFactory.getLogger(this.getClass)

  def attachMCLabel(mcSourceObj: MCSourceObject, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache = {

    // 装载业务区域标签属性
    val onSiteMap = scala.collection.mutable.Map[String, String]()
    // 根据largeCell解析出所属区域
//    println("current lacci : " + getQryKeys(mcSourceObj))
    val cachedArea = labelQryData.get(getQryKeys(mcSourceObj).head).get
    logger.debug("= = " * 20 + " cachedArea = " + cachedArea.mkString("[", ",", "]"))
    if (cachedArea.contains("areas")) {
      val areas = cachedArea("areas").trim()
      if (areas != "") areas.split(",").foreach(area => {
//        println("current area : " + area)
        onSiteMap += (area.trim -> "true")
      })
    }

    logger.debug("= = " * 20 + " onSiteMap = " + onSiteMap.mkString("[", ",", "]"))

    // 标记业务区域标签
    mcSourceObj.setLabel(LabelConstant.LABEL_ONSITE, onSiteMap)
    // 标记行政区域标签
    mcSourceObj.setLabel(LabelConstant.LABEL_AREA, cachedArea.filter(_._1 != "areas"))
    cache
  }

  /**
   * @param mc:MC信令对像
   * @return codis数据库的key
   */
  override def getQryKeys(mc: SourceObject): Set[String] = {
    val mcsource = mc.asInstanceOf[MCSourceObject]
    Set[String]("lacci2area:" + mcsource.lac + ":" + mcsource.ci)
  }
}