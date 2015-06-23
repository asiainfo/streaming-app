package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.cache.CacheCenter
import com.asiainfo.ocdc.streaming.constant.LabelConstant
import com.asiainfo.ocdc.streaming.eventrule.StreamingCache
import com.asiainfo.ocdc.streaming.tool.CacheFactory

import scala.collection.mutable.Map

/**
 * @author surq
 * @since 2015.4.2
 * @comment 给mc信令标记区域标签
 */
class SiteRule extends MCLabelRule {
  def attachMCLabel(mcSourceObj: MCSourceObject, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache = {

    // 装载业务区域标签属性
    val onSiteMap = scala.collection.mutable.Map[String, String]()
    // 根据largeCell解析出所属区域
    val cachedArea = labelQryData.get(getQryKeys(mcSourceObj)).get
    if (cachedArea != None) {
      var areas = cachedArea("areas").trim()
      if (areas != "") areas.split(".").foreach(area => { onSiteMap += (area.trim -> "true") })
    }

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
  override def getQryKeys(mc: SourceObject): String = {
    val mcsource = mc.asInstanceOf[MCSourceObject]
    "lacci2area:" + mcsource.lac + ":" + mcsource.ci
  }
}