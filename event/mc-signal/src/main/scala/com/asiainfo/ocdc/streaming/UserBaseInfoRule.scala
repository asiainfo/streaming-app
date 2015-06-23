package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.constant.LabelConstant
import com.asiainfo.ocdc.streaming.eventrule.StreamingCache

import scala.collection.mutable.Map

/**
 * Created by leo on 4/29/15.
 */
class UserBaseInfoRule extends MCLabelRule {
  def attachMCLabel(mcSourceObj: MCSourceObject, cache: StreamingCache, labelQryData: Map[String, Map[String, String]]): StreamingCache = {
    val imsi = mcSourceObj.imsi

    // get user base info by imsi
    //    val user_info_map = CacheFactory.getManager.getHashCacheMap("userinfo:" + imsi)
    //    val user_info_map = CacheCenter.getValue("userinfo:" + imsi, null).asInstanceOf[mutable.Map[String, String]]
    val user_info_map = labelQryData.get(getQryKeys(mcSourceObj)).get

    val info_cols = conf.get("user_info_cols").split(",")

    val propMap = scala.collection.mutable.Map[String, String]()
    if (info_cols.length > 0) {
      info_cols.foreach(x => {
        var v: String = ""
        user_info_map.get(x) match {
          case Some(value) => v = value
          case None =>
        }
        propMap += (x -> v)
      })
    }

    mcSourceObj.setLabel(LabelConstant.USER_BASE_INFO, propMap)
    cache
  }

  override def getQryKeys(mc: SourceObject): String = "userinfo:" + mc.asInstanceOf[MCSourceObject].imsi

}
