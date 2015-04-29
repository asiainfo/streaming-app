package com.asiainfo.ocdc.streaming

/**
 * Created by leo on 4/29/15.
 */
class UserBaseInfoRule extends MCLabelRule {
  def attachMCLabel(mcSourceObj: MCSourceObject, cache: StreamingCache): StreamingCache = {
    val imsi = mcSourceObj.imsi

    // get user base info by imsi
    val user_info_map = CacheFactory.getManager.getHashCacheMap("userinfo:" + imsi)

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

    mcSourceObj.setLabel(Constant.USER_BASE_INFO, propMap)
    cache
  }

}
