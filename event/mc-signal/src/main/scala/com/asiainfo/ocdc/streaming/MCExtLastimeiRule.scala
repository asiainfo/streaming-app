package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.streaming.constant.LabelConstant
import com.asiainfo.ocdc.streaming.eventrule.StreamingCache
import scala.collection.mutable.Map

/**
 * @author zt
 * @since 2015.6.24
 * @comment extern mc label: the imei who last used.
 */

class MCExtLastimeiRule extends MCLabelRule {
  def attachMCLabel(mcSourceObj: MCSourceObject, cache: StreamingCache,
                    labelQryData: Map[String, Map[String, String]]): StreamingCache = {

    val changeImeiCache = if (cache == null) new ChangeImeiProps
    else cache.asInstanceOf[ChangeImeiProps]
    mcSourceObj.setLabel(LabelConstant.LABEL_LASTIMEI, Map("imei" -> changeImeiCache.cacheValue))
    changeImeiCache.cacheValue = mcSourceObj.getImei
    changeImeiCache
  }
}

class ChangeImeiProps extends StreamingCache with Serializable {
  var cacheValue: String = ""
}