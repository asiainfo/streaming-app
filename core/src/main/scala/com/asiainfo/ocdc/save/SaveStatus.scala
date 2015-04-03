package com.asiainfo.ocdc.save

import com.asiainfo.ocdc.streaming.SourceObject
import scala.collection.mutable.Map
import scala.beans.BeanProperty
import com.asiainfo.ocdc.streaming.StreamingCache

/**
 * @author surq
 * @since 2015.4.2
 * @comment mc信令cache结构
 */
class SaveStatus extends StreamingCache with Serializable {
  @BeanProperty
  var updateStatus: Tuple2[String, LabelProps] = null
}