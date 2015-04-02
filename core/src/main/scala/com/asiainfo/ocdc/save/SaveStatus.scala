package com.asiainfo.ocdc.save

import com.asiainfo.ocdc.streaming.SourceObject
import scala.collection.mutable.Map
import scala.beans.BeanProperty

class SaveStatus extends StreamingCache with Serializable {
  @BeanProperty
  var updateStatus: Tuple2[String, LabelProps] = null
}