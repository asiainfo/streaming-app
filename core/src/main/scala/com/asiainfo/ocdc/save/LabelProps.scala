package com.asiainfo.ocdc.save

import com.asiainfo.ocdc.streaming.StreamingCache

import scala.beans.BeanProperty
import scala.collection.mutable.Map

/**
 * @author surq
 * @since 2015.4.2
 * @comment cache中的细粒度对像
 */
class LabelProps extends StreamingCache with Serializable {

  @BeanProperty
  var labelsPropList:Map[String,Map[String,String]] = null
  private def setLocation(property:String):Tuple2[String, Map[String, String]] = {
  (property,Map[String, String]())
  }
  
  def setSingleConditionProps(localList:List[String])= {
   val labelsPropList = localList.map(setLocation)
  }
}