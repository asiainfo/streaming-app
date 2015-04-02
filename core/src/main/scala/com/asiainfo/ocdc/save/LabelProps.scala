package com.asiainfo.ocdc.save

import scala.beans.BeanProperty
import scala.collection.mutable.Map

class LabelProps extends Serializable {

  @BeanProperty
  var labelsPropList:List[Tuple2[String,Map[String,String]]] = null
  private def setLocation(property:String):Tuple2[String, Map[String, String]] = {
  (property,Map[String, String]())
  }
  
  def setSingleConditionProps(localList:List[String])= {
   val labelsPropList = localList.map(setLocation)
  }
}