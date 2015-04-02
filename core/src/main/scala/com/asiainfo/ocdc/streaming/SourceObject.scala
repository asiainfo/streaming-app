package com.asiainfo.ocdc.streaming
import scala.collection.mutable.Map

/**
 * Created by tianyi on 3/26/15.
 */
abstract class SourceObject extends Serializable {
  private val labels = new java.util.HashMap[String, Map[String,String]]()
  final def setLabel(key: String, value: Map[String,String]) = {
    labels.put(key, value)
  }
  final def getLabel(key: String): Object = {
    labels.get(key)
  }
  final def removeLabel(key: String) = {
    labels.remove(key)
  }
}

