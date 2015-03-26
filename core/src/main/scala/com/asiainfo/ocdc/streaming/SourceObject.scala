package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */
abstract class SourceObject extends Serializable {
  private val labels = new java.util.HashMap[String, Object]()
  final def setLabel(key: String, value: Object) = {
    labels.put(key, value)
  }
  final def removeLabel(key: String) = {
    labels.remove(key)
  }
}

