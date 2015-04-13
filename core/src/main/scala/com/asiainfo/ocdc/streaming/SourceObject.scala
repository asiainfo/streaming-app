package com.asiainfo.ocdc.streaming

import scala.collection.mutable.Map

/**
 * Created by tianyi on 3/26/15.
 */
abstract class SourceObject(val slabels: Map[String, Map[String, String]]) extends Serializable {

  def setLabel(key: String, value: Map[String, String]) = {
    slabels += (key -> value)
  }

  def getLabels(key: String): Map[String, String] = {
    if (!slabels.contains(key)) {
      setLabel(key, Map[String, String]())
    }
    slabels.get(key).get
  }

  final def removeLabel(key: String) = {
    slabels.remove(key)
  }

  def generateId: String
}

