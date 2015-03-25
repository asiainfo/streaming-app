package com.asiainfo.ocdc.streaming.rule

import com.asiainfo.ocdc.streaming.source.SourceObj

trait LabelRule extends Serializable{
  //  val refTable: String
  //  def getRefKey(item: SourceObj): String
  //  def attachLabel(item: SourceObj, refValue: Option[Object]): (SourceObj, Option[Object])
  def attachLabel(item: SourceObj): SourceObj
}

trait EventRule extends Serializable{
  val selExpr: Map[String, String]
  val filterExpr: String
}