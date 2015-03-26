package com.asiainfo.ocdc.streaming

/**
 * Created by tianyi on 3/26/15.
 */
trait LabelRule extends Serializable{
  def init(conf: LabelRuleConf) = ???

  //  val refTable: String
  //  def getRefKey(item: SourceObj): String
  //  def attachLabel(item: SourceObj, refValue: Option[Object]): (SourceObj, Option[Object])
  def attachLabel(item: SourceObject): SourceObject
}