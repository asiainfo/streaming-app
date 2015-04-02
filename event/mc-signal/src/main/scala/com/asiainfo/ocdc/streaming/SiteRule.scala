package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.save.LabelProps

/**
 * @author surq
 * @since 2015.4.2
 * @comment 给mc信令标记区域标签
 */
class SiteRule extends MCLabelRule {
  def attachMCLabel(mcSourceObj: MCSourceObject, cache: StreamingCache) = {
    val lac = mcSourceObj.lac
    val ci = mcSourceObj.ci

    // 根据largeCell解析出所属区域
    val onsiteList = largeCellAnalysis((lac, ci))
    val propMap = scala.collection.mutable.Map[String, String]()
    onsiteList.map(location => (propMap += (location -> "true")))
    mcSourceObj.setLabel(Constant.LABEL_ONSITE, propMap)
  }

  /**
   * 根据largeCell解析出所属区域
   * @param onsite:MC信令代码
   * @return 所属区域列表
   */
  def largeCellAnalysis(onsite: Tuple2[Int, Int]): List[String] = {

    // TODO 根据onsite与KV库中的表并联找出所属区域（“学校”，“商场”）
    List("校园", "商场")

    val set = new java.util.HashSet[Char]()
    onsite._1.toString.substring(0, 3).toCharArray.foreach(x => set.add(x))
    set.toArray.map(_.toString).toList

  }
}