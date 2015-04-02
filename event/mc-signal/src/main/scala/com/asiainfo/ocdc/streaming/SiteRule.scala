package com.asiainfo.ocdc.streaming

import com.asiainfo.ocdc.save.LabelProps

class SiteRule extends MCLabelRule {
  def attachMCLabel(mcSourceObj: MCSourceObject, cache: StreamingCache) = {
    val lac = mcSourceObj.lac
    val ci = mcSourceObj.ci

    // 根据largeCell解析出所属区域
    val onsiteList = largeCellAnalysis((lac, ci))
    val labelProps = new LabelProps
    val areasLableList = labelProps.setSingleConditionProps(onsiteList)
    mcSourceObj.setLabel(Constant.LABEL_ONSITE, labelProps)
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