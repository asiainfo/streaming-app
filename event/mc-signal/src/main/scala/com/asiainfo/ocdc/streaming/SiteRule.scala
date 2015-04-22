package com.asiainfo.ocdc.streaming


/**
 * @author surq
 * @since 2015.4.2
 * @comment 给mc信令标记区域标签
 */
class SiteRule extends MCLabelRule {
  def attachMCLabel(mcSourceObj: MCSourceObject, cache: StreamingCache): StreamingCache = {
    val lac = mcSourceObj.lac
    val ci = mcSourceObj.ci

    // 根据largeCell解析出所属区域
    val onsiteList = largeCellAnalysis(lac, ci)
    val propMap = scala.collection.mutable.Map[String, String]()
    onsiteList.foreach(location => propMap += (location -> "true"))
    mcSourceObj.setLabel(Constant.LABEL_ONSITE, propMap)
    println(" set site label !!! ")
    propMap.iterator.foreach(x => {
      println(" area : " + x._1 + " flag : " + x._2)
    })
    cache
  }

  /**
   * 根据largeCell解析出所属区域
   * @param lac:MC信令代码
   * @param ci:MC信令代码
   * @return 所属区域列表
   */
  def largeCellAnalysis(lac: String, ci: String): List[String] = {
    val cachedArea = CacheFactory.getManager.getCommonCacheValue(
        "lacci2area", lac+":"+ci)
    if(cachedArea == null) {
      List[String]()
    } else {
      val list = cachedArea.split(",").toList
      list
    }
  }
}