package com.asiainfo.ocdc.streaming.constant

/**
 * @author surq
 * @since 2015.4.2
 * @comment 全局常量定义列表
 */
object LabelConstant {
  /**--------------------标签名称------------------------*/
  // --- user static fields ---
  val USER_BASE_INFO = "user_info"

  // --- lac,ci fields ---
  val LABEL_ONSITE = "area_onsite"
  // stay times
  val LABEL_STAY = "area_onsite_stay"
  // lac,ci static fields
  val LABEL_AREA = "area_info"

  // --- extern fields ---
  val LABEL_LASTIMEI = "ext_last_imei"

  val LABEL_TRACK = "ext_usertrack"


  /**------------------- 标签属性 -----------------------*/
  /** ＝＝＝＝连续停留标签属性＝＝＝＝ */
  // 在某区域的首条记录的时间
  val LABEL_STAY_FIRSTTIME = "firstTime"
  // 在某区域的最后一条记录的时间
  val LABEL_STAY_LASTTIME = "lastTime"
  // 非触点数据时MC数据源上打停留时间标签时的黙认时间
  val LABEL_STAY_DEFAULT_TIME = "0"
    // 收到第一条数据时所打标签值“0”
  val LABEL_STAY_TIME_ZERO = "0"
  /**--------------从配置文件读入参数 -------------------*/
  //字符串黙认字段分隔符
  val ITME_SPLIT_MARK = ","
  // 连续停留时间触发点
  val STAY_LIMITS = "stay.limits"
  // 推送满足设置的数据坎的最大值:true;最小值：false
  val STAY_MATCHMAX = "stay.matchMax"
  // 推送满足设置的数据的限定值，还是真实的累计值.真实的累计值:false;限定值:true
  val STAY_OUTPUTTHRESHOLD = "stay.outputThreshold"
  // 无效数据阈值的设定
  val STAY_TIMEOUT = "stay.timeout"
  //    无效数据阈值的设定
  val DEFAULT_TIMEOUT_VALUE = 30 * 60 * 1000
}