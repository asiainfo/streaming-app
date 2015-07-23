package com.asiainfo.ocdc.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, DataFrame}

/**
 * Created by leo on 7/23/15.
 */
class MissCallEvent extends MCBSEvent {
  override def transformDF(old_dataframe: DataFrame, selectExp: Seq[String]): RDD[Row] = {
    old_dataframe.map(row => (row.getString(2), row)).groupByKey().map(_._2.head)
  }
}
