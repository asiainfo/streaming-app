package com.asiainfo.ocdc.streaming

import org.apache.spark.sql.DataFrame

/**
 * Created by leo on 4/9/15.
 */
class MCEventRule extends EventRule {

  def output(data: DataFrame) {
    val selcol_size = selectExp.size
    data.map(row => {
      var message: String = ""
      for (i <- 0 to (selcol_size - 1)) {
        message += row.get(i).toString
      }
      message
    }).saveAsTextFile("hdfs://localhost:9000/user/leo/streaming/output")
  }

}
