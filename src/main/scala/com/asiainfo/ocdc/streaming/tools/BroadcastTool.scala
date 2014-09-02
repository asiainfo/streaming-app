package com.asiainfo.ocdc.streaming.tools

import org.apache.spark.broadcast.Broadcast
import scala.collection.mutable.HashMap

/**
 * Created by wangxj8 on 14-9-2.
 */
object BroadcastTool {
  def getValue(broadcast:Broadcast[HashMap[String, HashMap[String, Array[(String,String)]]]],tableName:String,key:String,columns:Array[String]):Array[(String,String)]={
    var result:AnyRef = null
      val broadcastValue=broadcast.value

      val table = broadcastValue.getOrElse(tableName,null)

      if(table != null){
        val keyvalue = table.getOrElse(key,null)
        if (keyvalue != null){
          result = columns.map(x=>{
            (tableName+"."+keyvalue.toMap.getOrElse(x,""))
          })
        }
      }
    result.asInstanceOf[Array[(String,String)]]
  }
}
