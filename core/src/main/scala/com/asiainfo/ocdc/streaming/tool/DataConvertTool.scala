package com.asiainfo.ocdc.streaming.tool

/**
 * Created by leo on 4/22/15.
 */
object DataConvertTool {

  def convertHex(in:String):String ={
    var lc = Integer.toHexString(in.toInt).toUpperCase
    while(lc.length<4){lc = "0" + lc}
    lc
  }

  /*def sort(xs: RDD[(String,SourceObject)]):RDD[(String,SourceObject)] = {
    if(xs.length <= 1)
      xs
    else {
      val pivot = xs(xs.length /2)
      Array.concat(
        sort(xs filter (pivot._1 >_._1)),
        xs filter (pivot ==),
        sort(xs filter (pivot._1 < _._1))
      )
    }
  }*/

}
