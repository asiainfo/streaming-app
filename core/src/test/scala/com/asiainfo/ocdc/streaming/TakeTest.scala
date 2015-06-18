package com.asiainfo.ocdc.streaming

/**
 * Created by leo on 6/17/15.
 */
object TakeTest {

  def main(args: Array[String]) {
    var seq = Seq("a","b","c","d","e")
    while (seq.size > 0) {
      seq.take(2).foreach(println(_))
      println("######################")
      seq = seq.drop(2)
      Thread.sleep(2000)
    }
  }
}
