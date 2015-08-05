package com.asiainfo.ocdc.streaming

import org.apache.spark.streaming.scheduler.{StreamingListenerBatchSubmitted, StreamingListener}

/**
 * Created by leo on 7/2/15.
 */
class ReceiveRecordNumListener extends StreamingListener {

  /** Called when a batch of jobs has been submitted for processing. */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    //TODO: update configuration periodicly
    /*
    val baseConfUpdateInteval = MainFrameConf.getInt("BaseConfUpdateInteval")
    def currentTime = System.currentTimeMillis()

    if (MainFrameConf.updateTime + baseConfUpdateInteval <= currentTime) {
      println("lastUpdateTimeMs = "+ MainFrameConf.updateTime +", currentTimeMs = "+currentTime+
              ", diffTimeMs=" +(currentTime-MainFrameConf.updateTime) +">= baseConfUpdateIntevalMs ("+ baseConfUpdateInteval +"), Begin to update MainFrameConf... ")
      MainFrameConf.update(currentTime)
      println("update MainFrameConf done.")
    }
    */
    println("Current batch receive record number : " + batchSubmitted.batchInfo.numRecords)

  }
}
