package com.asiainfo.ocdc.streaming

import org.apache.spark.streaming.scheduler.{StreamingListenerBatchSubmitted, StreamingListener}

/**
 * Created by leo on 7/2/15.
 */
class ReceiveRecordNumListener extends StreamingListener {

  /** Called when a batch of jobs has been submitted for processing. */
  override def onBatchSubmitted(batchSubmitted: StreamingListenerBatchSubmitted) {
    val baseConfUpdateInteval = MainFrameConf.getInt("BaseConfUpdateInteval")
    val currentTime = System.currentTimeMillis()

    if (MainFrameConf.updateTime + baseConfUpdateInteval >= currentTime)
      MainFrameConf.update(currentTime)

    System.out.println("Current batch receive record number : " + batchSubmitted.batchInfo.numRecords)

  }
}
