package com.asiainfo.ocdc.streaming.tasks;

import java.util.HashMap;
import java.util.TimerTask;

import org.apache.log4j.Logger;

import com.asiainfo.ocdc.streaming.producer.SendUtil;

/**
 * @author surq<br>
 * @since 2015.5.11<br>
 * socket recever 单位时间内接收数据量<br>
 * @param msg
 */
public class ReceiveCountTasker extends TimerTask{
	
	private  Logger logger = Logger.getLogger(this.getClass());
	private HashMap<String,Long> countMap = null;
	private long interval = 0;
	
	public ReceiveCountTasker(HashMap<String,Long> countMap,long printInterval) {
		this.countMap = countMap;
		this.interval = printInterval;
	}
    @Override
    public void run() {
    	
    	long thisCount = countMap.get("thisCount");
    	long lastCount = countMap.get("lastCount");
    	long receiveCount = 0l;
    	if (thisCount < lastCount){
    		receiveCount = Long.MAX_VALUE - lastCount + thisCount;
    	}else {
    		receiveCount = thisCount - lastCount;
    	}
    	countMap.put("lastCount", thisCount);
    	logger.info(SendUtil.timeFormat(System.currentTimeMillis()) +
    			":socket receiver 接收速度："+receiveCount+"/"+interval/1000 +"s");
    }
}