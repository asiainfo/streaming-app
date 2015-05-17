package com.asiainfo.ocdc.streaming.producer;

import java.util.HashMap;
import java.util.TimerTask;

/**
 * @author surq<br>
 * @since 2015.5.11<br>
 * socket recever 单位时间内接收数据量<br>
 * @param msg
 */
public class SocketReceiveCountTasker extends TimerTask{
	private HashMap<String,Long> countMap = null;
	private long interval = 0;
	
	public SocketReceiveCountTasker(HashMap<String,Long> countMap,long printInterval) {
		this.countMap = countMap;
		this.interval = printInterval;
	}
    @Override
    public void run() {
    	
    	long thicCount = countMap.get("thisCount");
    	long lastCount = countMap.get("lastCount");
    	long receiveCount = thicCount - lastCount;
    	countMap.put("lastCount", thicCount);
    	System.out.println(SendUtil.timeFormat(System.currentTimeMillis()) +
    			":socket receiver 接收速度："+receiveCount+"/"+interval +"s");
    }
    
	/**
	 * 初始化统计用map<br>
	 * @param countMap
	 */
	public void countTool(HashMap<String,Long> countMap){
		// 统记指标用的map
		countMap.put("lastCount", 0l);
		countMap.put("thicCount", 0l);
	}
}