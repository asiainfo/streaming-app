package com.asiainfo.ocdc.streaming.producer;

import java.util.HashMap;
import java.util.Timer;

import com.asiainfo.ocdc.streaming.tasks.ReceiveCountTasker;

/**
 * @author 宿荣全<br>
 * @since 2015.5.26<br>
 * 统计指定时间内接收的数据量<br>
 */
public class SpeedCompute {
	
	private ReceiveCountTasker SRCountTasker = null;
	private HashMap<String,Long> countMap = null;
	private long runInerval = 0;
	
	public SpeedCompute (HashMap<String,Long> countMap, long runInerval) {
		this.countMap = countMap;
		this.runInerval = runInerval;
	}

	/**
	 * 启动指定时间内接收多少数据的任务<br>
	 */
	public void startTask () {
		Timer timer = new Timer();
		SRCountTasker = new ReceiveCountTasker(countMap,runInerval);
		cleanContainer();
		timer.schedule(SRCountTasker, 10, runInerval);
	}
	
	/**
	 * 清空计数map<br>
	 * countMap 初始化<br>
	 */
	public void cleanContainer() {
		// 统记指标用的map
		countMap.put("lastCount", 0l);
		countMap.put("thisCount", 0l);
	}
	
	/**
	 * 计算器
	 * @param value：每次递加的数值<br>
	 */
	public void counter(int value) {
		// 成功接收一条数据，统计值加1
		long receiveCount = countMap.get("thisCount");
		receiveCount += value;
		if (receiveCount ==Long.MAX_VALUE){
			receiveCount = 0;
		}
		countMap.put("thisCount", receiveCount);
	}
}