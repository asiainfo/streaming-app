package com.asiainfo.ocdc.streaming.producer;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.logging.Logger;

import kafka.producer.KeyedMessage;

public class TestAutoproducer{

	@SuppressWarnings("static-access")
	public static void main(String args[]) {

		 producerSendTest();
		 
//		 timerTest();		 
		
	}
@SuppressWarnings("static-access")
private static void timerTest() {
	HashMap<String,Long> countMap = new HashMap<String,Long>();
	long printInterval = 1*1000;
	
	
	SocketReceiveCountTasker SRCountTasker = null;

	Timer timer = new Timer();
	SRCountTasker = new SocketReceiveCountTasker(countMap,printInterval);
	SRCountTasker.init();
	timer.schedule(SRCountTasker, 0, printInterval);

	long receiveCount = countMap.get("thisCount");
	receiveCount += 1;
	countMap.put("thisCount", receiveCount);
	
	try {
		Thread.currentThread().sleep(1000);
	} catch (InterruptedException e) {
		e.printStackTrace();
	}
}
	
	/**
	 * kafka producer test<br>
	 */
	@SuppressWarnings("static-access")
	private static void producerSendTest() {

		// 代码测试部分start－－－－－
		SendUtil sendutil = new SendUtil();
		sendutil.runThreadPoolTask();
		ArrayList<KeyedMessage<String, String>> msgList = null;
		// 代码测试部分end－－－－－
		LineNumberReader reader;
		try {
			while (true) {
				reader = new LineNumberReader(new FileReader(
						"/home/hadoop/hs_err_pid3676.log"));
				String line = null;
				while ((line = reader.readLine()) != null) {
					String xdrsend = reader.getLineNumber() + ":" + line;
					// 代码测试部分start－－－－－
					msgList = sendutil.packageMsg(xdrsend, msgList);
					// 代码测试部分end－－－－－
				}
				Thread.currentThread().sleep(1000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}