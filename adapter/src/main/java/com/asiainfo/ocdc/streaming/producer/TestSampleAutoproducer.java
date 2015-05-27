package com.asiainfo.ocdc.streaming.producer;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
import java.util.HashMap;

import kafka.producer.KeyedMessage;

/**
 * @author 宿荣全<br>
 * @since 2015.5.11<br>
 * send kafka 消息<br>
 * @param msg
 */
public class TestSampleAutoproducer{

	public static void main(String args[]) {

		 producerSendTest();
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
		// 根据socket.printCount.printflg判断是否启动打印单位时间内接收的数据条数
		HashMap<String,Long> countMap = new HashMap<String,Long>();
		
		// 统计接收速度
		SpeedCompute speedCompute = new SpeedCompute(countMap,10);
		speedCompute.startTask();
	
		
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
					speedCompute.counter(1);
					// 发送msg
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