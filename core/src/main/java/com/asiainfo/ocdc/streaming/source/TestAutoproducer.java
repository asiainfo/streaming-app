package com.asiainfo.ocdc.streaming.source;

import java.io.FileReader;
import java.io.LineNumberReader;
import java.util.ArrayList;

import kafka.producer.KeyedMessage;

public class TestAutoproducer {

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