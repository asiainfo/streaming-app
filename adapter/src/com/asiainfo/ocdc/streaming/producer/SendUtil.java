package com.asiainfo.ocdc.streaming.producer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;
//import com.asiainfo.ocdc.streaming.tool.AutoProducer;

public class SendUtil {

	// 用户配置相关参数
	public Properties prop = null;
	// lbk:message List队列
	LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> lbk = 
			new LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>>();
	
	public SendUtil() {
		prop = getPropConfig();
		propeTest();
	}

	/**
	 * 参数配置文件 内容打印测试<br>
	 * @return
	 */
	private void propeTest() {
		Properties prop = getPropConfig();
		Set<Entry<Object, Object>> entrySet = prop.entrySet();
		Iterator<Entry<Object, Object>> it = entrySet.iterator();
		System.out.println("kafka-producer.properties 属性配置一览：");
		while (it.hasNext()) {
			Entry<Object, Object> kvEntry = it.next();
			System.out.print(kvEntry.getKey() + "-----");
			System.out.println(kvEntry.getValue());
		}
	}

	/**
	 * loade 参数配置文件<br>
	 * @return Properties
	 */
	private Properties getPropConfig() {
		Properties prop = new Properties();
		try {
			// test 用
//			prop.load(this.getClass().getResourceAsStream("kafka-producer.properties"));
			prop.load(this.getClass().getClassLoader().getResourceAsStream("kafka-producer.properties"));
		} catch (IOException e) {
			e.printStackTrace();
		}
		return prop;
	}

	/**
	 * 创建线程池并启动线程任务<br>
	 */
	public void runThreadPoolTask() {
		String pNum = prop.getProperty("kafka.producer.numbers");
		int produceNum = 1;// 默认1个producer
		if (!"".equals(pNum.trim()))
			produceNum = Integer.parseInt(pNum.trim());

		ExecutorService executor = Executors.newFixedThreadPool(produceNum);
		for (int i = 0; i < produceNum; i++)
			executor.submit(new ProducerSendTask(lbk, prop));
	}
	
	/**
	 * @param msg：收到的单条消息<br>
	 * @param msgList:根据指定大小封装成message块包（押入队列）<br>
	 * @return msgList 返回message块<br>
	 */
	public ArrayList<KeyedMessage<String, String>> packageMsg (
			String msg, ArrayList<KeyedMessage<String, String>> msgList){
		
		String msgSize = prop.getProperty("kafka.producer.sendmsg.size");
		int msgListSize = 1; // 默认1条
		if (!"".equals(msgSize.trim())) msgListSize = Integer.parseInt(msgSize.trim());
		String topic = prop.getProperty("kafka.topic");
		
		if (!((msg.trim()).equals("") || msgListSize == 0)) {
			if (msgList != null 
					&& msgList.size() % msgListSize == 0 
					&& msgList.size() / msgListSize == 1) {
				lbk.offer(msgList);
				msgList = null;
			}
			if (msgList == null) {
				msgList = new ArrayList<KeyedMessage<String, String>>();
			}
			// TODO
//			String key = msg.split(",")[2];
			String key = msg.split(":")[0];
			msgList.add(new KeyedMessage<String, String>(topic, key, msg));
		}
		return msgList;
	}
}