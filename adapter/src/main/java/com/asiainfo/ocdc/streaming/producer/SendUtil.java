package com.asiainfo.ocdc.streaming.producer;

import java.io.FileInputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import kafka.producer.KeyedMessage;

public class SendUtil {

	// 用户配置相关参数
	public Properties prop = null;
	public ExecutorService executorPool = Executors.newCachedThreadPool();
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
		// 默认1个producer
		int produceNum = 1;
		if (!"".equals(pNum.trim())) produceNum = Integer.parseInt(pNum.trim());
		for (int i = 0; i < produceNum; i++) executorPool.submit(new ProducerSendTask(lbk, prop));
	}
	
	/**
	 * @param msg：收到的单条消息<br>
	 * @param msgList:根据指定大小封装成message块包（押入队列）<br>
	 * @return msgList 返回message块<br>
	 */
	public ArrayList<KeyedMessage<String, String>> packageMsg (
			String msg, ArrayList<KeyedMessage<String, String>> msgList){
		
		String msgSize = prop.getProperty("kafka.producer.sendmsg.size");
		// 默认1条
		int msgListSize = 1;
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
			String key = msg.split(",")[2];
			msgList.add(new KeyedMessage<String, String>(topic, key, msg));
		}
		return msgList;
	}
	
	/**
	 * 日期格式化工具<br>
	 * @param time
	 * @return
	 */
	public static String timeFormat(long time) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss"); 
	return format.format(new Date(time));
	}
}