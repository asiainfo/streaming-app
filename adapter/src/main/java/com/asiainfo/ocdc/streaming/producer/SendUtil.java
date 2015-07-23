package com.asiainfo.ocdc.streaming.producer;

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

import org.apache.log4j.Logger;

import com.asiainfo.ocdc.streaming.tasks.ProducerSendTask;

import kafka.producer.KeyedMessage;

/**
 * @author 宿荣全<br>
 * @since 2015.5.11<br>
 *        send kafka 消息<br>
 * @param msg
 */
public class SendUtil {
	public static ArrayList<String> eventList1 = new ArrayList<String>();
	public static ArrayList<String> eventList2 = new ArrayList<String>();
	static {
		eventList1.add("3");
		eventList1.add("5");
		eventList1.add("7");
		
		eventList2.add("8");
		eventList2.add("9");
		eventList2.add("10");
		eventList2.add("26");
	}

	private Logger logger = Logger.getLogger(this.getClass());
	// 用户配置相关参数
	public Properties prop = null;
	private ExecutorService executorPool = Executors.newCachedThreadPool();

	// lbk:message List队列
	LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> lbk = new LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>>();

	public SendUtil() {
		prop = getPropConfig();
		propeTest();
	}

	/**
	 * 参数配置文件 内容打印测试<br>
	 * 
	 * @return
	 */
	private void propeTest() {
		Properties prop = getPropConfig();
		Set<Entry<Object, Object>> entrySet = prop.entrySet();
		Iterator<Entry<Object, Object>> it = entrySet.iterator();
		logger.info("kafka-producer.properties 属性配置一览：");
		while (it.hasNext()) {
			Entry<Object, Object> kvEntry = it.next();
			logger.info(kvEntry.getKey() + " --> " + kvEntry.getValue());
		}
	}

	/**
	 * loade 参数配置文件<br>
	 * 
	 * @return Properties
	 */
	private Properties getPropConfig() {
		Properties prop = new Properties();
		try {
			// test 用
			// prop.load(this.getClass().getResourceAsStream("kafka-producer.properties"));
			prop.load(this.getClass().getClassLoader()
					.getResourceAsStream("kafka-producer.properties"));
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
		if (!"".equals(pNum.trim()))
			produceNum = Integer.parseInt(pNum.trim());
		for (int i = 0; i < produceNum; i++)
			executorPool.submit(new ProducerSendTask(lbk, prop));
	}

	/**
	 * @param msg
	 *            ：收到的单条消息<br>
	 * @param msgList
	 *            :根据指定大小封装成message块包（押入队列）<br>
	 * @return msgList 返回message块<br>
	 */
	public ArrayList<KeyedMessage<String, String>> packageMsg(String msg,
			ArrayList<KeyedMessage<String, String>> msgList) {

		String msgSize = prop.getProperty("kafka.producer.sendmsg.size");
		// 默认1条
		int msgListSize = 1;
		if (!"".equals(msgSize.trim()))
			msgListSize = Integer.parseInt(msgSize.trim());
		String topic = prop.getProperty("kafka.topic");

		if (!((msg.trim()).equals("") || msgListSize == 0)) {
			if (msgList != null && msgList.size() % msgListSize == 0
					&& msgList.size() / msgListSize == 1) {
				lbk.offer(msgList);
				msgList = null;
			}
			if (msgList == null) {
				msgList = new ArrayList<KeyedMessage<String, String>>();
			}

			// 根据业务需要改动 start
			// 过滤无效数据
			String[] dataSplit = msgFilter(msg);
			// 编辑kafka key
			String key = keyEditor(dataSplit);
			// 根据业务需要改动 end
			if (key != null) {
				msgList.add(new KeyedMessage<String, String>(topic, key, msg));
			}
		}
		return msgList;
	}

	/**
	 * 根据业务在此做一些数据的过滤，滤掉那些无效数据，并返回分隔后的数据字段列表
	 * 
	 * @param msg
	 * @return
	 */
	private String[] msgFilter(String msg) {
		String[] msgSplit = msg.split(",");

		if (msgSplit.length < 15) {
			logger.warn("信令长度小于15" + msgSplit.length + "：");
			return null;
		}
		String emptStr = "000000000000000";
		// 主叫imsi被叫imsi同时为空，此数据为无效数据
		if (emptStr.equals(msgSplit[6].trim())
				&& emptStr.equals(msgSplit[7].trim()))
			return null;
		if ("".equals(msgSplit[6].trim()) && "".equals(msgSplit[7].trim()))
			return null;
		return msgSplit;
	}

	/**
	 * 根据业务编辑imsi
	 * @param dataSplit
	 * @return
	 */
	private String keyEditor(String[] dataSplit) {
		// 数据无效被过滤掉了
		if (dataSplit == null) return null;

		String eventID = dataSplit[0].trim();
		String issmsalone = dataSplit[14].trim();

		String key =  dataSplit[6].trim();
		if (eventList1.contains(eventID)) {
			key = dataSplit[7].trim();
		} else if ("2".equals(issmsalone) && eventList2.contains(eventID)) {
			key = dataSplit[7].trim();
		}
		return key;
	}

	/**
	 * 日期格式化工具<br>
	 * 格式：yyyy-MM-dd HH:mm:ss<br>
	 * 
	 * @param time
	 * @return
	 */
	public static String timeFormat(long time) {
		SimpleDateFormat formatObj = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss:SSS");
		return formatObj.format(new Date(time));
	}

	/**
	 * 获取线程池
	 * 
	 * @return
	 */
	public ExecutorService getExecutorPool() {
		return executorPool;
	}
}