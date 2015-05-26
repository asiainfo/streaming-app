package com.asiainfo.ocdc.streaming.tasks;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.ArrayList;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 * @author 宿荣全<br>
 * @since 2015.5.11<br>
 * send kafka 消息<br>
 * @param msg
 */
public class ProducerSendTask implements Callable<String> ,Thread.UncaughtExceptionHandler{
	
	private  Logger logger = Logger.getLogger(this.getClass());
	private LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> lbk = null;
	Properties props = null;
	
	public ProducerSendTask(
			LinkedBlockingQueue<ArrayList<KeyedMessage<String, String>>> lbk, Properties props) {
		this.lbk = lbk;
		this.props = props;
	}

	public String call() {
		
		try {
			logger.info("ProducerSendTask start! "+ Thread.currentThread().getId());
			// 设置配置属性
			ProducerConfig config = new ProducerConfig(props);
			// 创建producer
			Producer<String, String> producer = new Producer<String, String>(config);
			while (true) {
				ArrayList<KeyedMessage<String, String>> msgList = lbk.take();
				producer.send(msgList);
			}
		}catch(Exception e){
			uncaughtException(Thread.currentThread(),e);
		}
		return "";
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		e.printStackTrace();
	}
}