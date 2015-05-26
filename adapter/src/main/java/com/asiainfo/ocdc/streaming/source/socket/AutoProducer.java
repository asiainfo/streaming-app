package com.asiainfo.ocdc.streaming.source.socket;

import kafka.producer.KeyedMessage;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.concurrent.ExecutorService;

import org.apache.log4j.Logger;

import com.asiainfo.ocdc.streaming.producer.SendUtil;
import com.asiainfo.ocdc.streaming.producer.SocketReceiveCountTasker;

/**
 * @author surq<br>
 * @since 2015.5.11<br>
 *        send kafka 消息<br>
 * @param msg
 */
public class AutoProducer {
	
	private static Logger logger = Logger.getLogger(AutoProducer.class);  
	// 是否要逐条打印接收的socket数据flg
	private static boolean xdrprint = false;
	private static ArrayList<KeyedMessage<String, String>> msgList = null;
	private static SendUtil sendutil = null;
	private static boolean blprintflg = false;
	// 根据socket.printCount.printflg判断是否启动打印单位时间内接收的数据条数
	private static HashMap<String,Long> countMap = new HashMap<String,Long>();
	
	@SuppressWarnings("static-access")
	public static void main(String args[]) {
		
		sendutil = new SendUtil();
		// 配置文件参数解析
		// soceke server联接配置
		String socketIp = sendutil.prop.getProperty("socket.server.ip").trim();
		String socketPort = sendutil.prop.getProperty("socket.server.port").trim();
		int port = Integer.parseInt(socketPort);
		// 是否要逐条打印接收的socket数据
		String print_flg = sendutil.prop.getProperty("socket.receiver.print").trim();
		xdrprint = Boolean.parseBoolean(print_flg);
		// 是否要打印socket接收的速度
		String printflg = sendutil.prop.getProperty("socket.printCount.printflg").trim();
		blprintflg = Boolean.parseBoolean(printflg);
		// 打印socket接收速度周期单位（秒）
		long printInterval = 0l;
		if (blprintflg){
			String interval = sendutil.prop.getProperty("socket.printCount.interval").trim();
			printInterval = Integer.parseInt(interval) * 1000;
		}
		
		// 打印socket接收速度任务
		SocketReceiveCountTasker SRCountTasker = null;
		if (blprintflg) {
			Timer timer = new Timer();
			SRCountTasker = new SocketReceiveCountTasker(countMap,printInterval);
			SRCountTasker.init();
			timer.schedule(SRCountTasker, 10, printInterval);
		}
		// 开启producer线程池执等待执行任务
		sendutil.runThreadPoolTask();
		
		// 开启SocketHeartBeat 任务
		ExecutorService executor = sendutil.getExecutorPool();
		
		SocketHeartBeatTask socketHeartBeat = new SocketHeartBeatTask(socketIp, port);
		executor.submit(socketHeartBeat);
		try {
			while (true) {
				// socket server未中断联接并且能够获取socket数据流的情况下做如下处理：
				if (socketHeartBeat.isConnected() && !socketHeartBeat.isInterrupted()){
					// socket数据的抽取和封装
					msgAction(socketHeartBeat.getDataInputStream());
				} else {
					// socket server关闭联接等待1s钟继续试探获取联接
					Thread.currentThread().sleep(1000);
					if (blprintflg) {
						// socket传输速度变量重置
						SRCountTasker.init();
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 对socket流中的数据抽取和封装<br>
	 * @param ds
	 * @throws IOException 
	 */
	private static void msgAction(DataInputStream ds) throws IOException {
		int msgLength = 0;
		int msgType = 0;
		int dataLen = 0;
		byte[] len = new byte[2];
		byte[] cmd = new byte[2];
		byte[] status = new byte[2];
		byte[] index = new byte[2];
		ds.readByte();
		ds.readByte();
		ds.readFully(len);
		msgLength = socketUtil.bytesToInt(len);
		ds.readByte();
		ds.readFully(cmd);
		
		msgType = socketUtil.bytesToInt(cmd);
		if (msgType == 0x8003) {
			byte[] rep = new byte[msgLength - 2];
			ds.readFully(rep);
		} else if (msgType == 0x0002) {
			ds.readFully(status);
			ds.readFully(index);
			len = null;
			cmd = null;
			status = null;
			index = null;
			int count = 0;
			while (true) {
				byte[] type = new byte[2];
				byte[] varInfo = new byte[2];
				// flag
				ds.readByte();
				ds.readFully(type);
				ds.readFully(varInfo);
				// format
				ds.readByte();
				dataLen = ds.readInt();
				
				byte[] xdr = new byte[dataLen];
				ds.readFully(xdr);
				String xdrs = new String(xdr, "utf-8");
				String xdrsend = xdrs.substring(0, xdrs.length() - 2);
				// added by surq 2015.5.12 start------
				// 追加从socket接收数据的时间戳“yyyy-MM-dd HH:mm:ss:SSS”
				xdrsend = xdrsend + "," +SendUtil.timeFormat(System.currentTimeMillis());
				// 打印socket接收到的每条数据
				if (xdrprint) logger.info(xdrsend);
				// 打印socket接收数据的速度处理
				if (blprintflg){
					// 成功接收一条数据，统计值加1
					long receiveCount = countMap.get("thisCount");
					receiveCount += 1;
					if (receiveCount ==Long.MAX_VALUE){
						receiveCount = 0;
					}
					countMap.put("thisCount", receiveCount);
				}
				msgList = sendutil.packageMsg(xdrsend, msgList);
				// added by surq 2015.5.12 end------
				type = null;
				varInfo = null;
				xdr = null;
				count = count + 10 + dataLen;
				if (msgLength - 6 == count) {
					break;
				}
			}
		}
	}
}