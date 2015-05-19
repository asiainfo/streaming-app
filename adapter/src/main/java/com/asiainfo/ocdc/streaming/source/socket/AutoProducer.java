package com.asiainfo.ocdc.streaming.source.socket;

import com.asiainfo.ocdc.streaming.producer.SocketReceiveCountTasker;
import kafka.producer.KeyedMessage;

import java.io.DataInputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Timer;
import java.util.concurrent.ExecutorService;

import com.asiainfo.ocdc.streaming.producer.SendUtil;

/**
 * @author surq<br>
 * @since 2015.5.11<br>
 *        send kafka 消息<br>
 * @param msg
 */
public class AutoProducer {
	
	@SuppressWarnings("static-access")
	public static void main(String args[]) {
		
		SendUtil sendutil = new SendUtil();
		String print_flg = sendutil.prop.getProperty("socket.receiver.print").trim();
		boolean xdrprint = Boolean.parseBoolean(print_flg);
		String socketIp = sendutil.prop.getProperty("socket.server.ip").trim();
		String socketPort = sendutil.prop.getProperty("socket.server.port").trim();
		int port = Integer.parseInt(socketPort);
		// 根据socket.printCount.printflg判断是否启动打印单位时间内接收的数据条数
		HashMap<String,Long> countMap = new HashMap<String,Long>();
		String interval = sendutil.prop.getProperty("socket.printCount.interval").trim();
		long printInterval = Integer.parseInt(interval) * 1000;
		String printflg = sendutil.prop.getProperty("socket.printCount.printflg").trim();
		boolean blprintflg = Boolean.parseBoolean(printflg);
		SocketReceiveCountTasker SRCountTasker = null;
		if (blprintflg) {
			Timer timer = new Timer();
			SRCountTasker = new SocketReceiveCountTasker(countMap,printInterval);
			timer.schedule(SRCountTasker, 0, printInterval);
			SRCountTasker.countTool(countMap);
		}
		// 开启producer线程池执等待执行任务
		sendutil.runThreadPoolTask();
		ArrayList<KeyedMessage<String, String>> msgList = null;
		
		// 开启SocketHeartBeat
		ExecutorService executor = sendutil.executorPool;
		SocketHeartBeatTask socketHeartBeat = new SocketHeartBeatTask(socketIp, port);
		executor.submit(socketHeartBeat);
		try {
			while (true) {
				int msgLength = 0;
				int msgType = 0;
				int dataLen = 0;
				byte[] len = new byte[2];
				byte[] cmd = new byte[2];
				byte[] status = new byte[2];
				byte[] index = new byte[2];

				if (!socketHeartBeat.isInterrupted()){
					
					DataInputStream ds = socketHeartBeat.getDataInputStream();
					// flag
					ds.readByte();
					// format
					ds.readByte();
					// System.out.println(Integer.toHexString(format)+" ");
					ds.readFully(len);
					msgLength = socketUtil.bytesToInt(len);
					// System.out.println(Integer.toHexString(len[0])+" ");
					// System.out.println(Integer.toHexString(len[1])+" ");
					ds.readByte();
					ds.readFully(cmd);
					// System.out.println(Integer.toHexString(cmd[0])+" ");
					// System.out.println(Integer.toHexString(cmd[1])+" ");
					
					msgType = socketUtil.bytesToInt(cmd);
					if (msgType == 0x8003) {
						byte[] rep = new byte[msgLength - 2];
						ds.readFully(rep);
					} else if (msgType == 0x0002) {
						ds.readFully(status);
						// int msgStatus = socketUtil.bytesToInt(status);
						ds.readFully(index);
						// int msgIndex = socketUtil.bytesToInt(index);
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
							if (xdrprint) System.out.println("xdr:" + xdrsend);
					
							// 成功接收一条数据，统计值加1
							long receiveCount = countMap.get("thisCount");
							receiveCount += 1;
							countMap.put("thisCount", receiveCount);
							
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
				} else {
					Thread.currentThread().sleep(1000);
					if (blprintflg) {
						SRCountTasker.countTool(countMap);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}