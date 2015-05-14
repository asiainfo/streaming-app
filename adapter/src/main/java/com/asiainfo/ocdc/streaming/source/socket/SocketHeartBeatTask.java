package com.asiainfo.ocdc.streaming.source.socket;

import java.io.OutputStream;
import java.io.DataInputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.Callable;

import com.asiainfo.ocdc.streaming.producer.SendUtil;

/**
 * @author surq<br>
 * @since 2015.5.11<br>
 * 向socket的outputStream中发送心跳数据<br>
 * @param msg
 */
public class SocketHeartBeatTask implements Callable<String> {

	private String socketIp;
	private int port;
	// true: socket连通状态，false:socket中断状态
	private boolean interrupted = false;
	// 共享数据流
	public DataInputStream dataInputStream = null;
	
	/**
	 * constructor 
	 * @param socketIp
	 * @param port
	 */
	public SocketHeartBeatTask(String socketIp, int port) {
		this.socketIp = socketIp;
		this.port = port;
	}
	
	@Override
	public String call() throws Exception {
		Socket socket = getSocket(socketIp, port);
		// 创建socket失败,退出心跳机制
		if (socket == null) return "1";
		DataInputStream ds = new DataInputStream(socket.getInputStream());
		// bindReq 去除请求信息头
		ds.readFully(new byte[11]);
		setDataInputStream(ds);
		while (true) {
			try {
				if (isInterrupted()) {
					System.out.println(SendUtil.timeFormat(System.currentTimeMillis()) + ":Socket 异常重新试探...");
					socket = getSocket(socketIp, port);
					// 创建socket失败,退出心跳机制
					if (socket == null) break;
					ds = new DataInputStream(socket.getInputStream());
					// bindReq 去除请求信息头
					ds.readFully(new byte[11]);
					setDataInputStream(ds);
				}
				
				OutputStream outputStream = socket.getOutputStream();
				outputStream.write(getHeartBeatInfo());
				outputStream.flush();
				Thread.sleep(15000);
				// 设定socket连通状态
				setInterrupted(false);
			} catch (SocketException e) {
				// 设定socket中断状态
				setInterrupted(true);
				e.printStackTrace();
			}
		}
		return "1";
	}

	/**
	 * 创建socket并发送注册请求<br>
	 * 
	 * @param socketIp
	 * @param port
	 * @return
	 */
	private Socket getSocket(String socketIp, int port) {
		try {
			return new Socket(socketIp, port);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * 获取heartBeat 信息<br>
	 * 
	 * @return
	 */
	private byte[] getHeartBeatInfo() {
		byte[] msgConnect = new byte[11];
		msgConnect[0] = (byte) 0x9e;
		msgConnect[1] = (byte) 0x62;
		msgConnect[2] = (byte) 0x00;
		msgConnect[3] = (byte) 0x06;
		msgConnect[4] = (byte) 0x00;
		msgConnect[5] = (byte) 0x00;
		msgConnect[6] = (byte) 0x03;
		msgConnect[7] = (byte) 0x00;
		msgConnect[8] = (byte) 0x00;
		msgConnect[9] = (byte) 0x00;
		msgConnect[10] = (byte) 0x00;
		return msgConnect;
	}

	/**
	 * 取socket连接状态
	 */
	public boolean isInterrupted() {
		return interrupted;
	}

	/**
	 * 设定socket连接状态
	 * @param interrupted
	 */
	private void setInterrupted(boolean interrupted) {
		this.interrupted = interrupted;
	}
	
	/**
	 * 取数据流操作
	 * @param dataInputStream
	 */
	public DataInputStream getDataInputStream() {
		return dataInputStream;
	}

	/**
	 * 赋数据流操作
	 * @param dataInputStream
	 */
	private void setDataInputStream(DataInputStream dataInputStream) {
		this.dataInputStream = dataInputStream;
	}
}