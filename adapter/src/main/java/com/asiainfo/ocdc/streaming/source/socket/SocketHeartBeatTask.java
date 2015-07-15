package com.asiainfo.ocdc.streaming.source.socket;

import java.io.OutputStream;
import java.io.DataInputStream;
import java.net.Socket;
import java.net.SocketException;
import java.util.concurrent.Callable;

import org.apache.log4j.Logger;

import com.asiainfo.ocdc.streaming.producer.SendUtil;

/**
 * @author 宿荣全<br>
 * @since 2015.5.11<br>
 * 向socket的outputStream中发送心跳数据<br>
 * @param msg
 */
public class SocketHeartBeatTask implements Callable<String> ,Thread.UncaughtExceptionHandler{

	private  Logger logger = Logger.getLogger(this.getClass());
	private String socketIp;
	private int port;
	private boolean interrupted = false;
	private boolean connected = false;

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
	public String call() {
		try{
			logger.info("SocketHeartBeatTask start! "+ Thread.currentThread().getId());
			Socket socket = getSocket();
			// 创建socket失败,退出心跳机制
			if (socket == null) {
				logger.error("socket["+socketIp+":"+port +"] 创建失败！");
				return "1";
			}
			// 发送SOCKET请求信号，并返加socket的InputStream
			DataInputStream ds = socketUtil.sendHeadMsg(socket);
			// bindReq 去除请求信息头
			ds.readFully( new byte[11]);
			setDataInputStream(ds);
			setConnected(true);
			while (true) {
				try {
					if (isInterrupted()) {
						logger.warn(SendUtil.timeFormat(System.currentTimeMillis()) + ":Socket 异常重新试探...");
						socket = getSocket();
						// 创建socket失败,退出心跳机制
						if (socket == null){
							logger.error("socket["+socketIp+":"+port +"] 创建失败！");
							 continue;
						}
						// 发送请求信号，并返加socket的InputStream
						ds = socketUtil.sendHeadMsg(socket);
						// bindReq 去除请求信息头
						ds.readFully(new byte[11]);
						setDataInputStream(ds);
					}
					OutputStream outputStream = socket.getOutputStream();
					outputStream.write(socketUtil.getHeartBeatInfo());
					outputStream.flush();
					Thread.sleep(15000);
					// 设定socket连通状态
					setInterrupted(false);
				} catch (SocketException e) {
					// 设定socket中断状态
					setInterrupted(true);
					socket.close();
					e.printStackTrace();
				}
			}
	
		}catch(Exception e){
			uncaughtException(Thread.currentThread(),e);
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
	private Socket getSocket() {
		try {
			return new Socket(socketIp, port);
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
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
	 * 第一次连接用，启动后让共享资源处于false,确定heartbeat连接成功后设为false<br>
	 * @return
	 */
	public boolean isConnected() {
		return connected;
	}

	public void setConnected(boolean connected) {
		this.connected = connected;
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

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		e.printStackTrace();
	}
}