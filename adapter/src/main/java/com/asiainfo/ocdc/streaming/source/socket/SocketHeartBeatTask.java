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
public class SocketHeartBeatTask implements Callable<String> ,Thread.UncaughtExceptionHandler{

	private String socketIp;
	private int port;
	private boolean connected = false;
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
	public String call() {
		try{
			System.out.println("SocketHeartBeatTask start!! "+ Thread.currentThread().getId());
			Socket socket = getSocket();
			// 创建socket失败,退出心跳机制
			if (socket == null) {
				System.out.println("socket["+socketIp+":"+port +"] 创建失败！");
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
						System.out.println(SendUtil.timeFormat(System.currentTimeMillis()) + ":Socket 异常重新试探...");
						socket = getSocket();
						// 创建socket失败,退出心跳机制
						if (socket == null){
							System.out.println("socket["+socketIp+":"+port +"] 创建失败！");
							 continue;
						}
						// 发送请求信号，并返加socket的InputStream
						ds = socketUtil.sendHeadMsg(socket);
						// bindReq 去除请求信息头
						ds.readFully(new byte[11]);
						setDataInputStream(ds);
						setConnected(true);
					}
					OutputStream outputStream = socket.getOutputStream();
					outputStream.write(socketUtil.getHeartBeatInfo());
					outputStream.flush();
					Thread.sleep(15000);
					// 设定socket连通状态
					setInterrupted(false);
				} catch (SocketException e) {
					// 设定socket中断状态
					setInterrupted(false);
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
	
	/**
	 * 获取socket dataStream的联接状态<br>
	 */
	public Boolean isConnected() {
		return connected;
	}
	/**
	 * 获取socket dataStream数据时为true,否则为false<br>
	 * @param connected
	 */
	private void setConnected(Boolean connected) {
		this.connected = connected;
	}

	@Override
	public void uncaughtException(Thread t, Throwable e) {
		e.printStackTrace();
	}
}