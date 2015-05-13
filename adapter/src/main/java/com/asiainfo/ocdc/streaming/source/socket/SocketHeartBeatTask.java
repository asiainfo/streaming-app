package com.asiainfo.ocdc.streaming.source.socket;

import java.io.OutputStream;

/**
 * @author surq<br>
 * @since 2015.5.11<br>
 * 向socket的outputStream中发送心跳数据<br>
 * @param msg
 */
public class SocketHeartBeatTask implements Runnable {
	
	private OutputStream out = null;
	public SocketHeartBeatTask(OutputStream out) {
		this.out = out;
	}
	public void run() {
		try {
			while (true) {
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
				out.write(msgConnect);
				out.flush();
				Thread.sleep(15000);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}