package com.asiainfo.ocdc.streaming.source.socket;
import kafka.producer.KeyedMessage;

import java.io.DataInputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;

import com.asiainfo.ocdc.streaming.producer.SendUtil;

/**
 * @author surq<br>
 * @since 2015.5.11<br>
 * send kafka 消息<br>
 * @param msg
 */
public class AutoProducer {
	@SuppressWarnings("resource")
    public static void main(String args[]) {

        try {
            // added by surq 2015.5.12 start------
    		SendUtil sendutil = new SendUtil();
    		String print_flg = sendutil.prop.getProperty("socket.receiver.print").trim();
    		boolean xdrprint = Boolean.parseBoolean(print_flg);
    		String socketIp = sendutil.prop.getProperty("socket.server.ip").trim();
    		String socketPort = sendutil.prop.getProperty("socket.server.port").trim();
    		int port=Integer.parseInt(socketPort);
    		sendutil.runThreadPoolTask();
    		ArrayList<KeyedMessage<String, String>> msgList = null;
            // added by surq 2015.5.12 end------
    	
			Socket socket = new Socket(socketIp, port);
    		OutputStream out = socket.getOutputStream();
            out.write(socketUtil.getMsgConnect());
            out.flush();

            DataInputStream ds = new DataInputStream(socket.getInputStream());
            // bindReq
            ds.readFully(new byte[11]);
            // 开启SocketHeartBeat
            new Thread(new SocketHeartBeatTask(out)).start();
            
            while (true) {
                int msgLength = 0;
                int msgType = 0;
                int dataLen = 0;
                byte[] len = new byte[2];
                byte[] cmd = new byte[2];
                byte[] status = new byte[2];
                byte[] index = new byte[2];
                // flag
                ds.readByte();
                // format
                ds.readByte();
                //System.out.println(Integer.toHexString(format)+" ");
                ds.readFully(len);
                msgLength = socketUtil.bytesToInt(len);
                //System.out.println(Integer.toHexString(len[0])+" ");
                //System.out.println(Integer.toHexString(len[1])+" ");
                ds.readByte();
                ds.readFully(cmd);
                //System.out.println(Integer.toHexString(cmd[0])+" ");
                //System.out.println(Integer.toHexString(cmd[1])+" ");

                msgType = socketUtil.bytesToInt(cmd);
                if(msgType == 0x8003){
                    byte[] rep = new byte[msgLength-2];
                    ds.readFully(rep);
                }else if(msgType == 0x0002){
                    ds.readFully(status);
//                    int msgStatus = socketUtil.bytesToInt(status);
                    ds.readFully(index);
//                    int msgIndex = socketUtil.bytesToInt(index);
                    len = null;
                    cmd = null;
                    status = null;
                    index = null;
                    int count=0;
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
                        String xdrsend = xdrs.substring(0,xdrs.length()-2);
                        // added by surq 2015.5.12 start------
                        if(xdrprint) System.out.println("xdr:" + xdrsend);
                        msgList = sendutil.packageMsg(xdrsend, msgList);
                        // added by surq 2015.5.12 end------
                        type = null;
                        varInfo = null;
                        xdr = null;
                        count = count+10+dataLen;
                        if(msgLength - 6 == count){
                            break;
                        }
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}