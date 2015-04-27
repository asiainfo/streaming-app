package com.asiainfo.ocdc.streaming.tool;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.Timer;
import java.util.TimerTask;

public class TalkClient {

    static Socket socket = null;
    static OutputStream out = null;
    static InputStream in = null;
    static DataInputStream ds = null;
    static Integer oldct = 0;
    static Integer ct = 0;

    public static int toInt(byte[] bArr) {
        int iout = 0;
        byte bLoop;
        for (int i = 0; i < 2; i++) {
            bLoop = bArr[i];
            iout += (bLoop & 0xFF) << (8 * i);
        }
        return iout;
    }

    public static String negativeIntToHex(int i) {
        // 负整数时，前面输入了多余的 FF ，没有去掉前面多余的 FF，按并双字节形式输出
        return Integer.toHexString(i);// FFFFFFFE
    }

    public static int HexToNegativeInt(String hex) {
        return Integer.valueOf(hex, 16);
    }

    private static void AppendToFile(String c, String fn) {
        try {
            // 打开一个随机访问文件流，按读写方式
            RandomAccessFile randomFile = new RandomAccessFile(fn, "rw");
            // 文件长度，字节数
            long fileLength = randomFile.length();
            // 将写文件指针移到文件尾。
            randomFile.seek(fileLength);
            randomFile.writeBytes(c);
            randomFile.close();
            // OutputStreamWriter osw = new OutputStreamWriter(new
            // FileOutputStream(
            // fn),"UTF-8" );
            //
            // osw.write(c);
            // osw.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static int bytesToInt(byte[] bytes) {
        int num = bytes[1] & 0xFF;
        num |= ((bytes[0] << 8) & 0xFF00);
        return num;
    }

    public static void main(String args[]) {

        if (args.length < 9) {
            System.out.println("Usage: ./TalkClient zk brokers topic batchnum thread Shost Sport ctprint xdrprint");
            System.exit(0);
        }

        try {
            String zk = args[0];
            String brokers = args[1];
            String topic = args[2];
            int batch = new Integer(args[3]);
            final int thread = new Integer(args[4]);
            String ctprint = args[7];
            String xdrprint = args[8];

            Properties props = new Properties();
            props.put("zookeeper.connect", zk);
            props.put("group.id", "test");
            props.put("metadata.broker.list", brokers);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("zookeeper.session.timeout.ms", "400");
            props.put("zookeeper.sync.time.ms", "200");
            props.put("auto.commit.interval.ms", "1000");
            // Producer producer = new Producer<String, String>(new ProducerConfig(props));
            final HashMap<Integer, Producer<String, String>> producerMap = new HashMap<Integer, Producer<String, String>>();
            for (int t = 0; t < thread; t++) {
                producerMap.put(t, new Producer<String, String>(new ProducerConfig(props)));
            }
            ExecutorService executor = Executors.newFixedThreadPool(thread);
            List<KeyedMessage<String, String>> message = new ArrayList<KeyedMessage<String, String>>();
            int ret = 0;
            ;

            if (ctprint.equals("yes")) {
                Timer timer = new Timer();
                timer.schedule(new SocketCt(), 2000, 60000);
            }
            socket = new Socket(args[5], new Integer(args[6]));
            out = socket.getOutputStream();

            byte[] msgConnect = new byte[47];
            msgConnect[0] = (byte) 0x9e;
            msgConnect[1] = (byte) 0x62;
            msgConnect[2] = (byte) 0x00;
            msgConnect[3] = (byte) 0x2a;
            msgConnect[4] = (byte) 0x00;
            msgConnect[5] = (byte) 0x00;
            msgConnect[6] = (byte) 0x01;
            msgConnect[7] = (byte) 0x00;
            msgConnect[8] = (byte) 0x00;
            msgConnect[9] = (byte) 0x00;
            msgConnect[10] = (byte) 0x00;
            msgConnect[11] = (byte) 0x7a;
            msgConnect[12] = (byte) 0x78;
            msgConnect[13] = (byte) 0x74;
            msgConnect[14] = (byte) 0x32;
            msgConnect[15] = (byte) 0x30;
            msgConnect[16] = (byte) 0x30;
            msgConnect[17] = (byte) 0x30;
            msgConnect[18] = (byte) 0x00;
            msgConnect[19] = (byte) 0x00;
            msgConnect[20] = (byte) 0x00;
            msgConnect[21] = (byte) 0x00;
            msgConnect[22] = (byte) 0x00;
            msgConnect[23] = (byte) 0x00;
            msgConnect[24] = (byte) 0x00;
            msgConnect[25] = (byte) 0x00;
            msgConnect[26] = (byte) 0x00;
            msgConnect[27] = (byte) 0x7a;
            msgConnect[28] = (byte) 0x78;
            msgConnect[29] = (byte) 0x74;
            msgConnect[30] = (byte) 0x32;
            msgConnect[31] = (byte) 0x30;
            msgConnect[32] = (byte) 0x30;
            msgConnect[33] = (byte) 0x30;
            msgConnect[34] = (byte) 0x00;
            msgConnect[35] = (byte) 0x00;
            msgConnect[36] = (byte) 0x00;
            msgConnect[37] = (byte) 0x00;
            msgConnect[38] = (byte) 0x00;
            msgConnect[39] = (byte) 0x00;
            msgConnect[40] = (byte) 0x00;
            msgConnect[41] = (byte) 0x00;
            msgConnect[42] = (byte) 0x00;
            msgConnect[43] = (byte) 0x00;
            msgConnect[44] = (byte) 0x01;
            msgConnect[45] = (byte) 0x42;
            msgConnect[46] = (byte) 0x69;

            out.write(msgConnect);
            out.flush();

            in = socket.getInputStream();
            ds = new DataInputStream(in);
            byte[] bindReq = new byte[11];
            ds.readFully(bindReq);
            bindReq = null;

            ExecutorService exec = Executors.newCachedThreadPool();
            exec.execute(createTask(1));
            while (true) {
                byte flag;
                byte format;
                int msgLength = 0;
                int msgType = 0;
                int msgIndex = 0;
                int msgStatus = 0;
                int dataLen = 0;
                byte[] len = new byte[2];
                byte[] cmd = new byte[2];
                byte[] status = new byte[2];
                byte[] index = new byte[2];

                flag = ds.readByte();
                //System.out.println(Integer.toHexString(flag)+" ");
                format = ds.readByte();
                //System.out.println(Integer.toHexString(format)+" ");
                ds.readFully(len);
                msgLength = bytesToInt(len);
                //System.out.println(Integer.toHexString(len[0])+" ");
                //System.out.println(Integer.toHexString(len[1])+" ");
                ds.readByte();
                ds.readFully(cmd);
                //System.out.println(Integer.toHexString(cmd[0])+" ");
                //System.out.println(Integer.toHexString(cmd[1])+" ");

                msgType = bytesToInt(cmd);
                if (msgType == 0x8003) {
                    byte[] rep = new byte[msgLength - 2];
                    ds.readFully(rep);
                } else if (msgType == 0x0002) {
                    ds.readFully(status);
                    msgStatus = bytesToInt(status);
                    ds.readFully(index);
                    msgIndex = bytesToInt(index);
                    len = null;
                    cmd = null;
                    status = null;
                    index = null;
                    int count = 0;
                    while (true) {
                        byte[] type = new byte[2];
                        byte[] varInfo = new byte[2];
                        flag = ds.readByte();
                        ds.readFully(type);
                        ds.readFully(varInfo);
                        format = ds.readByte();
                        dataLen = ds.readInt();

                        byte[] xdr = new byte[dataLen];
                        ds.readFully(xdr);
                        String xdrs = new String(xdr, "utf-8");
                        String xdrsend = xdrs.substring(0, xdrs.length() - 2);
                        if (xdrprint.equals("yes")) {
                            System.out.println("xdr:" + xdrsend);
                        }
                        // producer.send(new KeyedMessage<String, String>(topic, xdrsend));
                        //AppendToFile(new String(xdr, "utf-8"),"/bigdata/interface/signal/ceshi.txt");

                        String key = xdrsend.split(",")[2];
                        message.add(new KeyedMessage<String, String>(topic, key, xdrsend));
                        ret += 1;
                        ct++;
                        if (ret == batch) {
                            final List<KeyedMessage<String, String>> bm = message;
                            executor.submit(new Runnable() {
                                public void run() {
                                    try {
                                        Producer<String, String> producer = producerMap.get((int) (Thread.currentThread().getId() % thread));
                                        if (producer != null) {
                                            producer.send(bm);
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            });
                            message = new ArrayList<KeyedMessage<String, String>>();
                            ret = 0;
                        }

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
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private static Runnable createTask(final int taskID) {
        return new Runnable() {

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
        };
    }

    static class SocketCt extends TimerTask {
        @Override
        public void run() {
            System.out.println(ct - oldct);
            oldct = ct;
        }
    }
}