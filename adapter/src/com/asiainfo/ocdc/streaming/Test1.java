package com.asiainfo.ocdc.streaming;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.*;
import java.net.Socket;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by leo on 4/28/15.
 */
public class Test1 {

    static Integer oldct = 0;
    static Integer ct = 0;

    public static void main(String args[]) {

        try {
            String zk = "localhost:2181";
            String brokers = "localhost:9092,localhost:9093";
            String topic = "mc_signal3";
            int batch = 1000;
            final int thread = 10;

            Properties props = new Properties();
            props.put("zookeeper.connect", zk);
            props.put("metadata.broker.list", brokers);
            props.put("serializer.class", "kafka.serializer.StringEncoder");
            props.put("zookeeper.session.timeout.ms", "1000");
            props.put("zookeeper.sync.time.ms", "1000");
            /*props.put("auto.commit.interval.ms", "10000");
            props.put("request.timeout.ms", "60000");
            props.put("producer.type", "async");
            props.put("retry.backoff.ms", "3000");
            props.put("queue.buffering.max.ms", "5000");
            props.put("queue.buffering.max.messages", "50000");
            props.put("queue.enqueue.timeout.ms", "0");*/


            // Producer producer = new Producer<String, String>(new ProducerConfig(props));
            final HashMap<Integer, Producer<String, String>> producerMap = new HashMap<Integer, Producer<String, String>>();
            for (int t = 0; t < thread; t++) {
                producerMap.put(t, new Producer<String, String>(new ProducerConfig(props)));
            }
            ExecutorService executor = Executors.newFixedThreadPool(thread);
            List<KeyedMessage<String, String>> message = new ArrayList<KeyedMessage<String, String>>();
            int ret = 0;

            Timer timer = new Timer();
            timer.schedule(new SocketCt(), 2000, 60000);

            while (true) {
                String xdrsend = "11111111111111111111aaaaaaaaaaaaaaaaaaaaaaaaaaaaa%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%";
                // producer.send(new KeyedMessage<String, String>(topic, xdrsend));
                //AppendToFile(new String(xdr, "utf-8"),"/bigdata/interface/signal/ceshi.txt");

                String key = "key";
                message.add(new KeyedMessage<String, String>(topic, key, xdrsend));
                ret += 1;
                ct++;
                if (ret == batch) {
                    final List<KeyedMessage<String, String>> bm = message;
                    message = new ArrayList<KeyedMessage<String, String>>();
                    executor.submit(new Runnable() {
                        public void run() {
                            try {
                                Producer<String, String> producer = producerMap.get((int) (Thread.currentThread().getId() % thread));
                                if (producer != null) {
                                    producer.send(bm);
                                    System.out.println("Producer " + producer.toString() + " send message : ");
                                    System.out.println(bm.size());
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                    });
                    ret = 0;
                }
            }
        } catch (Exception e) {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }


    static class SocketCt extends TimerTask {
        @Override
        public void run() {
            System.out.println(ct - oldct);
            oldct = ct;
        }
    }

}
