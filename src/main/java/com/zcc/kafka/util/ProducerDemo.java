package com.zcc.kafka.util;
/**
 * 版权：zcc
 * 作者：c0z00k8
 * @data 2018年5月10日
 */

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;

/**
 * @author c0z00k8
 *
 */
public class ProducerDemo {
	static private final String TOPIC = "test";
	static private final String ZOOKEEPER = "192.168.88.133:2181";
	static private final String BROKER_LIST = "192.168.88.133:9092";
	static private final int PARTITIONS = 3;
	
	public static void main(String[] args) {
//		Map<String, String> map =new HashMap<String, String>();
//		map.put("k", "v");
//		System.out.println(map.get("k"));
		Producer<String, String> producer = initProducer();
		try {
			sendOne(producer, TOPIC);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private static Producer<String,String> initProducer(){
		Properties props=new Properties();
		props.put("metadata.broker.list",BROKER_LIST);
		props.put("zookeeper.connect", ZOOKEEPER);
		props.put("serializer.class",StringEncoder.class.getName());
//		props.put("partitioner.class",HashPartitioner.class.getName());
		props.put("producer.type","async");//异步模式
		props.put("batch.num.messages", "3");
	    props.put("queue.buffering.max.ms", "1000");
	    props.put("queue.buffering.max.messages", "1000");
	    props.put("queue.enqueue.timeout.ms", "2000");
	    ProducerConfig config=new ProducerConfig(props);
	    Producer<String,String> producer = new Producer<String,String>(config);
		return producer;
	}
	
	//发送消息
	public static void sendOne(Producer<String, String> producer, String topic) throws Exception{
		KeyedMessage<String,String> message1=new KeyedMessage<String, String>(topic,"21","test 21");
		producer.send(message1);
		Thread.sleep(5000);
		KeyedMessage<String,String> message2=new KeyedMessage<String, String>(topic,"21","test 22");
		producer.send(message2);
		Thread.sleep(5000);
		KeyedMessage<String,String> message3=new KeyedMessage<String, String>(topic,"21","test 23");
		producer.send(message3);
		Thread.sleep(5000);
		KeyedMessage<String,String> message4=new KeyedMessage<String, String>(topic,"21","test 24");
		producer.send(message4);
		Thread.sleep(5000);
		producer.close();
	}
	
}
