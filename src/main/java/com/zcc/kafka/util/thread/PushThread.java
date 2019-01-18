/**
 * 版权：zcc
 * 作者：c0z00k8
 * @data 2018年9月20日
 */
package com.zcc.kafka.util.thread;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * @author c0z00k8
 *
 */
public class PushThread implements Runnable {

//	@Override
	public void run() {
		PushQueue queue=PushQueue.getInstance();
		Properties props = new Properties();
//		//broker列表
//		props.put("metadata.broker.list", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
//		//串行化
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
//		//
//		props.put("request.required.acks", "0");

		props.put("bootstrap.servers", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
		 props.put("acks", "1");
		 //限制客户端在单个连接上能够发送的未响应请求的个数。设置此值是1表示kafka broker在响应请求之前client不能再向同一个broker发送请求。注意：设置此参数是为了避免消息乱序
		 props.put("max.in.flight.requests.per.connection", "1");
		 props.put("min.insync.replicas", "1");//写入到该值的副本才算成功
		 props.put("unclean.leader.election.enable", "false");//关闭unclean leader选举，即不允许非ISR中的副本被选举为leader，以避免数据丢失
		 props.put("enable.auto.commit", "false");//禁止自动提交
		 
		 props.put("max.block.ms", 600000);
		 props.put("request.timeout.ms", 600000);
		 props.put("send.buffer.bytes", 1024000);
		 props.put("retries", 100);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 335544320);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
//		 for (int i = 1; i <= 1000000; i++)
//		     producer.send(new ProducerRecord<String, String>("zcc", Integer.toString(i), "hello word"
//						+ "asd测试测试测试测试测试测试测试测试测试测试测试测试测试测试测试测试测试测试"
//						+ "测试测试测试测试测试测试测试测试测试测试测试测试测试测试测试测试测试测试 zcc"+i));
		 
		//创建生产者配置对象
//		ProducerConfig config = new ProducerConfig(props);

		//创建生产者
//		Producer<String, String> producer = new Producer<String, String>(config);
//		System.out.println("!````````start--time:"+new Date());
//		for (int i = 1; i <= 100000; i++) {
			
//		}
//		System.out.println("send over!````````endtime:"+new Date());
		int i=0;
		while (true) {
			i++;
			try {
				String text=queue.takeMsg();
//				sender.send("zcc-test", text);
//				KeyedMessage<String, String> msg = new KeyedMessage<String, String>("zcc",i+"" ,text);
//				producer.send(msg);
//				producer.send(new ProducerRecord<String, String>("zcc", Integer.toString(i), text));
//				amqpTemplate.convertAndSend(text); 
				ProducerRecord<String, String> myRecord = new ProducerRecord<String, String>("zcc", Integer.toString(i), text);
			      producer.send(myRecord,
			               new Callback() {
			                   public void onCompletion(RecordMetadata metadata, Exception e) {
			                       if(e != null) {
			                          e.printStackTrace();
			                       } else {
//			                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
			                       }
			                   }
			               });
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
			
	}
	
}
