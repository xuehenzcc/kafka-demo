/**
 * 版权：zcc
 * 作者：c0z00k8
 * @data 2018年11月5日
 */
package com.zcc.kafka.util;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;

import com.zcc.kafka.util.thread.PushThread;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;



/**
 * @author c0z00k8
 *
 */
@SuppressWarnings("deprecation")
public class ProduceTest {

	public static void main(String[] args) {
		new Thread(new PushThread()).start();
		new Thread(new PushThread()).start();
		new Thread(new PushThread()).start();
		new Thread(new PushThread()).start();
		new Thread(new PushThread()).start();
		System.out.println("!````````start--time:"+new Date());
		for (int i = 1; i <= 1000000; i++) {
			try {
				String mm="{\"name\":\"name-阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name2\":\"name2-阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name3\":\"name3-阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name4\":\"name4-阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name5\":\"name5-阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\"}";
				com.zcc.kafka.util.thread.PushQueue.getInstance().putMsg(mm);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		System.out.println("!````````end--time:"+new Date());
	}
	
		
	@Test
	public void testSend(){
		Properties props = new Properties();
		//broker列表
//		props.put("metadata.broker.list", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
		//串行化
//		props.put("serializer.class", "kafka.serializer.StringEncoder");
		//
//		props.put("request.required.acks", "0");
		System.out.println("!````````start--time:"+new Date());
		 props.put("bootstrap.servers", "hadoop1:9092,hadoop2:9092,hadoop3:9092");
		 props.put("acks", "all");
		 props.put("retries", 100);
		 props.put("batch.size", 16384);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 KafkaProducer<String, String> producer = new KafkaProducer<String,String>(props);
		 String mm="";
		 for (int i = 1; i <= 1000000; i++){
			  mm="{\"id\":"+i+",\"number\":"+i+",\"name1\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name10\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name12\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name13\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name14\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name15\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name16\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name17\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name18\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\","
						+ "\"name9\":\"阿道夫阿斯顿发的发送到发送到发送到发达大是大非aaaaaaaaaaaaaa\"}";
//		      producer.send(new ProducerRecord<String, String>("channelzcc", Integer.toString(i), mm));
		      ProducerRecord<String, String> myRecord = new ProducerRecord<String, String>("channelzcc", Integer.toString(i), mm);
		      producer.send(myRecord,
		               new Callback() {
		                   public void onCompletion(RecordMetadata metadata, Exception e) {
		                       if(e != null) {
		                          e.printStackTrace();
		                       } else {
//		                          System.out.println("The offset of the record we just sent is: " + metadata.offset());
		                       }
		                   }
		               });
		 }
		 producer.flush();
		 producer.close();
		 
		//创建生产者配置对象
//		ProducerConfig config = new ProducerConfig(props);
//
//		//创建生产者
//		Producer<String, String> producer = new Producer<String, String>(config);
//		System.out.println("!````````start--time:"+new Date());
//		for (int i = 1; i <= 10; i++) {
//			KeyedMessage<String, String> msg = new KeyedMessage<String, String>("zcc",i+"" ,"hello world zcc2");
//			producer.send(msg);
//		}
		System.out.println("send over!````````endtime:"+new Date());
//		producer.close();
	}
	
	
	
	
}
