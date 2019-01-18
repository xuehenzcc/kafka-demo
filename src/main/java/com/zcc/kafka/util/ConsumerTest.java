/**
 * 版权：zcc
 * 作者：c0z00k8
 * @data 2018年11月5日
 */
package com.zcc.kafka.util;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.Test;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 * @author c0z00k8
 *
 */
public class ConsumerTest {

	/**
     * 消费者
     */
    @Test
    public void testConumser(){
        //
        Properties props = new Properties();
        props.put("zookeeper.connect", "192.168.88.137:2181");
        props.put("group.id", "g3");
        props.put("zookeeper.session.timeout.ms", "500");
        props.put("zookeeper.sync.time.ms", "250");
        props.put("auto.commit.interval.ms", "1000");
        props.put("auto.offset.reset", "smallest");
        //创建消费者配置对象
        ConsumerConfig config = new ConsumerConfig(props);
        //
        Map<String, Integer> map = new HashMap<String, Integer>();
        map.put("zcc", new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> msgs = Consumer.createJavaConsumerConnector(new ConsumerConfig(props)).createMessageStreams(map);
        List<KafkaStream<byte[], byte[]>> msgList = msgs.get("zcc");
        for(KafkaStream<byte[],byte[]> stream : msgList){
            ConsumerIterator<byte[],byte[]> it = stream.iterator();
            while(it.hasNext()){
                byte[] message = it.next().message();
                System.out.println(new String(message));
            }
        }
    }
    
}
