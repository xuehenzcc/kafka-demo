/**
 * 版权：zcc
 * 作者：c0z00k8
 * @data 2018年9月20日
 */
package com.zcc.kafka.util.thread;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author c0z00k8
 *
 */
public class PushQueue {

	private ArrayBlockingQueue<String> queue=new ArrayBlockingQueue<String>(10);
	
	public void putMsg(String msg) throws InterruptedException{
		queue.put(msg);
	}
	
	public String takeMsg() throws InterruptedException{
		return queue.take();
	}
	
	private static class Singleton{
		private static PushQueue instance;
		static{
			instance=new PushQueue();
		}
		public static PushQueue getInstance(){
			return instance;
		}
	}
	
	public static PushQueue getInstance(){
		return Singleton.getInstance();
	}
}
