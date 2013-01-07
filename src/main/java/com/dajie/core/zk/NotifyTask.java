package com.dajie.core.zk;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class NotifyTask implements Runnable {

	private static Log logger = LogFactory.getLog(NotifyTask.class);

	private ZkClient zkClient;
	private BlockingDeque<String> messages;

	public NotifyTask() {
		this.zkClient = null;
		this.messages = new LinkedBlockingDeque<String>();
	}

	public void setZkClient(ZkClient zkClient) {
		this.zkClient = zkClient;
	}

	/**
	 * 异步实现此方法
	 * 
	 * @param znodePath
	 */
	public void addMessage(String znodePath) {
		try {
			messages.put(znodePath);
		} catch (InterruptedException e) {
			logger.error(e);
		}
	}

	public void run() {
		while (true) {
			String znodePath = null;
			try {
				znodePath = messages.take();
			} catch (InterruptedException e) {
				logger.error(e);
				continue;
			}

			if (zkClient == null) {
				logger.error("zkClient == null");
				continue;
			}
			final int loop = 3; // 最多尝试3次update,如果全失败，则放回messages队列中
			boolean result = false;
			for (int i = 0; i < loop; ++i) {
				result = zkClient.update(znodePath);
				if (result) {
					break;
				} else {
					try {
						TimeUnit.MILLISECONDS.sleep(300);
					} catch (InterruptedException e) {
						logger.error(e);
					}
				}
			}
			if (!result) {
				if (!messages.contains(znodePath)) {
					try {
						messages.put(znodePath); // 马上添加到队列尾部
					} catch (InterruptedException e) {
						logger.error(e);
					}
				}
			}
		}

	}
}
