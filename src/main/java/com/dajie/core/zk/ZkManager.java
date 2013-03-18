package com.dajie.core.zk;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZooKeeper;

/**
 * 
 * @author yong.li@dajie-inc.com
 * 
 */
public class ZkManager {

    private static Log logger = LogFactory.getLog(ZkManager.class);

    private ZooKeeper zk;

    private String zkAddress;

    private ZkWatcher watcher = new ZkWatcher(true);

    /** 异步通知任务 */
    private NotifyTask notifyTask = new NotifyTask();

    public void setZkClient(ZkClient zkClient) {
        notifyTask.setZkClient(zkClient);
    }

    public boolean initialize(String zkAddress) throws IOException {
        logger.info("ZkManager.initialize zkAddress " + zkAddress);
        this.zkAddress = zkAddress;
        try {
            synchronized (watcher) {
                zk = new ZooKeeper(zkAddress, 2000, watcher);
                watcher.wait();
            }
        } catch (IllegalArgumentException e) {
            logger.error("ZkManager.initialize " + e);
            throw e;
        } catch (IOException e) {
            logger.error("ZkManager.initialize " + e);
            throw e;
        } catch (Throwable e) {
            logger.error("ZkManager.initialize " + e);
            return false;
        }
        new Thread(notifyTask).start();
        return true;
    }

    private ZooKeeper getZk() {
        logger.info("ZkManager.getZk zkAddress " + zkAddress);
        return zk;
    }

    /**
     * 取数据
     * 
     * @param znodePath
     * @return
     */
    public String getData(String znodePath) {
        String version = "";
        synchronized (watcher) {
            try {
                byte[] rawData = getZk().getData(znodePath, true, null);
                if (rawData != null && rawData.length != 0) {
                    version = new String(rawData);
                }
            } catch (KeeperException e) {
                logger.error("ZkManager.getData() znodePath:" + znodePath + "\t" + e.toString());
            } catch (InterruptedException e) {
                logger.error("ZkManager.getData() znodePath:" + znodePath + "\t" + e.toString());
            }
        }
        return version;
    }

    /**
     * 取数据
     * 
     * @param znodePath
     * @return
     */
    public String getData(String znodePath, boolean watch) {
        String version = "";
        synchronized (watcher) {
            try {
                byte[] rawData = getZk().getData(znodePath, watch, null);
                if (rawData != null && rawData.length != 0) {
                    version = new String(rawData);
                }
            } catch (KeeperException e) {
                logger.error("ZkManager.getData() znodePath:" + znodePath + "\t" + e.toString());
            } catch (InterruptedException e) {
                logger.error("ZkManager.getData() znodePath:" + znodePath + "\t" + e.toString());
            }
        }
        return version;
    }

    /**
     * 为了重新注册watcher用
     * 
     * @param znodePath
     * @return
     */
    public void exists(String znodePath) {
        synchronized (watcher) {
            try {
                getZk().exists(znodePath, true);
            } catch (KeeperException e) {
                logger.error("ZkManager.exists() znodePath:" + znodePath + "\t" + e.toString());
            } catch (InterruptedException e) {
                logger.error("ZkManager.exists() znodePath:" + znodePath + "\t" + e.toString());
            }
        }
    }

    /**
     * 取子目录名
     * 
     * @param znodePath
     * @return
     */
    public List<String> getChildren(String znodePath) {
        List<String> list = new ArrayList<String>();
        synchronized (watcher) {
            try {
                list = getZk().getChildren(znodePath, false);
                exists(znodePath);
            } catch (KeeperException e) {
                logger.error("ZkManager.getChildren() znodePath:" + znodePath + "\t" + e.toString());
            } catch (InterruptedException e) {
                logger.error("ZkManager.getChildren() znodePath:" + znodePath + "\t" + e.toString());
            }
        }
        return list;
    }

    private class ZkWatcher implements Watcher {

        public ZkWatcher(boolean isFirst) {
            this.isFirst = isFirst;
        }

        private boolean isAlive = true;

        private boolean isFirst = true;

        public void process(WatchedEvent event) {
            // System.out.println("event:" + event);
            logger.info("ZkManager.ZkWatcher.process get notify : path = " + event.getPath()
                    + " type = " + event.getType() + " state = " + event.getState());
            if (!isAlive) {
                logger.error("ZkManager.ZkWatcher.process get notify : path = " + event.getPath()
                        + " type = " + event.getType() + " state = " + event.getState()
                        + " but watcher is already not alive!");
                return;
            }

            if (event.getType() == EventType.None) {
                switch (event.getState()) {
                    case SyncConnected:
                        synchronized (this) {
                            this.notifyAll();
                        }
                        if (!isFirst) { // 这点非常重要
                            // 在expire后且重新SyncConnected,需要去刷新全部配置!
                            // 感谢超哥
                            notifyTask.addMessage(ZkClient.UPDATE_ALL_NAMESPACE);
                        }
                        return;
                    case Disconnected:
                        return;
                    case Expired:
                        while (true) {
                            try {
                                try {
                                    zk.close();
                                } catch (InterruptedException e) {
                                    logger.error("ZkManager.ZkWatcher.process close " + e);
                                }
                                isAlive = false;
                                watcher = new ZkWatcher(false);
                                synchronized (watcher) {
                                    zk = new ZooKeeper(zkAddress, 2000, watcher);
                                    watcher.wait();
                                }
                                notifyTask.addMessage(ZkClient.UPDATE_ALL_NAMESPACE);
                                return;
                            } catch (Throwable e) {
                                logger.error("ZkManager.ZkWatcher.process reCreate " + e);
                                try {
                                    TimeUnit.SECONDS.sleep(1);
                                } catch (InterruptedException e1) {
                                    e1.printStackTrace();
                                    logger.error("ZkManager.ZkWatcher.process Thread.sleep " + e);
                                    continue;
                                }

                            }
                        }
                    default:
                        break;
                }
            } else if (event.getPath().length() != 0) {
                String znodePath = event.getPath();
                exists(znodePath); // 再注册一下
                notifyTask.addMessage(znodePath);
            }
        }
    }
}
