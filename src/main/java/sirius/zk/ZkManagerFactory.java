package sirius.zk;

import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import sirius.dbresource.Constants;


/**
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class ZkManagerFactory {

    private static Logger logger = Constants.logger;

    private static ConcurrentHashMap<String, ZkManager> zkManagerMap = new ConcurrentHashMap<String, ZkManager>();

    public static synchronized ZkManager get(String zkAddress) {
        logger.info("ZkManagerFactory.get zkAddress " + zkAddress);
        ZkManager zm = zkManagerMap.get(zkAddress);
        if (null == zm) {
            logger.info("ZkManagerFactory.get new ZkManager for zkAddress " + zkAddress);
            zm = new ZkManager();
            try {
                if (zm.initialize(zkAddress)) {
                    zkManagerMap.put(zkAddress, zm);
                } else {
                    return null;
                }
            } catch (IOException e) {
                logger.error("ZkManagerFactory.get IOException " + e + " for zkAddress "
                        + zkAddress);
            }
        }
        return zm;
    }
}
