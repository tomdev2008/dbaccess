package sirius.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class CacheClientFactory {

    private static Logger logger = Constant.logger;

    private static Map<Entry, CacheClient> entryCacheClientMap = new HashMap<Entry, CacheClient>();

    public static CacheClient getCacheClient(final Entry entry) {
        if (entry == null) {
            logger.error("CacheClientFactory.getCacheClient() arg entry is null!");
            return null;
        }
        synchronized (CacheClientFactory.class) {
            CacheClient client = entryCacheClientMap.get(entry);
            if (client == null) {
                client = new CacheClient(entry.getNamespace(), entry.getBusiness());
                if (client != null) {
                    entryCacheClientMap.put(entry, client);
                }
            }
            return client;
        }
    }

}
