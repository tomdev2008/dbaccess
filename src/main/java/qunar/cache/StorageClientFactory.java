package qunar.cache;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import qunar.cache.exception.CacheException;

/**
 * 业务使用此类实例化StorageClient
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class StorageClientFactory {

    private static Logger logger = Constant.logger;

    private static Map<Entry, StorageClient> entryStorageClientMap = new HashMap<Entry, StorageClient>();

    public static StorageClient getStorageClient(final Entry entry, String cipher) {
        if (entry == null) {
            logger.error("StorageClientFactory.getStorageClient() arg entry is null!");
            return null;
        }
        synchronized (StorageClientFactory.class) {
            StorageClient client = entryStorageClientMap.get(entry);
            if (client == null) {
                client = new StorageClient(entry.getNamespace(), entry.getBusiness(), cipher);
                if (client != null) {
                    entryStorageClientMap.put(entry, client);
                }
            }
            return client;
        }
    }
}
