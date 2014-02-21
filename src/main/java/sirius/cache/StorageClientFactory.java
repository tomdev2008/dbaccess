package sirius.cache;

import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;

/**
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
