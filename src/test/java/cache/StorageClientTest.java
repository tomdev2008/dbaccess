package cache;

import java.util.concurrent.TimeUnit;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.junit.Test;

import sirius.cache.Constant;
import sirius.cache.StorageClient;

public class StorageClientTest extends TestCase {

    private static Logger logger = Constant.logger;

    private static StorageClient sc = new StorageClient("proxy_preview_dba_rw", "test_biz");

    private static final String CACHE_KEY = "test_key";

    private static final String CACHE_VALUE = "test_value";

    static {
        BasicConfigurator.configure();
    }

    @Test
    public void testSwitchMaster() {
        int loop = 1000;
        while (loop-- > 0) {
            try {
                byte[] data = sc.get(CACHE_KEY);
                if (data != null && data.length != 0) {
                    String value = new String(data);
                    logger.debug(String.format("key:%s\ttarget:%s\treal:%s", CACHE_KEY,
                            CACHE_VALUE, value));
                }
            } catch (Exception e) {
                logger.error(e);
            }
            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
