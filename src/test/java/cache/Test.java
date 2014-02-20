package cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import sirius.cache.CacheAccess;
import sirius.cache.StorageClient;
import sirius.cache.exception.CacheException;

public class Test {

    private static Logger logger = Logger.getLogger("storageClient_test_logger");

    private static final String CACHE_KEY = "test_key";

    private static final String CACHE_VALUE = "test_value";

    public static void main(String[] args) {
        //        BasicConfigurator.configure();
        DOMConfigurator.configure("log4j.xml");
        //        testBatchWrite();
        testBatchReadMaster();
    }

    static void testBatchReadMaster() {
        long begin = System.currentTimeMillis();

        StorageClient storageClient = new StorageClient("dba_test_rw", "dba_test");
        int total = 10 * 10000;
        int succeed = 0;
        int failed = 0;
        for (int i = 0; i < total; ++i) {
            try {
                String key = String.valueOf(i);
                byte[] data = storageClient.get(key);
                if (data == null || data.length == 0) {
                    continue;
                }
                String value = new String(data);
                if (key.equals(value)) {
                    ++succeed;
                } else {
                    ++failed;
                }
                if (i % 100 == 0) {
                    logger.debug(String.format("(total, succeed, failed, current)=(%d,%d,%d,%d)",
                            total, succeed, failed, i));
                }
            } catch (CacheException e) {
                e.printStackTrace();
            }

            //            try {
            //                TimeUnit.MILLISECONDS.sleep(5);
            //            } catch (InterruptedException e) {
            //                logger.error("", e);
            //            }

        }
        long end = System.currentTimeMillis();
        long timeCost = end - begin;
        String info = String
                .format("testBatchReadMaster done TimeCost: %d ms (total, succeed, percent)=(%d, %d, %f%%)",
                        timeCost, total, succeed, (float) succeed / (float) total * 100);
        logger.debug(info);
    }

    static void testBatchWrite() {
        long begin = System.currentTimeMillis();
        StorageClient storageClient = new StorageClient("dba_test_rw", "dba_test");
        int threadSize = 10;
        int start = 1;
        int batchSize = 100 * 100;
        List<Thread> threads = new ArrayList<Thread>();
        for (int i = 0; i < threadSize; ++i) {
            Thread thread = new Thread(new BatchWriteWorker(storageClient, start, batchSize));
            thread.setName("BatchWriteWorker_" + i);
            start += batchSize;
            threads.add(thread);
        }

        for (Thread thread : threads) {
            thread.start();
        }

        try {
            TimeUnit.SECONDS.sleep(1);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                logger.error("", e);
            }
        }
        long end = System.currentTimeMillis();
        long timeCost = end - begin;
        int total = threadSize * batchSize;
        logger.debug(String.format(
                "testBatchWrite() Total_OP: %d, TimeCost: %d ms, ThreadCount: %d", total, timeCost,
                threadSize));
    }

    static void testSwitchMaster() {
        StorageClient storageClient = new StorageClient("proxy_preview_dba_rw", "test_biz");
        boolean res;
        try {
            res = storageClient.set(CACHE_KEY, CACHE_VALUE.getBytes(), 60 * 60);
            logger.debug("res:" + res);
        } catch (CacheException e1) {
            logger.error("", e1);
        }

        List<Thread> threads = new ArrayList<Thread>();
        int threadSize = 100;
        for (int i = 0; i < threadSize; ++i) {
            threads.add(new Thread(new Worker(storageClient)));
        }

        for (Thread thread : threads) {
            thread.start();
        }

        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        try {
            TimeUnit.SECONDS.sleep(3);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static class BatchWriteWorker implements Runnable {

        static final int PAGE_SIZE = 1000;

        private int offset;

        private int size;

        private CacheAccess ca;

        public BatchWriteWorker(CacheAccess ca, int offset, int size) {
            this.ca = ca;
            this.offset = offset;
            this.size = size;
        }

        @Override
        public void run() {
            logger.debug(String.format("%s starts, (offset, size)=(%d, %d)", Thread.currentThread()
                    .getName(), offset, size));
            int failed = 0;
            int succeed = 0;
            for (int i = 0; i < size; ++i) {
                String key = String.valueOf((i + offset));
                String value = key;
                boolean result = false;
                try {
                    result = ca.set(key, value.getBytes(), 60 * 60);
                    if (result) {
                        ++succeed;
                    } else {
                        ++failed;
                    }
                } catch (CacheException e) {
                    logger.error("", e);
                }

                if (i % PAGE_SIZE == 0) {
                    logger.debug(String.format("%s (offset, size, current)=(%d, %d, %d)", Thread
                            .currentThread().getName(), offset, size, i + offset));
                }
            }

            float percent = (float) succeed / (float) size;
            String info = String.format(
                    "%s Done, (offset, size, succeed, failed, percent)=(%d, %d, %d, %d, %.2f %%)",
                    Thread.currentThread().getName(), offset, size, succeed, failed, percent * 100);
            logger.debug(info);
        }
    }

    static class Worker implements Runnable {

        private CacheAccess ca;

        Worker(CacheAccess ca) {
            this.ca = ca;
        }

        @Override
        public void run() {
            while (true) {
                byte[] data;
                try {
                    //                    data = ca.get(CACHE_KEY);
                    //                    if (data != null && data.length != 0) {
                    //                        String value = new String(data);
                    //                        logger.debug(String.format("key:%s\ttarget:%s\treal:%s", CACHE_KEY,
                    //                                CACHE_VALUE, value));
                    //                        try {
                    //                            TimeUnit.MILLISECONDS.sleep(20);
                    //                        } catch (Exception e) {
                    //                            logger.error(e);
                    //                        }
                    //                    }
                    boolean result = ca.set(CACHE_KEY, CACHE_VALUE.getBytes(), 100);
                    logger.debug(String.format("set(%s, %s) result:%s)", CACHE_KEY, CACHE_VALUE,
                            result));
                } catch (CacheException e1) {
                    logger.error("", e1);
                }

            }
        }
    }

}
