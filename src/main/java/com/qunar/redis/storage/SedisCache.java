package com.qunar.redis.storage;

import java.util.HashMap;
import java.util.Map;

public class SedisCache {

    private final static Map<String, Sedis> qedisMap = new HashMap<String, Sedis>();

    public static Sedis getQedis(String namespace, String cipher) {
        if ("".equals(namespace)) {
            return null;
        }
        synchronized (SedisCache.class) {
            String k = namespace;
            Sedis qedis = qedisMap.get(k);
            if (qedis == null) {
                qedis = new Sedis(namespace, cipher);
                if (qedis != null) {
                    qedisMap.put(k, qedis);
                }
            }
            return qedis;
        }

    }
}
