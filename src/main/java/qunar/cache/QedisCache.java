package qunar.cache;

import java.util.HashMap;
import java.util.Map;

public class QedisCache {

    private final static Map<String, Qedis> qedisMap = new HashMap<String, Qedis>();

    public static Qedis getQedis(String namespace, String business, String cipher) {
        if ("".equals(namespace) || "".equals(business)) {
            return null;
        }
        synchronized (QedisCache.class) {
            String k = namespace + Constant.SEPARATOR + business;
            Qedis qedis = qedisMap.get(k);
            if (qedis == null) {
                qedis = new Qedis(namespace, business, cipher);
                if (qedis != null) {
                    qedisMap.put(k, qedis);
                }
            }
            return qedis;
        }

    }
}
