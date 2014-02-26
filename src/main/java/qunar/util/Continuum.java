package qunar.util;

import java.util.Comparator;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

/**
 * 一致性Hash实现
 * 
 * @author liyong19861014@gmail.com
 * 
 */
public class Continuum {

    private String name;

    /** 希望是字典序的列表，使用TreeMap */
    private TreeMap<String, Integer> desc_map; // desc to capacity

    private TreeSet<Point> points;

    private ReentrantReadWriteLock rwLock;

    public Continuum(String name) {
        this.name = name;
        desc_map = new TreeMap<String, Integer>();
        points = new TreeSet<Continuum.Point>();
        rwLock = new ReentrantReadWriteLock();
    }

    /**
     * hash type if uint32;
     * 
     * @param hash
     * 
     * @return
     */
    public String locate(long hash) {
        String desc = "";
        ReadLock rLock = rwLock.readLock();
        try {
            rLock.lock();
            Point p = points.higher(new Point(hash, ""));
            if (p != null) {
                desc = p.getDesc();
            } else {
                if (!points.isEmpty()) {
                    desc = points.first().getDesc();
                }
            }
        } finally {
            rLock.unlock();
        }
        return desc;
    }

    public void add(String desc, int capacity) {
        WriteLock wLock = rwLock.writeLock();
        try {
            wLock.lock();
            desc_map.put(desc, new Integer(capacity));
        } finally {
            wLock.unlock();
        }
    }

    public void remove(String desc) {
        WriteLock wLock = rwLock.writeLock();
        try {
            wLock.lock();
            desc_map.remove(desc);
        } finally {
            wLock.unlock();
        }
    }

    public void clear() {
        WriteLock wLock = rwLock.writeLock();
        try {
            wLock.lock();
            desc_map.clear();
        } finally {
            wLock.unlock();
        }
    }

    public int size() {
        ReadLock rLock = rwLock.readLock();
        try {
            rLock.lock();
            return points.size();
        } finally {
            rLock.unlock();
        }
    }

    public String getName() {
        return name;
    }

    public boolean rebuild() {
        ReadLock rLock = rwLock.readLock();
        long total_point = 0;
        rLock.lock();
        for (Entry<String, Integer> entry : desc_map.entrySet()) {
            total_point += entry.getValue().intValue();
        }
        if (total_point == 0) {
            rLock.unlock();
            return false;
        }
        TreeSet<Point> target = new TreeSet<Continuum.Point>();
        for (Entry<String, Integer> entry : desc_map.entrySet()) {
            for (int k = 0; k < entry.getValue().intValue(); ++k) {
                String desc = String.format("%s-%x", entry.getKey(), k);
                // long p = Continuum.hash(desc);
                long p = Continuum.hash(desc.getBytes(), desc.length());
                target.add(new Point(p, entry.getKey()));
            }
        }
        rLock.unlock();

        WriteLock wLock = rwLock.writeLock();
        try {
            wLock.lock();
            points.clear();
            points.addAll(target);
        } finally {
            wLock.unlock();
        }
        return true;
    }

    /**
     * 返回的是uint32
     * 
     * @param text
     * @return
     */
    public static long hash(String text) {
        long h = MurmurHash.hash(text.getBytes(), 0);
        if (h < 0) {
            h = h & MurmurHash.MASK;
        }
        return h;
    }

    public static long hash(byte[] data, int length) {
        long h = MurmurHash.hash(data, 0, length, 0);
        if (h < 0) {
            h = h & MurmurHash.MASK;
        }
        return h;
    }

    static class CompareByPoint implements Comparator<Point> {

        @Override
        public int compare(Point o1, Point o2) {
            return o1.getPoint() < o2.getPoint() ? -1 : 1;
        }
    }

    static class Point implements Comparable<Point> {

        private long point;

        private String desc;

        Point() {
            this.point = 0;
        }

        Point(long p, String desc) {
            this.point = p;
            this.desc = desc;
        }

        public long getPoint() {
            return point;
        }

        public String getDesc() {
            return desc;
        }

        public String toString() {
            StringBuffer sb = new StringBuffer();
            sb.append("{").append(point).append(":").append(desc).append("}");
            return sb.toString();
        }

        @Override
        public int compareTo(Point o) {
            if (point == o.getPoint()) {
                return 0;
            }
            if (point < o.getPoint()) {
                return -1;
            }
            return 1;
        }
    }
}
