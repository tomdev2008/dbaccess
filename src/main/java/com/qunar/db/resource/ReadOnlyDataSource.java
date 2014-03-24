package com.qunar.db.resource;

import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.qunar.db.util.DbLogger;
import com.qunar.redis.storage.Constant;
import com.qunar.zkclient.ZkClient;
import com.qunar.zkclient.exception.ZkException;

public class ReadOnlyDataSource extends BasicDataSource implements Closeable {

    private static final Logger logger = LoggerFactory.getLogger(DbLogger.class);

    private final String namespace;

    private final String cipher;

    private Random rand;

    private ReentrantReadWriteLock rwlock;

    private List<Entry> readEntries;

    private List<Entry> writeEntries;

    private ZkClient zkClient;

    private boolean closed;

    public ReadOnlyDataSource(String namespace, String cipher) {
        super(ConnectionAccess.DATABASE_DESC_PREFIX + namespace);
        this.namespace = namespace;
        this.cipher = cipher;
        this.rand = new Random(System.currentTimeMillis());
        this.rwlock = new ReentrantReadWriteLock();
        this.readEntries = new LinkedList<Entry>();
        this.writeEntries = new LinkedList<Entry>();
        this.closed = false;
        this.zkClient = ZkClient.getInstance(Constant.DEFAULT_ZK_ADDRESS);
        zkClient.addNodeDataListener(this);
        try {
            zkClient.exist(getNodePath(), true);
        } catch (ZkException e) {
            logger.error(desc(e.getMessage()), e);
        }
        update(getNodePath());

    }

    protected String desc(String msg) {
        StringBuffer sb = new StringBuffer();
        sb.append("[ReadOnlyDataSource, ns:").append(namespace);
        if (msg != null) {
            sb.append(", msg:").append(msg);
        }
        sb.append("]");
        return sb.toString();
    }

    @Override
    public Connection getConnection() throws SQLException {
        try {
            rwlock.readLock().lock();
            //
            if (!readEntries.isEmpty()) {
                int readSize = readEntries.size();
                logger.debug("readSize:" + readSize);
                if (readSize == 1) {
                    return readEntries.get(0).getConnection();
                } else {
                    int index = rand.nextInt(readSize);
                    return readEntries.get(index).getConnection();
                }
            } else if (!writeEntries.isEmpty()) { // readEntries is empty, get conn from writeEntries!
                int writeSize = writeEntries.size();
                logger.debug("writeSize:" + writeSize);
                if (writeSize == 1) {
                    return writeEntries.get(0).getConnection();
                } else {
                    int index = rand.nextInt(writeSize);
                    return writeEntries.get(index).getConnection();
                }
            } else { //both readEntries and writeEntries are empty
                return null;
            }

        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    public boolean update(String value) {
        if (closed) {
            return true;
        }
        try {
            rwlock.writeLock().lock();
            if (zkClient == null) {
                logger.error(desc("zkClient is null!"));
                return true;
            }
            List<String> nodes = null;
            try {
                nodes = zkClient.getChildren(getNodePath(), false);
                logger.debug(desc("nodes:" + nodes));
            } catch (Exception e) {
                logger.error(desc(e.getMessage()), e);
            }
            if (nodes == null) {
                logger.error(desc("getChildren return null!"));
                return true;
            }

            List<Entry> newReadEntries = new ArrayList<Entry>();
            List<Entry> newWriteEntries = new ArrayList<Entry>();
            for (String node : nodes) {
                // <host>:<port>:<user>:<pwd>:<db>:<flag>:<pattern_str>:<core>:<max>
                //      0:     1:     2:    3:   4:     5:            6:     7:    8
                String[] sp = node.split(Constant.SEPARATOR);
                String host = sp[0];
                int port = 3306; //default 3306
                try {
                    port = Integer.parseInt(sp[1]);
                } catch (Exception e) {
                    logger.error(desc(e.getMessage()), e);
                }
                String user = sp[2];
                String pwd = cipher;
                String db = sp[4];
                String flag = sp[5];
                String patternStr = sp[6];
                int core = 2; // default 2
                try {
                    core = Integer.parseInt(sp[7]);
                } catch (Exception e) {
                    logger.error(desc(e.getMessage()), e);
                }
                int max = 8; // default 8
                try {
                    max = Integer.parseInt(sp[8]);
                } catch (Exception e) {
                    logger.error(desc(e.getMessage()), e);
                }
                try {
                    if (flag != null && flag.contains(ConnectionAccess.READ_FLAG)) {
                        Entry entry = new Entry(host, port, user, pwd, flag, patternStr, db, core,
                                max);
                        newReadEntries.add(entry);
                    }
                    if (flag != null && flag.contains(ConnectionAccess.WRITE_FLAG)) {
                        Entry entry = new Entry(host, port, user, pwd, flag, patternStr, db, core,
                                max);
                        newWriteEntries.add(entry);
                    }
                } catch (Exception e) {
                    logger.error(desc(e.getMessage()), e);
                }
            }
            // apply to new read/write entries
            logger.debug(desc(String.format(
                    "oldReadSize=%d,newReadSize=%d, oldWriteSize=%d, newWriteSize=%d",
                    readEntries.size(), newReadEntries.size(), writeEntries.size(),
                    newWriteEntries.size())));
            List<Entry> oldReadEntries = readEntries;
            readEntries = newReadEntries;
            // destroy old read entries
            for (Entry entry : oldReadEntries) {
                try {
                    entry.closeDataSource();
                } catch (Exception e) {
                    logger.error(desc(e.getMessage()), e);
                }
            }
            oldReadEntries.clear();

            // destroy old write entries
            List<Entry> oldWriteEntries = writeEntries;
            writeEntries = newWriteEntries;
            for (Entry entry : oldWriteEntries) {
                try {
                    entry.closeDataSource();
                } catch (Exception e) {
                    logger.error(desc(e.getMessage()), e);
                }
            }
            oldWriteEntries.clear();
        } finally {
            rwlock.writeLock().unlock();
        }
        return true;
    }

    @Override
    public boolean delete() {
        return true;
    }

    @Override
    public void close() throws IOException {
        try {
            rwlock.writeLock().lock();
            this.closed = true;
            for (Entry entry : readEntries) {
                try {
                    entry.closeDataSource();
                } catch (Exception e) {
                    logger.error(desc(e.getMessage()), e);
                }
            }
        } finally {
            rwlock.writeLock().unlock();
        }
    }
    /*
    public static void main(String[] args) {
        BasicConfigurator.configure();
        ReadOnlyDataSource ds = new ReadOnlyDataSource("test3", "1234");
        int loop = 100;
        for (int i = 0; i < loop; ++i) {

            try {
                Connection conn = ds.getConnection();
                logger.debug("conn.isValid(1000):" + conn.isValid(1000));
                conn.close();
            } catch (SQLException e1) {
                e1.printStackTrace();
            }

            try {
                TimeUnit.SECONDS.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        try {
            ds.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    */
}
