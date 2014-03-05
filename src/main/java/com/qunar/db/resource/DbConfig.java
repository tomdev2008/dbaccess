package com.qunar.db.resource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;
import com.qunar.db.util.DbLogger;
import com.qunar.redis.storage.Constant;
import com.qunar.zkclient.ZkClient;
import com.qunar.zkclient.exception.ZkException;
import com.qunar.zkclient.listener.NodeDataListener;

/**
 * 
 * @author michael
 * @email liyong19861014@gmail.com
 */
public class DbConfig extends NodeDataListener implements ConnectionAccess {

    private static final Logger logger = LoggerFactory.getLogger(DbLogger.class);

    private final String namespace;

    private final String cipher;

    private Random rand;

    private ReentrantReadWriteLock rwlock;

    private List<Entry> readEntries;

    private List<Entry> writeEntries;

    private ZkClient zkClient;

    public DbConfig(String namespace, String cipher) {
        super(DATABASE_DESC_PREFIX + namespace);
        this.namespace = namespace;
        this.cipher = cipher;
        this.rand = new Random(System.currentTimeMillis());
        this.rwlock = new ReentrantReadWriteLock();
        this.readEntries = new LinkedList<DbConfig.Entry>();
        this.writeEntries = new LinkedList<DbConfig.Entry>();
        this.zkClient = ZkClient.getInstance(Constant.DEFAULT_ZK_ADDRESS);
        //        this.zkClient = ZkClient.getInstance("127.0.0.1:2181");
        zkClient.addNodeDataListener(this);
        try {
            zkClient.exist(getNodePath(), true);
        } catch (ZkException e) {
            logger.error(desc(e.getMessage()), e);
        }
        update(getNodePath());
    }

    @Override
    public Connection getReadConnection() throws Exception {
        if (readEntries.isEmpty()) {
            return null;
        }
        try {
            rwlock.readLock().lock();
            int size = readEntries.size();
            if (size == 1) {
                return readEntries.get(0).getConnection();
            } else {
                int index = rand.nextInt(size);
                return readEntries.get(index).getConnection();
            }
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    public Connection getReadConnection(String pattern) throws Exception {
        if (readEntries.isEmpty()) {
            return null;
        }
        try {
            rwlock.readLock().lock();
            Connection conn = null;
            for (Entry entry : readEntries) {
                if (entry.match(pattern)) {
                    conn = entry.getConnection();
                    break;
                }
            }
            return conn;
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    public Connection getWriteConnection() throws Exception {
        if (writeEntries.isEmpty()) {
            return null;
        }
        try {
            rwlock.readLock().lock();
            return writeEntries.get(0).getConnection();
        } finally {
            rwlock.readLock().unlock();
        }
    }

    @Override
    public Connection getWriteConnection(String pattern) throws Exception {
        if (writeEntries.isEmpty()) {
            return null;
        }
        try {
            rwlock.readLock().lock();
            Connection conn = null;
            for (Entry entry : writeEntries) {
                if (entry.match(pattern)) {
                    conn = entry.getConnection();
                    break;
                }
            }
            return conn;
        } finally {
            rwlock.readLock().unlock();
        }

    }

    @Override
    public boolean update(String value) {
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

            List<Entry> newReadEntries = new ArrayList<DbConfig.Entry>();
            List<Entry> newWriteEntries = new ArrayList<DbConfig.Entry>();
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
                    Entry entry = new Entry(host, port, user, pwd, flag, patternStr, db, core, max);
                    if (entry.isWritable()) {
                        newWriteEntries.add(entry);
                    } else {
                        newReadEntries.add(entry);
                    }
                } catch (Exception e) {
                    logger.error(desc(e.getMessage()), e);
                }
            }

            // apply to new read/write entries
            logger.debug(desc(String.format(
                    "oldReadSize=%d,oldWriteSize=%d,newReadSize=%d,newWriteSize=%d",
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

            List<Entry> oldWriteEntries = writeEntries;
            writeEntries = newWriteEntries;
            // destroy old write entries
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
        // do nothing
        return true;
    }

    protected String desc(String msg) {
        StringBuffer sb = new StringBuffer();
        sb.append("[ns:").append(namespace);
        if (msg != null) {
            sb.append(", msg:").append(msg);
        }
        sb.append("]");
        return sb.toString();
    }

    private static class Entry {

        private String host;

        private int port;

        private String user;

        private String password;

        private String rwFlag;

        private String patternStr;

        private Pattern pattern;

        private String dbName;

        private int coreSize;

        private int maxSize;

        private static final String CONNECTION_URL_PREFIX = "jdbc:mysql://";

        private static final String CONNECTION_URL_SUFFIX = "?useunicode=true&characterencoding=utf8&autoReconnect=true";

        private static final String MYSQL_DRIVER_CLASS = "com.mysql.jdbc.Driver";

        private DataSource dataSource;

        Entry(String host, int port, String user, String password, String rwFlag,
                String patternStr, String dbName, int coreSize, int maxSize) {
            this.host = host;
            this.port = port;
            this.user = user;
            this.password = password;
            this.rwFlag = rwFlag;
            this.patternStr = patternStr;
            if (this.patternStr != null && !this.patternStr.isEmpty()) {
                this.pattern = Pattern.compile(this.patternStr);
            } else {
                this.pattern = Pattern.compile("bad bad bad regex string");
            }
            this.dbName = dbName;
            this.coreSize = coreSize;
            this.maxSize = maxSize;
            this.dataSource = null;
            initDataSource();
        }

        private void initDataSource() {
            BoneCPDataSource ds = new BoneCPDataSource();
            ds.setDriverClass(MYSQL_DRIVER_CLASS);
            ds.setUsername(user);
            ds.setPassword(password);
            ds.setJdbcUrl(getConnectionUrl());
            ds.setPartitionCount(1);
            ds.setMinConnectionsPerPartition(coreSize);
            ds.setMaxConnectionsPerPartition(maxSize);
            ds.setConnectionTimeoutInMs(1000);
            ds.setAcquireIncrement(2);
            ds.setIdleConnectionTestPeriodInSeconds(2);
            ds.setIdleMaxAgeInMinutes(15);
            ds.setConnectionTestStatement("SELECT 1");
            ds.setLogStatementsEnabled(false);
            ds.setAcquireRetryDelayInMs(1000);
            ds.setAcquireRetryAttempts(3);
            ds.setLazyInit(true);
            ds.setDisableJMX(true);
            ds.setPoolAvailabilityThreshold(10);
            logger.debug("ds:" + ds.toString());
            this.dataSource = ds;
        }

        private String getConnectionUrl() {
            StringBuffer sb = new StringBuffer();
            sb.append(CONNECTION_URL_PREFIX).append(this.host).append(":").append(this.port)
                    .append("/").append(this.dbName).append(CONNECTION_URL_SUFFIX);
            return sb.toString();
        }

        public boolean isWritable() {
            return rwFlag != null && rwFlag.contains(WRITE_FLAG);
        }

        public boolean isReadable() {
            return rwFlag != null && rwFlag.contains(READ_FLAG);
        }

        public Connection getConnection() throws SQLException {
            return dataSource.getConnection();
        }

        public void closeDataSource() throws SQLException {
            if (dataSource != null) {
                BoneCPDataSource ds = (BoneCPDataSource) dataSource;
                ds.close();
            }
        }

        public boolean match(String pat) {
            if (pat == null) {
                return false;
            }
            return pattern.matcher(pat).matches();
        }

        @Override
        public String toString() {
            JSONObject json = new JSONObject();
            try {
                json.put("host", host);
                json.put("port", port);
                json.put("user", user);
                json.put("password", "******");
                json.put("flag", rwFlag);
                json.put("pattern", pattern);
                json.put("db_name", dbName);
                json.put("coreSize", coreSize);
                json.put("maxSize", maxSize);
                json.put("dataSource", dataSource.toString());
            } catch (JSONException e) {
                logger.error(e.getMessage(), e);
            }
            return json.toString();
        }

    }
}
