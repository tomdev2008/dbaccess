package com.qunar.db.resource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jolbox.bonecp.BoneCPDataSource;
import com.qunar.db.util.DbLogger;

public class Entry {

    private static final Logger logger = LoggerFactory.getLogger(DbLogger.class);

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

    Entry(String host, int port, String user, String password, String rwFlag, String patternStr,
            String dbName, int coreSize, int maxSize) {
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
        ds.setServiceOrder("LIFO");
        this.dataSource = ds;
    }

    private String getConnectionUrl() {
        StringBuffer sb = new StringBuffer();
        sb.append(CONNECTION_URL_PREFIX).append(this.host).append(":").append(this.port)
                .append("/").append(this.dbName).append(CONNECTION_URL_SUFFIX);
        return sb.toString();
    }

    public boolean isWritable() {
        return rwFlag != null && rwFlag.contains(ConnectionAccess.WRITE_FLAG);
    }

    public boolean isReadable() {
        return rwFlag != null && rwFlag.contains(ConnectionAccess.READ_FLAG);
    }

    public Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

    public void closeDataSource() throws Exception {
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
