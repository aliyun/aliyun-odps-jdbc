package com.aliyun.odps.jdbc.impl;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import com.aliyun.odps.Odps;
import com.aliyun.odps.account.Account;
import com.aliyun.odps.account.AliyunAccount;

public class NonRegisteringOdpsDriver implements Driver {

    public final static String DEFAULT_PREFIX = "jdbc:odps:";
    private String             acceptPrefix   = DEFAULT_PREFIX;

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        String host;
        String project;
        {
            int start = acceptPrefix.length();
            int pos = url.indexOf('/', start);
            if (pos != -1) {
                host = url.substring(start, pos);
            } else {
                host = url.substring(start);
            }

            int p1 = url.indexOf('?', pos + 1);
            if (p1 == -1) {
                project = url.substring(pos + 1);
            } else {
                project = url.substring(pos + 1, p1);
            }
        }

        String accessId = info.getProperty("user");
        String accessKey = info.getProperty("password");
        String endpoint = "http://" + host + "/api";

        if (accessId == null || accessId.isEmpty()) {
            accessId = info.getProperty("access_id");
        }

        if (accessKey == null || accessKey.isEmpty()) {
            accessKey = info.getProperty("access_key");
        }

        Account account = new AliyunAccount(accessId, accessKey);
        Odps odps = new Odps(account);
        odps.setDefaultProject(project);
        odps.setEndpoint(endpoint);

        OdpsConnection conn = new OdpsConnection(odps, url, info);

        return conn;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        if (url == null) {
            return false;
        }

        if (url.startsWith(acceptPrefix)) {
            return true;
        }

        return false;
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return null;
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }

}
