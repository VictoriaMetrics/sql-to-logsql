package com.victoriametrics.logsql.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.util.Properties;
import java.util.logging.Logger;

/**
 * JDBC driver implementation for the sql-to-logsql translation service.
 */
public final class LogsqlDriver implements Driver {

    /**
     * JDBC URL prefix for the driver.
     */
    public static final String URL_PREFIX = "jdbc:logsql://";

    static {
        try {
            DriverManager.registerDriver(new LogsqlDriver());
        } catch (SQLException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        if (!acceptsURL(url)) {
            return null;
        }

        LogsqlConnectionConfig config = LogsqlUrlParser.parse(url, info);
        return new LogsqlConnection(config);
    }

    @Override
    public boolean acceptsURL(String url) {
        return url != null && url.startsWith(URL_PREFIX);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        LogsqlConnectionConfig config = LogsqlUrlParser.parse(url, info);
        return config.toDriverPropertyInfo();
    }

    @Override
    public int getMajorVersion() {
        return 0;
    }

    @Override
    public int getMinorVersion() {
        return 1;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() {
        return Logger.getLogger("com.victoriametrics.logsql.jdbc");
    }
}
