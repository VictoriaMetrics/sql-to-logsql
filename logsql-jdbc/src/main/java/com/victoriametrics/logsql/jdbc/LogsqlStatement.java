package com.victoriametrics.logsql.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class LogsqlStatement implements Statement {

    private final LogsqlConnection connection;
    private LogsqlResultSet currentResultSet;
    private String translatedLogsql;
    private boolean closed;
    private int maxRows;
    private int fetchSize = 0;
    private int queryTimeoutSeconds = 0;
    private boolean poolable = false;
    private boolean closeOnCompletion = false;
    private boolean closing = false;

    LogsqlStatement(LogsqlConnection connection) {
        this.connection = Objects.requireNonNull(connection, "connection");
    }

    LogsqlConnection getConnectionInternal() {
        return connection;
    }

    String getTranslatedLogsql() {
        return translatedLogsql;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkOpen();
        LogsqlQueryResult result = connection.executeQuery(sql, maxRows);
        this.translatedLogsql = result.getLogsql();
        closeCurrentResultSet();
        this.currentResultSet = buildResultSet(result);
        return currentResultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closing = true;
            try {
                closeCurrentResultSet();
            } finally {
                closing = false;
                closed = true;
            }
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("max field size must be non-negative");
        }
        // no-op
    }

    @Override
    public int getMaxRows() throws SQLException {
        return maxRows;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        if (max < 0) {
            throw new SQLException("max rows must be non-negative");
        }
        this.maxRows = max;
    }

    @Override
    public void setEscapeProcessing(boolean enable) {
        // no-op
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return queryTimeoutSeconds;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        if (seconds < 0) {
            throw new SQLException("query timeout must be non-negative");
        }
        this.queryTimeoutSeconds = seconds;
    }

    @Override
    public void cancel() throws SQLException {
        throw new SQLFeatureNotSupportedException("Cancel is not supported");
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // no warnings
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        throw new SQLFeatureNotSupportedException("Cursor naming is not supported");
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        executeQuery(sql);
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return currentResultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return -1;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        closeCurrentResultSet();
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw new SQLFeatureNotSupportedException("Only forward fetch direction is supported");
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0) {
            throw new SQLException("fetch size must be non-negative");
        }
        this.fetchSize = rows;
    }

    @Override
    public int getFetchSize() throws SQLException {
        return fetchSize;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates are not supported");
    }

    @Override
    public void clearBatch() throws SQLException {
        // no-op
    }

    @Override
    public int[] executeBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch updates are not supported");
    }

    @Override
    public Connection getConnection() throws SQLException {
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        if (current == Statement.CLOSE_CURRENT_RESULT) {
            closeCurrentResultSet();
        } else if (current == Statement.CLOSE_ALL_RESULTS) {
            closeCurrentResultSet();
        }
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        throw new SQLFeatureNotSupportedException("Generated keys are not supported");
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        this.poolable = poolable;
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return poolable;
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        this.closeOnCompletion = true;
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return closeOnCompletion;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isInstance(this)) {
            return iface.cast(this);
        }
        throw new SQLFeatureNotSupportedException("Not a wrapper for " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isInstance(this);
    }

    private LogsqlResultSet buildResultSet(LogsqlQueryResult result) {
        List<String> columns = result.getColumnNames();
        int columnCount = columns.size();
        int[] columnTypes = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            columnTypes[i] = inferColumnType(result.getRows(), columns.get(i));
        }

        List<Object[]> rows = new ArrayList<>(result.getRows().size());
        for (Map<String, Object> row : result.getRows()) {
            Object[] values = new Object[columnCount];
            for (int i = 0; i < columnCount; i++) {
                values[i] = row.get(columns.get(i));
            }
            rows.add(values);
        }

        return new LogsqlResultSet(this, columns, columnTypes, rows);
    }

    private int inferColumnType(List<Map<String, Object>> rows, String column) {
        for (Map<String, Object> row : rows) {
            Object value = row.get(column);
            if (value == null) {
                continue;
            }
            if (value instanceof Boolean) {
                return Types.BOOLEAN;
            }
            if (value instanceof Byte || value instanceof Short || value instanceof Integer) {
                return Types.INTEGER;
            }
            if (value instanceof Long) {
                return Types.BIGINT;
            }
            if (value instanceof Float) {
                return Types.REAL;
            }
            if (value instanceof Double) {
                return Types.DOUBLE;
            }
            if (value instanceof java.math.BigDecimal) {
                return Types.NUMERIC;
            }
            if (value instanceof java.time.temporal.Temporal || value instanceof java.util.Date) {
                return Types.TIMESTAMP;
            }
            if (value instanceof byte[]) {
                return Types.VARBINARY;
            }
            return Types.VARCHAR;
        }
        return Types.VARCHAR;
    }

    void onResultSetClosed(LogsqlResultSet resultSet) throws SQLException {
        if (currentResultSet == resultSet) {
            currentResultSet = null;
        }
        if (closeOnCompletion && !closing && !closed) {
            close();
        }
    }

    void closeCurrentResultSet() throws SQLException {
        if (currentResultSet != null) {
            LogsqlResultSet rs = currentResultSet;
            currentResultSet = null;
            rs.closeFromStatement();
        }
    }

    void checkOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Statement is closed");
        }
        if (connection.isClosed()) {
            throw new SQLException("Connection is closed");
        }
    }

    ResultSet getCurrentResultSet() {
        return currentResultSet;
    }
}
