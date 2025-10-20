package com.victoriametrics.logsql.jdbc;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSetMetaData;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Types;
import java.util.List;

class LogsqlResultSetMetaData implements ResultSetMetaData {

    private final List<String> columnNames;
    private final int[] columnTypes;

    LogsqlResultSetMetaData(List<String> columnNames, int[] columnTypes) {
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        int type = getColumnType(column);
        return type == Types.VARCHAR || type == Types.CHAR || type == Types.LONGVARCHAR;
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return false;
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return ResultSetMetaData.columnNullable;
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        int type = getColumnType(column);
        return type == Types.INTEGER
                || type == Types.BIGINT
                || type == Types.DECIMAL
                || type == Types.NUMERIC
                || type == Types.DOUBLE
                || type == Types.FLOAT
                || type == Types.REAL
                || type == Types.SMALLINT
                || type == Types.TINYINT;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return columnNames.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return 0;
    }

    @Override
    public int getScale(int column) throws SQLException {
        return 0;
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return "";
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return "";
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return columnTypes[column - 1];
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.BOOLEAN:
            case Types.BIT:
                return "BOOLEAN";
            case Types.INTEGER:
                return "INTEGER";
            case Types.BIGINT:
                return "BIGINT";
            case Types.DOUBLE:
                return "DOUBLE";
            case Types.FLOAT:
                return "FLOAT";
            case Types.REAL:
                return "REAL";
            case Types.NUMERIC:
            case Types.DECIMAL:
                return "NUMERIC";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            case Types.DATE:
                return "DATE";
            case Types.TIME:
                return "TIME";
            case Types.VARBINARY:
                return "VARBINARY";
            default:
                return "VARCHAR";
        }
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return false;
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int type = getColumnType(column);
        switch (type) {
            case Types.BOOLEAN:
            case Types.BIT:
                return Boolean.class.getName();
            case Types.INTEGER:
            case Types.SMALLINT:
            case Types.TINYINT:
                return Integer.class.getName();
            case Types.BIGINT:
                return Long.class.getName();
            case Types.DOUBLE:
            case Types.FLOAT:
            case Types.REAL:
                return Double.class.getName();
            case Types.NUMERIC:
            case Types.DECIMAL:
                return BigDecimal.class.getName();
            case Types.TIMESTAMP:
                return Timestamp.class.getName();
            case Types.DATE:
                return Date.class.getName();
            case Types.TIME:
                return Time.class.getName();
            case Types.VARBINARY:
                return byte[].class.getName();
            default:
                return String.class.getName();
        }
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
}
