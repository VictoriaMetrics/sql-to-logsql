package com.victoriametrics.logsql.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.ParameterMetaData;
import java.sql.PreparedStatement;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLType;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Calendar;

class LogsqlPreparedStatement extends LogsqlStatement implements PreparedStatement {

    private static final Object UNSET = new Object();
    private static final Object NULL_VALUE = new Object();

    private final String sqlTemplate;
    private final int parameterCount;
    private final Object[] parameters;

    LogsqlPreparedStatement(LogsqlConnection connection, String sql) throws SQLException {
        super(connection);
        if (sql == null) {
            throw new SQLException("SQL must not be null");
        }
        this.sqlTemplate = sql;
        this.parameterCount = countParameters(sql);
        this.parameters = new Object[parameterCount];
        Arrays.fill(this.parameters, UNSET);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkOpen();
        return super.executeQuery(renderSql());
    }

    @Override
    public int executeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setParameter(parameterIndex, NULL_VALUE);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setParameter(parameterIndex, x);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setParameter(parameterIndex, x == null ? NULL_VALUE : x.clone());
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setParameter(parameterIndex, x == null ? NULL_VALUE : new Date(x.getTime()));
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setParameter(parameterIndex, x == null ? NULL_VALUE : new Time(x.getTime()));
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setParameter(parameterIndex, x == null ? NULL_VALUE : new Timestamp(x.getTime()));
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void clearParameters() throws SQLException {
        Arrays.fill(parameters, UNSET);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x) throws SQLException {
        setParameter(parameterIndex, x == null ? NULL_VALUE : x);
    }

    @Override
    public boolean execute() throws SQLException {
        checkOpen();
        super.executeQuery(renderSql());
        return true;
    }

    @Override
    public void addBatch() throws SQLException {
        throw new SQLFeatureNotSupportedException("Batch execution is not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQL REF is not supported");
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Blob is not supported");
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Clob is not supported");
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Array is not supported");
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        ResultSet rs = getCurrentResultSet();
        return rs != null ? rs.getMetaData() : null;
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex, x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex, x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex, x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex, sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setParameter(parameterIndex, x == null ? NULL_VALUE : x.toString());
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        throw new SQLFeatureNotSupportedException("Parameter metadata is not supported");
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw new SQLFeatureNotSupportedException("RowId is not supported");
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex, value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        throw new SQLFeatureNotSupportedException("NClob is not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw new SQLFeatureNotSupportedException("SQLXML is not supported");
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public void setObject(int parameterIndex, Object x, SQLType targetSqlType) throws SQLException {
        setObject(parameterIndex, x);
    }

    @Override
    public long executeLargeUpdate() throws SQLException {
        throw new SQLFeatureNotSupportedException("Updates are not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        throw new SQLFeatureNotSupportedException("Streams are not supported");
    }

    private void setParameter(int parameterIndex, Object value) throws SQLException {
        if (parameterIndex < 1 || parameterIndex > parameterCount) {
            throw new SQLException("Parameter index out of range: " + parameterIndex);
        }
        parameters[parameterIndex - 1] = value;
    }

    private String renderSql() throws SQLException {
        if (parameterCount == 0) {
            return sqlTemplate;
        }

        StringBuilder builder = new StringBuilder();
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int parameterIndex = 0;
        int length = sqlTemplate.length();
        for (int i = 0; i < length; i++) {
            char c = sqlTemplate.charAt(i);
            if (c == '\'') {
                builder.append(c);
                if (inSingleQuote && i + 1 < length && sqlTemplate.charAt(i + 1) == '\'') {
                    builder.append('\'');
                    i++;
                } else {
                    inSingleQuote = !inSingleQuote;
                }
            } else if (c == '"') {
                builder.append(c);
                if (inDoubleQuote && i + 1 < length && sqlTemplate.charAt(i + 1) == '"') {
                    builder.append('"');
                    i++;
                } else {
                    inDoubleQuote = !inDoubleQuote;
                }
            } else if (!inSingleQuote && !inDoubleQuote && c == '?') {
                if (parameterIndex >= parameterCount) {
                    throw new SQLException("Too many parameters in SQL template");
                }
                builder.append(formatParameter(getParameterValue(parameterIndex)));
                parameterIndex++;
            } else {
                builder.append(c);
            }
        }

        if (parameterIndex < parameterCount) {
            throw new SQLException("Not all parameters were set");
        }
        return builder.toString();
    }

    private Object getParameterValue(int index) throws SQLException {
        Object value = parameters[index];
        if (value == UNSET) {
            throw new SQLException("Parameter " + (index + 1) + " is not set");
        }
        return value == NULL_VALUE ? null : value;
    }

    private String formatParameter(Object value) throws SQLException {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String || value instanceof Character) {
            return quote(value.toString());
        }
        if (value instanceof Boolean) {
            return ((Boolean) value) ? "TRUE" : "FALSE";
        }
        if (value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long) {
            return value.toString();
        }
        if (value instanceof Float || value instanceof Double) {
            if (((Number) value).doubleValue() == Double.POSITIVE_INFINITY || ((Number) value).doubleValue() == Double.NEGATIVE_INFINITY || Double.isNaN(((Number) value).doubleValue())) {
                throw new SQLException("Floating point value cannot be represented: " + value);
            }
            return value.toString();
        }
        if (value instanceof BigDecimal) {
            return ((BigDecimal) value).toPlainString();
        }
        if (value instanceof byte[]) {
            return formatBytes((byte[]) value);
        }
        if (value instanceof Date) {
            return quote(value.toString());
        }
        if (value instanceof Time) {
            return quote(value.toString());
        }
        if (value instanceof Timestamp) {
            return quote(value.toString());
        }
        if (value instanceof Instant) {
            return quote(DateTimeFormatter.ISO_INSTANT.format((Instant) value));
        }
        if (value instanceof OffsetDateTime) {
            return quote(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format((OffsetDateTime) value));
        }
        if (value instanceof ZonedDateTime) {
            return quote(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(((ZonedDateTime) value).toOffsetDateTime()));
        }
        if (value instanceof LocalDateTime) {
            return quote(DateTimeFormatter.ISO_LOCAL_DATE_TIME.format((LocalDateTime) value));
        }
        if (value instanceof LocalDate) {
            return quote(DateTimeFormatter.ISO_LOCAL_DATE.format((LocalDate) value));
        }
        if (value instanceof Calendar) {
            Calendar calendar = (Calendar) value;
            return quote(DateTimeFormatter.ISO_OFFSET_DATE_TIME.format(ZonedDateTime.ofInstant(calendar.toInstant(), calendar.getTimeZone().toZoneId())));
        }
        return quote(value.toString());
    }

    private String quote(String value) {
        String escaped = value.replace("'", "''");
        return "'" + escaped + "'";
    }

    private int countParameters(String sql) {
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;
        int count = 0;
        int length = sql.length();
        for (int i = 0; i < length; i++) {
            char c = sql.charAt(i);
            if (c == '\'') {
                if (inSingleQuote && i + 1 < length && sql.charAt(i + 1) == '\'') {
                    i++;
                } else {
                    inSingleQuote = !inSingleQuote;
                }
            } else if (c == '\"') {
                if (inDoubleQuote && i + 1 < length && sql.charAt(i + 1) == '\"') {
                    i++;
                } else {
                    inDoubleQuote = !inDoubleQuote;
                }
            } else if (!inSingleQuote && !inDoubleQuote && c == '?') {
                count++;
            }
        }
        return count;
    }

    private String formatBytes(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        sb.append("X'");
        for (byte b : bytes) {
            sb.append(String.format("%02X", b));
        }
        sb.append("'");
        return sb.toString();
    }
}
