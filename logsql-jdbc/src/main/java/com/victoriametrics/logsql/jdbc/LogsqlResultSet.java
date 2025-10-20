package com.victoriametrics.logsql.jdbc;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.Array;
import java.sql.Statement;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Date;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

class LogsqlResultSet implements ResultSet {

    private final LogsqlStatement statement; // may be null for metadata result sets
    private final List<String> columnNames;
    private final Map<String, Integer> columnIndexes;
    private final List<Object[]> rows;
    private final int[] columnTypes;
    private final LogsqlResultSetMetaData metaData;

    private int cursor = -1;
    private boolean closed = false;
    private boolean wasNull = false;

    LogsqlResultSet(LogsqlStatement statement, List<String> columnNames, int[] columnTypes, List<Object[]> rows) {
        this.statement = statement;
        this.columnNames = columnNames;
        this.columnTypes = columnTypes;
        this.rows = rows;
        this.columnIndexes = new HashMap<>();
        for (int i = 0; i < columnNames.size(); i++) {
            columnIndexes.put(columnNames.get(i), i);
        }
        this.metaData = new LogsqlResultSetMetaData(columnNames, columnTypes);
    }

    void closeFromStatement() {
        closed = true;
        cursor = rows.size();
    }

    @Override
    public boolean next() throws SQLException {
        ensureOpen();
        if (cursor + 1 < rows.size()) {
            cursor++;
            wasNull = false;
            return true;
        }
        cursor = rows.size();
        wasNull = false;
        return false;
    }

    @Override
    public void close() throws SQLException {
        if (!closed) {
            closed = true;
            if (statement != null) {
                statement.onResultSetClosed(this);
            }
        }
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        return value != null ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Boolean value = toBoolean(getColumnValue(columnIndex));
        return value != null && value;
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Number number = toNumber(getColumnValue(columnIndex));
        return number == null ? 0 : number.byteValue();
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Number number = toNumber(getColumnValue(columnIndex));
        return number == null ? 0 : number.shortValue();
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Number number = toNumber(getColumnValue(columnIndex));
        return number == null ? 0 : number.intValue();
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Number number = toNumber(getColumnValue(columnIndex));
        return number == null ? 0L : number.longValue();
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Number number = toNumber(getColumnValue(columnIndex));
        return number == null ? 0F : number.floatValue();
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Number number = toNumber(getColumnValue(columnIndex));
        return number == null ? 0D : number.doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        BigDecimal value = getBigDecimal(columnIndex);
        if (value == null) {
            return null;
        }
        return value.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            byte[] bytes = (byte[]) value;
            return bytes.clone();
        }
        if (value instanceof String) {
            return ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        throw new SQLException("Cannot convert value to byte[]: " + value.getClass().getName());
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        return toSqlDate(getColumnValue(columnIndex));
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        return toSqlTime(getColumnValue(columnIndex));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        return toSqlTimestamp(getColumnValue(columnIndex));
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        Object value = getColumnValue(columnLabel);
        return value != null ? value.toString() : null;
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        Boolean value = toBoolean(getColumnValue(columnLabel));
        return value != null && value;
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        Number number = toNumber(getColumnValue(columnLabel));
        return number == null ? 0 : number.byteValue();
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        Number number = toNumber(getColumnValue(columnLabel));
        return number == null ? 0 : number.shortValue();
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        Number number = toNumber(getColumnValue(columnLabel));
        return number == null ? 0 : number.intValue();
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        Number number = toNumber(getColumnValue(columnLabel));
        return number == null ? 0L : number.longValue();
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        Number number = toNumber(getColumnValue(columnLabel));
        return number == null ? 0F : number.floatValue();
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        Number number = toNumber(getColumnValue(columnLabel));
        return number == null ? 0D : number.doubleValue();
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        BigDecimal value = getBigDecimal(columnLabel);
        if (value == null) {
            return null;
        }
        return value.setScale(scale, RoundingMode.HALF_UP);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        Object value = getColumnValue(columnLabel);
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            return ((byte[]) value).clone();
        }
        if (value instanceof String) {
            return ((String) value).getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        throw new SQLException("Cannot convert value to byte[]: " + value.getClass().getName());
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return toSqlDate(getColumnValue(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return toSqlTime(getColumnValue(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return toSqlTimestamp(getColumnValue(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {
        // no warnings produced
    }

    @Override
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return metaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {
        return getColumnValue(columnIndex);
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getColumnValue(columnLabel);
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        Integer index = columnIndexes.get(columnLabel);
        if (index == null) {
            throw new SQLException("Column not found: " + columnLabel);
        }
        return index + 1;
    }

    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        return value == null ? null : new StringReader(value);
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        String value = getString(columnLabel);
        return value == null ? null : new StringReader(value);
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object value = getColumnValue(columnIndex);
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException ex) {
                throw new SQLException("Failed to convert value to BigDecimal: " + value, ex);
            }
        }
        throw new SQLException("Cannot convert value to BigDecimal: " + value.getClass().getName());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        Object value = getColumnValue(columnLabel);
        if (value == null) {
            return null;
        }
        if (value instanceof BigDecimal) {
            return (BigDecimal) value;
        }
        if (value instanceof Number) {
            return BigDecimal.valueOf(((Number) value).doubleValue());
        }
        if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException ex) {
                throw new SQLException("Failed to convert value to BigDecimal: " + value, ex);
            }
        }
        throw new SQLException("Cannot convert value to BigDecimal: " + value.getClass().getName());
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        ensureOpen();
        return cursor < 0 && !rows.isEmpty();
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        ensureOpen();
        return rows.isEmpty() ? false : cursor >= rows.size();
    }

    @Override
    public boolean isFirst() throws SQLException {
        ensureOpen();
        return cursor == 0 && !rows.isEmpty();
    }

    @Override
    public boolean isLast() throws SQLException {
        ensureOpen();
        return !rows.isEmpty() && cursor == rows.size() - 1;
    }

    @Override
    public void beforeFirst() throws SQLException {
        ensureOpen();
        cursor = -1;
    }

    @Override
    public void afterLast() throws SQLException {
        ensureOpen();
        cursor = rows.size();
    }

    @Override
    public boolean first() throws SQLException {
        ensureOpen();
        if (rows.isEmpty()) {
            cursor = rows.size();
            return false;
        }
        cursor = 0;
        wasNull = false;
        return true;
    }

    @Override
    public boolean last() throws SQLException {
        ensureOpen();
        if (rows.isEmpty()) {
            cursor = rows.size();
            return false;
        }
        cursor = rows.size() - 1;
        wasNull = false;
        return true;
    }

    @Override
    public int getRow() throws SQLException {
        ensureOpen();
        if (cursor < 0 || cursor >= rows.size()) {
            return 0;
        }
        return cursor + 1;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        ensureOpen();
        int target;
        if (row > 0) {
            target = row - 1;
        } else if (row < 0) {
            target = rows.size() + row;
        } else {
            cursor = -1;
            return false;
        }
        if (target < 0) {
            cursor = -1;
            return false;
        }
        if (target >= rows.size()) {
            cursor = rows.size();
            return false;
        }
        cursor = target;
        wasNull = false;
        return true;
    }

    @Override
    public boolean relative(int rowsOffset) throws SQLException {
        ensureOpen();
        int target = cursor + rowsOffset;
        if (cursor < 0) {
            target = rowsOffset - 1;
        }
        if (target < 0) {
            cursor = -1;
            return false;
        }
        if (target >= rows.size()) {
            cursor = rows.size();
            return false;
        }
        cursor = target;
        wasNull = false;
        return true;
    }

    @Override
    public boolean previous() throws SQLException {
        ensureOpen();
        if (cursor <= 0) {
            cursor = -1;
            return false;
        }
        cursor--;
        wasNull = false;
        return true;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        if (direction != ResultSet.FETCH_FORWARD) {
            throw unsupported();
        }
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        if (rows < 0) {
            throw new SQLException("Fetch size must be non-negative");
        }
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 0;
    }

    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        return false;
    }

    @Override
    public boolean rowInserted() throws SQLException {
        return false;
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        return false;
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void insertRow() throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateRow() throws SQLException {
        throw unsupported();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw unsupported();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw unsupported();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw unsupported();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw unsupported();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw unsupported();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnLabel);
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public Date getDate(int columnIndex, java.util.Calendar cal) throws SQLException {
        return adjustWithCalendar(getDate(columnIndex), cal);
    }

    @Override
    public Date getDate(String columnLabel, java.util.Calendar cal) throws SQLException {
        return adjustWithCalendar(getDate(columnLabel), cal);
    }

    @Override
    public Time getTime(int columnIndex, java.util.Calendar cal) throws SQLException {
        return adjustWithCalendar(getTime(columnIndex), cal);
    }

    @Override
    public Time getTime(String columnLabel, java.util.Calendar cal) throws SQLException {
        return adjustWithCalendar(getTime(columnLabel), cal);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, java.util.Calendar cal) throws SQLException {
        return adjustWithCalendar(getTimestamp(columnIndex), cal);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, java.util.Calendar cal) throws SQLException {
        return adjustWithCalendar(getTimestamp(columnLabel), cal);
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        String value = getString(columnIndex);
        if (value == null) {
            return null;
        }
        try {
            return new URL(value);
        } catch (MalformedURLException e) {
            throw new SQLException("Invalid URL value: " + value, e);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        String value = getString(columnLabel);
        if (value == null) {
            return null;
        }
        try {
            return new URL(value);
        } catch (MalformedURLException e) {
            throw new SQLException("Invalid URL value: " + value, e);
        }
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBlob(int columnIndex, Blob x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBlob(String columnLabel, Blob x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateClob(int columnIndex, Clob x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateClob(String columnLabel, Clob x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw unsupported();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw unsupported();
    }

    @Override
    public int getHoldability() throws SQLException {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return closed;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNClob(int columnIndex, NClob nClob) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNClob(String columnLabel, NClob nClob) throws SQLException {
        throw unsupported();
    }

    @Override
    public NClob getNClob(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public NClob getNClob(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw unsupported();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getString(columnLabel);
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        throw unsupported();
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw unsupported();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw unsupported();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        return convertToType(getColumnValue(columnIndex), type, columnIndex);
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return convertToType(getColumnValue(columnLabel), type, columnIndexes.get(columnLabel) + 1);
    }

    private <T> T convertToType(Object value, Class<T> type, int columnIndex) throws SQLException {
        if (value == null) {
            return null;
        }
        if (type.isInstance(value)) {
            return type.cast(value);
        }
        if (type == String.class) {
            return type.cast(value.toString());
        }
        if (Number.class.isAssignableFrom(type)) {
            Number number = toNumber(value);
            if (number == null) {
                return null;
            }
            if (type == Integer.class) {
                return type.cast(Integer.valueOf(number.intValue()));
            }
            if (type == Long.class) {
                return type.cast(Long.valueOf(number.longValue()));
            }
            if (type == Double.class) {
                return type.cast(Double.valueOf(number.doubleValue()));
            }
            if (type == Float.class) {
                return type.cast(Float.valueOf(number.floatValue()));
            }
            if (type == Short.class) {
                return type.cast(Short.valueOf(number.shortValue()));
            }
            if (type == Byte.class) {
                return type.cast(Byte.valueOf(number.byteValue()));
            }
            if (type == BigDecimal.class) {
                return type.cast(new BigDecimal(number.toString()));
            }
        }
        if (type == Boolean.class) {
            Boolean b = toBoolean(value);
            return b == null ? null : type.cast(b);
        }
        if (type == Date.class) {
            return type.cast(toSqlDate(value));
        }
        if (type == Time.class) {
            return type.cast(toSqlTime(value));
        }
        if (type == Timestamp.class) {
            return type.cast(toSqlTimestamp(value));
        }
        if (type == byte[].class && value instanceof byte[]) {
            return type.cast(((byte[]) value).clone());
        }
        throw new SQLException("Cannot convert column " + columnIndex + " to type " + type.getName());
    }

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("ResultSet is closed");
        }
    }

    private Object getColumnValue(int columnIndex) throws SQLException {
        ensureOpen();
        ensureValidColumnIndex(columnIndex);
        if (cursor < 0 || cursor >= rows.size()) {
            throw new SQLException("Cursor is not positioned on a row");
        }
        Object value = rows.get(cursor)[columnIndex - 1];
        wasNull = value == null;
        return value;
    }

    private Object getColumnValue(String columnLabel) throws SQLException {
        ensureOpen();
        Integer index = columnIndexes.get(columnLabel);
        if (index == null) {
            throw new SQLException("Column not found: " + columnLabel);
        }
        return getColumnValue(index + 1);
    }

    private void ensureValidColumnIndex(int columnIndex) throws SQLException {
        if (columnIndex < 1 || columnIndex > columnNames.size()) {
            throw new SQLException("Column index out of range: " + columnIndex);
        }
    }

    private Number toNumber(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Number) {
            return (Number) value;
        }
        if (value instanceof String) {
            try {
                return new BigDecimal((String) value);
            } catch (NumberFormatException ex) {
                throw new SQLException("Failed to convert value to number: " + value, ex);
            }
        }
        throw new SQLException("Cannot convert value to number: " + value.getClass().getName());
    }

    private Boolean toBoolean(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue() != 0;
        }
        if (value instanceof String) {
            String normalized = ((String) value).trim().toLowerCase(java.util.Locale.ROOT);
            if (normalized.isEmpty()) {
                return null;
            }
            if (normalized.equals("true") || normalized.equals("1") || normalized.equals("yes")) {
                return true;
            }
            if (normalized.equals("false") || normalized.equals("0") || normalized.equals("no")) {
                return false;
            }
        }
        throw new SQLException("Cannot convert value to boolean: " + value);
    }

    private Date toSqlDate(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Date) {
            return (Date) value;
        }
        if (value instanceof java.util.Date) {
            return new Date(((java.util.Date) value).getTime());
        }
        if (value instanceof Number) {
            return new Date(((Number) value).longValue());
        }
        if (value instanceof String) {
            String str = (String) value;
            try {
                if (str.length() == 10 && str.charAt(4) == '-' && str.charAt(7) == '-') {
                    return Date.valueOf(str);
                }
                Instant instant = parseInstant(str);
                return new Date(instant.toEpochMilli());
            } catch (Exception e) {
                throw new SQLException("Cannot parse date value: " + str, e);
            }
        }
        throw new SQLException("Cannot convert value to Date: " + value.getClass().getName());
    }

    private Time toSqlTime(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Time) {
            return (Time) value;
        }
        if (value instanceof java.util.Date) {
            return new Time(((java.util.Date) value).getTime());
        }
        if (value instanceof Number) {
            return new Time(((Number) value).longValue());
        }
        if (value instanceof String) {
            String str = (String) value;
            try {
                if (str.length() == 8 && str.charAt(2) == ':' && str.charAt(5) == ':') {
                    return Time.valueOf(str);
                }
                Instant instant = parseInstant(str);
                return new Time(instant.toEpochMilli());
            } catch (Exception e) {
                throw new SQLException("Cannot parse time value: " + str, e);
            }
        }
        throw new SQLException("Cannot convert value to Time: " + value.getClass().getName());
    }

    private Timestamp toSqlTimestamp(Object value) throws SQLException {
        if (value == null) {
            return null;
        }
        if (value instanceof Timestamp) {
            return (Timestamp) value;
        }
        if (value instanceof java.util.Date) {
            return new Timestamp(((java.util.Date) value).getTime());
        }
        if (value instanceof Number) {
            return new Timestamp(((Number) value).longValue());
        }
        if (value instanceof String) {
            String str = (String) value;
            try {
                Instant instant = parseInstant(str);
                return Timestamp.from(instant);
            } catch (DateTimeParseException e) {
                try {
                    return Timestamp.valueOf(str);
                } catch (IllegalArgumentException ex) {
                    throw new SQLException("Cannot parse timestamp value: " + str, e);
                }
            }
        }
        throw new SQLException("Cannot convert value to Timestamp: " + value.getClass().getName());
    }

    private Instant parseInstant(String value) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException e) {
            try {
                return OffsetDateTime.parse(value).toInstant();
            } catch (DateTimeParseException ex) {
                return LocalDateTime.parse(value).atZone(ZoneId.systemDefault()).toInstant();
            }
        }
    }

    private <T extends java.util.Date> T adjustWithCalendar(T date, java.util.Calendar cal) {
        if (date == null || cal == null) {
            return date;
        }
        cal.setTimeInMillis(date.getTime());
        return date;
    }

    private SQLFeatureNotSupportedException unsupported() {
        return new SQLFeatureNotSupportedException("Operation not supported on LogsqlResultSet");
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
