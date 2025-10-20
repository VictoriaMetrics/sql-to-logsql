package com.victoriametrics.logsql.jdbc;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.regex.Pattern;

final class LogsqlConnection implements Connection {

    private static final TypeReference<LinkedHashMap<String, Object>> MAP_TYPE =
            new TypeReference<LinkedHashMap<String, Object>>() {
            };

    private static final class TableEntry {
        final String name;
        final String type;
        final String remarks;

        TableEntry(String name, String type, String remarks) {
            this.name = name;
            this.type = type;
            this.remarks = remarks;
        }
    }

    private final LogsqlConnectionConfig config;
    private final HttpClient httpClient;
    private final ObjectMapper mapper = new ObjectMapper();
    private final String baseUrl;
    private final DatabaseMetaData metadata;
    private boolean closed;
    private boolean readOnly = true;
    private boolean autoCommit = true;

    LogsqlConnection(LogsqlConnectionConfig config) throws SQLException {
        this.config = Objects.requireNonNull(config, "config");
        this.httpClient = createHttpClient(config);
        this.baseUrl = buildBaseUrl(config);
        this.metadata = createMetadata();
        performHealthCheck();
    }

    LogsqlQueryResult executeQuery(String sql, int maxRows) throws SQLException {
        ensureOpen();
        if (sql == null) {
            throw new SQLException("SQL must not be null");
        }
        String payload;
        try {
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("sql", sql);
            if (config.getEndpoint() != null) {
                body.put("endpoint", config.getEndpoint());
            }
            if (config.getBearerToken() != null) {
                body.put("bearerToken", config.getBearerToken());
            }
            payload = mapper.writeValueAsString(body);
        } catch (JsonProcessingException e) {
            throw new SQLException("Failed to serialize request payload", e);
        }

        HttpRequest request = baseRequestBuilder(buildUri("/api/v1/sql-to-logsql"))
                .timeout(config.getTimeout())
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(payload))
                .build();

        HttpResponse<String> response = send(request);
        if (response.statusCode() >= 400) {
            throw new SQLException("Query execution failed: " + extractErrorMessage(response));
        }

        Map<String, Object> resultMap = parseJson(response.body());
        String translated = (String) resultMap.getOrDefault("logsql", null);
        String data = (String) resultMap.getOrDefault("data", "");

        List<Map<String, Object>> rows = new ArrayList<>();
        Set<String> columnOrder = new LinkedHashSet<>();
        if (data != null && !data.isBlank()) {
            String[] lines = data.split("\\r?\\n");
            for (String line : lines) {
                if (line == null) {
                    continue;
                }
                String trimmed = line.trim();
                if (trimmed.isEmpty()) {
                    continue;
                }
                Map<String, Object> row = parseRow(trimmed);
                rows.add(row);
                columnOrder.addAll(row.keySet());
            }
        }

        List<String> columns = new ArrayList<>(columnOrder);
        for (Map<String, Object> row : rows) {
            for (String column : columns) {
                row.putIfAbsent(column, null);
            }
        }

        if (maxRows > 0 && rows.size() > maxRows) {
            rows = new ArrayList<>(rows.subList(0, maxRows));
        }

        return new LogsqlQueryResult(translated, columns, rows);
    }

    private Map<String, Object> parseJson(String json) throws SQLException {
        if (json == null || json.isBlank()) {
            return Collections.emptyMap();
        }
        try {
            return mapper.readValue(json, MAP_TYPE);
        } catch (IOException e) {
            throw new SQLException("Failed to parse response JSON", e);
        }
    }

    private Map<String, Object> parseRow(String jsonLine) throws SQLException {
        try {
            return mapper.readValue(jsonLine, MAP_TYPE);
        } catch (IOException e) {
            throw new SQLException("Failed to parse response row: " + jsonLine, e);
        }
    }

    private HttpResponse<String> send(HttpRequest request) throws SQLException {
        try {
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return response;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new SQLException("Request interrupted", e);
        } catch (IOException e) {
            throw new SQLException("HTTP request failed", e);
        }
    }

    private void performHealthCheck() throws SQLException {
        HttpRequest request = baseRequestBuilder(buildUri("/healthz"))
                .timeout(Duration.ofSeconds(Math.min(5, Math.max(1, config.getTimeout().toSeconds()))))
                .GET()
                .build();

        HttpResponse<String> response = send(request);
        if (response.statusCode() >= 400) {
            throw new SQLException("Failed to connect to sql-to-logsql service at " + baseUrl
                    + ": status=" + response.statusCode());
        }
    }

    private HttpRequest.Builder baseRequestBuilder(URI uri) {
        HttpRequest.Builder builder = HttpRequest.newBuilder(uri)
                .header("Accept", "application/json");
        for (Map.Entry<String, String> header : config.getHeaders().entrySet()) {
            builder.header(header.getKey(), header.getValue());
        }
        return builder;
    }

    private static HttpClient createHttpClient(LogsqlConnectionConfig config) throws SQLException {
        HttpClient.Builder builder = HttpClient.newBuilder();
        Duration connectTimeout = config.getTimeout();
        if (connectTimeout == null || connectTimeout.isZero() || connectTimeout.isNegative()) {
            connectTimeout = LogsqlConnectionConfig.DEFAULT_TIMEOUT;
        }
        builder.connectTimeout(connectTimeout.compareTo(Duration.ofSeconds(5)) > 0
                ? Duration.ofSeconds(5)
                : connectTimeout);

        if ("https".equalsIgnoreCase(config.getScheme()) && !config.isVerifyTls()) {
            try {
                SSLContext sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, new TrustManager[]{new InsecureTrustManager()}, new SecureRandom());
                builder.sslContext(sslContext);
                SSLParameters parameters = new SSLParameters();
                parameters.setEndpointIdentificationAlgorithm("");
                builder.sslParameters(parameters);
            } catch (NoSuchAlgorithmException | KeyManagementException e) {
                throw new SQLException("Failed to configure insecure SSL context", e);
            }
        }

        return builder.build();
    }

    private String buildBaseUrl(LogsqlConnectionConfig config) {
        StringBuilder sb = new StringBuilder();
        sb.append(config.getScheme())
                .append("://")
                .append(config.getHost());
        if (config.getPort() > 0) {
            sb.append(":").append(config.getPort());
        }
        String basePath = config.getBasePath();
        if (basePath != null && !basePath.isBlank()) {
            if (!basePath.startsWith("/")) {
                sb.append('/');
            }
            sb.append(trimTrailingSlash(basePath));
        }
        return sb.toString();
    }

    private DatabaseMetaData createMetadata() {
        InvocationHandler handler = this::handleMetadataInvocation;
        return (DatabaseMetaData) Proxy.newProxyInstance(
                DatabaseMetaData.class.getClassLoader(),
                new Class[]{DatabaseMetaData.class},
                handler
        );
    }

    private Object handleMetadataInvocation(Object proxy, Method method, Object[] args) throws Throwable {
        String name = method.getName();
        switch (name) {
            case "getConnection":
                return this;
            case "getURL":
                return baseUrl;
            case "getUserName":
                return null;
            case "getDatabaseProductName":
                return "VictoriaLogs (via sql-to-logsql)";
            case "getDatabaseProductVersion":
                return "unknown";
            case "getDriverName":
                return "LogSQL JDBC Driver";
            case "getDriverVersion":
                return "0.1.0-SNAPSHOT";
            case "getDriverMajorVersion":
                return Integer.valueOf(0);
            case "getDriverMinorVersion":
                return Integer.valueOf(1);
            case "getJDBCMajorVersion":
                return Integer.valueOf(4);
            case "getJDBCMinorVersion":
                return Integer.valueOf(2);
            case "getIdentifierQuoteString":
                return "\"";
            case "supportsResultSetType":
                return Boolean.valueOf(((Integer) args[0]) == ResultSet.TYPE_FORWARD_ONLY);
            case "supportsResultSetConcurrency":
                return Boolean.valueOf(((Integer) args[0]) == ResultSet.TYPE_FORWARD_ONLY
                        && ((Integer) args[1]) == ResultSet.CONCUR_READ_ONLY);
            case "supportsBatchUpdates":
            case "supportsTransactions":
            case "supportsSchemasInTableDefinitions":
            case "supportsCatalogsInTableDefinitions":
            case "supportsStoredProcedures":
                return Boolean.FALSE;
            case "supportsMixedCaseIdentifiers":
            case "supportsMixedCaseQuotedIdentifiers":
                return Boolean.TRUE;
            case "storesLowerCaseIdentifiers":
            case "storesLowerCaseQuotedIdentifiers":
            case "storesUpperCaseIdentifiers":
            case "storesUpperCaseQuotedIdentifiers":
                return Boolean.FALSE;
            case "storesMixedCaseIdentifiers":
            case "storesMixedCaseQuotedIdentifiers":
                return Boolean.TRUE;
            case "isReadOnly":
                return Boolean.TRUE;
            case "allTablesAreSelectable":
                return Boolean.TRUE;
            case "nullsAreSortedHigh":
            case "nullsAreSortedLow":
            case "nullsAreSortedAtStart":
            case "nullsAreSortedAtEnd":
                return Boolean.FALSE;
            case "supportsResultSetHoldability":
                return Boolean.valueOf(((Integer) args[0]) == ResultSet.HOLD_CURSORS_OVER_COMMIT);
            case "getResultSetHoldability":
                return ResultSet.HOLD_CURSORS_OVER_COMMIT;
            case "getDefaultTransactionIsolation":
                return Integer.valueOf(Connection.TRANSACTION_NONE);
            case "getDatabaseMajorVersion":
            case "getDatabaseMinorVersion":
                return Integer.valueOf(0);
            case "getTableTypes":
                return buildTableTypesMetadata();
            case "getTables": {
                String tableNamePattern = args != null && args.length > 2 ? (String) args[2] : null;
                String[] tableTypes = args != null && args.length > 3 ? (String[]) args[3] : null;
                return buildTablesMetadata(tableNamePattern, tableTypes);
            }
            case "getColumns": {
                String tableNamePattern = args != null && args.length > 2 ? (String) args[2] : null;
                String columnNamePattern = args != null && args.length > 3 ? (String) args[3] : null;
                return buildColumnsMetadata(tableNamePattern, columnNamePattern);
            }
            case "getSQLKeywords":
                return "";
            case "getExtraNameCharacters":
                return "";
            case "getCatalogSeparator":
                return ".";
            case "getCatalogTerm":
            case "getSchemaTerm":
                return "";
            case "supportsGetGeneratedKeys":
            case "supportsStatementPooling":
            case "supportsSavepoints":
            case "supportsNamedParameters":
                return Boolean.FALSE;
            case "unwrap": {
                Class<?> iface = (Class<?>) args[0];
                if (iface.isInstance(proxy)) {
                    return iface.cast(proxy);
                }
                if (iface.isInstance(this)) {
                    return iface.cast(this);
                }
                throw new SQLFeatureNotSupportedException("Not a wrapper for " + iface.getName());
            }
            case "isWrapperFor": {
                Class<?> iface = (Class<?>) args[0];
                return iface.isInstance(proxy) || iface.isInstance(this);
            }
            case "toString":
                return "LogsqlDatabaseMetaDataProxy";
            case "hashCode":
                return System.identityHashCode(proxy);
            case "equals":
                return proxy == args[0];
            default:
                throw new SQLFeatureNotSupportedException(name + " is not supported");
        }
    }

    private ResultSet buildTablesMetadata(String tableNamePattern, String[] types) throws SQLException {
        List<TableEntry> tables = fetchTables(tableNamePattern, types);
        List<String> columns = List.of(
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "TABLE_TYPE",
                "REMARKS",
                "TYPE_CAT",
                "TYPE_SCHEM",
                "TYPE_NAME",
                "SELF_REFERENCING_COL_NAME",
                "REF_GENERATION"
        );
        int[] columnTypes = new int[]{
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR
        };
        List<Object[]> rows = new ArrayList<>();
        for (TableEntry table : tables) {
            rows.add(new Object[]{
                    null,
                    null,
                    table.name,
                    table.type,
                    table.remarks,
                    null,
                    null,
                    null,
                    null,
                    null
            });
        }
        return new LogsqlResultSet(null, columns, columnTypes, rows);
    }

    private ResultSet buildTableTypesMetadata() {
        List<String> columns = List.of("TABLE_TYPE");
        int[] columnTypes = new int[]{java.sql.Types.VARCHAR};
        List<Object[]> rows = new ArrayList<>();
        rows.add(new Object[]{"TABLE"});
        rows.add(new Object[]{"VIEW"});
        return new LogsqlResultSet(null, columns, columnTypes, rows);
    }

    private ResultSet buildColumnsMetadata(String tableNamePattern, String columnNamePattern) throws SQLException {
        List<String> columns = List.of(
                "TABLE_CAT",
                "TABLE_SCHEM",
                "TABLE_NAME",
                "COLUMN_NAME",
                "DATA_TYPE",
                "TYPE_NAME",
                "COLUMN_SIZE",
                "BUFFER_LENGTH",
                "DECIMAL_DIGITS",
                "NUM_PREC_RADIX",
                "NULLABLE",
                "REMARKS",
                "COLUMN_DEF",
                "SQL_DATA_TYPE",
                "SQL_DATETIME_SUB",
                "CHAR_OCTET_LENGTH",
                "ORDINAL_POSITION",
                "IS_NULLABLE",
                "SCOPE_CATALOG",
                "SCOPE_SCHEMA",
                "SCOPE_TABLE",
                "SOURCE_DATA_TYPE",
                "IS_AUTOINCREMENT",
                "IS_GENERATEDCOLUMN"
        );
        int[] columnTypes = new int[]{
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.INTEGER,
                java.sql.Types.VARCHAR,
                java.sql.Types.INTEGER,
                java.sql.Types.INTEGER,
                java.sql.Types.INTEGER,
                java.sql.Types.INTEGER,
                java.sql.Types.INTEGER,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.INTEGER,
                java.sql.Types.INTEGER,
                java.sql.Types.INTEGER,
                java.sql.Types.INTEGER,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR,
                java.sql.Types.INTEGER,
                java.sql.Types.VARCHAR,
                java.sql.Types.VARCHAR
        };

        List<Object[]> rows = new ArrayList<>();
        List<TableEntry> tables = fetchTables(tableNamePattern, null);
        for (TableEntry table : tables) {
            List<Map<String, Object>> describeRows = runDescribeCommand(table);
            int ordinal = 1;
            for (Map<String, Object> row : describeRows) {
                String columnName = stringValue(row, "field_name");
                if (columnName == null || !matchesPattern(columnName, columnNamePattern)) {
                    continue;
                }
                String hits = stringValue(row, "hits");
                rows.add(new Object[]{
                        null,
                        null,
                        table.name,
                        columnName,
                        Integer.valueOf(java.sql.Types.NULL),
                        "null",
                        null,
                        null,
                        null,
                        Integer.valueOf(10),
                        Integer.valueOf(DatabaseMetaData.columnNullable),
                        hits,
                        null,
                        null,
                        null,
                        null,
                        Integer.valueOf(ordinal++),
                        "YES",
                        null,
                        null,
                        null,
                        null,
                        "NO",
                        "NO"
                });
            }
        }

        rows.sort(Comparator.comparing((Object[] r) -> ((String) r[2]).toUpperCase(Locale.ROOT))
                .thenComparing(r -> ((String) r[3]).toUpperCase(Locale.ROOT)));
        return new LogsqlResultSet(null, columns, columnTypes, rows);
    }

    private List<TableEntry> fetchTables(String tableNamePattern, String[] types) throws SQLException {
        List<TableEntry> result = new ArrayList<>();
        if (isTableTypeIncluded(types, "TABLE")) {
            for (Map<String, Object> entry : runShowCommand("SHOW TABLES")) {
                String name = stringValue(entry, "table_name");
                if (name == null || !matchesPattern(name, tableNamePattern)) {
                    continue;
                }
                String remarks = stringValue(entry, "query");
                result.add(new TableEntry(name, "TABLE", remarks));
            }
        }
        if (isTableTypeIncluded(types, "VIEW")) {
            for (Map<String, Object> entry : runShowCommand("SHOW VIEWS")) {
                String name = stringValue(entry, "view_name");
                if (name == null || !matchesPattern(name, tableNamePattern)) {
                    continue;
                }
                String remarks = stringValue(entry, "query");
                result.add(new TableEntry(name, "VIEW", remarks));
            }
        }
        result.sort(Comparator.comparing(t -> t.name.toUpperCase(Locale.ROOT)));
        return result;
    }

    private List<Map<String, Object>> runShowCommand(String sql) throws SQLException {
        LogsqlQueryResult result = executeQuery(sql, 0);
        return result.getRows();
    }

    private List<Map<String, Object>> runDescribeCommand(TableEntry table) throws SQLException {
        String sql = ("VIEW".equalsIgnoreCase(table.type) ? "DESCRIBE VIEW " : "DESCRIBE TABLE ") + table.name;
        return executeQuery(sql, 0).getRows();
    }

    private boolean isTableTypeIncluded(String[] requestedTypes, String candidateType) {
        if (requestedTypes == null || requestedTypes.length == 0) {
            return true;
        }
        for (String type : requestedTypes) {
            if (type == null || type.isEmpty()) {
                return true;
            }
            if (type.equalsIgnoreCase(candidateType)) {
                return true;
            }
        }
        return false;
    }

    private boolean matchesPattern(String value, String pattern) {
        if (pattern == null || pattern.isEmpty()) {
            return true;
        }
        if (value == null) {
            return false;
        }
        String upperValue = value.toUpperCase(Locale.ROOT);
        String upperPattern = pattern.toUpperCase(Locale.ROOT);

        StringBuilder regex = new StringBuilder();
        regex.append('^');
        StringBuilder literal = new StringBuilder();
        for (int i = 0; i < upperPattern.length(); i++) {
            char c = upperPattern.charAt(i);
            if (c == '%') {
                if (literal.length() > 0) {
                    regex.append(Pattern.quote(literal.toString()));
                    literal.setLength(0);
                }
                regex.append(".*");
            } else if (c == '_') {
                if (literal.length() > 0) {
                    regex.append(Pattern.quote(literal.toString()));
                    literal.setLength(0);
                }
                regex.append('.');
            } else {
                literal.append(c);
            }
        }
        if (literal.length() > 0) {
            regex.append(Pattern.quote(literal.toString()));
        }
        regex.append('$');
        return upperValue.matches(regex.toString());
    }

    private String stringValue(Map<String, Object> entry, String key) {
        Object value = entry.get(key);
        return value == null ? null : value.toString();
    }

    private URI buildUri(String path) {
        String normalizedPath = path == null ? "" : path;
        if (!normalizedPath.startsWith("/")) {
            normalizedPath = "/" + normalizedPath;
        }
        return URI.create(baseUrl + normalizedPath);
    }

    private static String trimTrailingSlash(String value) {
        if (value == null) {
            return null;
        }
        int len = value.length();
        while (len > 0 && value.charAt(len - 1) == '/') {
            len--;
        }
        return value.substring(0, len);
    }

    private String extractErrorMessage(HttpResponse<String> response) {
        String body = response.body();
        if (body == null || body.isBlank()) {
            return "status=" + response.statusCode();
        }
        try {
            Map<String, Object> map = mapper.readValue(body, MAP_TYPE);
            Object error = map.get("error");
            if (error != null) {
                return error.toString();
            }
        } catch (IOException ignored) {
            // fall back to raw body
        }
        return body;
    }

    private void ensureOpen() throws SQLException {
        if (closed) {
            throw new SQLException("Connection is closed");
        }
    }

    @Override
    public Statement createStatement() throws SQLException {
        ensureOpen();
        return new LogsqlStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        ensureOpen();
        return new LogsqlPreparedStatement(this, sql);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported");
    }

    @Override
    public String nativeSQL(String sql) {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() {
        return autoCommit;
    }

    @Override
    public void commit() {
        // no-op: read only
    }

    @Override
    public void rollback() {
        // no-op: read only
    }

    @Override
    public void close() {
        closed = true;
    }

    @Override
    public boolean isClosed() {
        return closed;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        ensureOpen();
        return metadata;
    }

    @Override
    public void setReadOnly(boolean readOnly) {
        this.readOnly = readOnly;
    }

    @Override
    public boolean isReadOnly() {
        return readOnly;
    }

    @Override
    public void setCatalog(String catalog) {
        // no-op
    }

    @Override
    public String getCatalog() {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) {
        // no-op
    }

    @Override
    public int getTransactionIsolation() {
        return Connection.TRANSACTION_NONE;
    }

    @Override
    public SQLWarning getWarnings() {
        return null;
    }

    @Override
    public void clearWarnings() {
        // no warnings to clear
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        ensureOpen();
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Only forward-only, read-only result sets are supported");
        }
        return new LogsqlStatement(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        if (resultSetType != ResultSet.TYPE_FORWARD_ONLY || resultSetConcurrency != ResultSet.CONCUR_READ_ONLY) {
            throw new SQLFeatureNotSupportedException("Only forward-only, read-only result sets are supported");
        }
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported");
    }

    @Override
    public Map<String, Class<?>> getTypeMap() {
        return Collections.emptyMap();
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Custom type maps are not supported");
    }

    @Override
    public void setHoldability(int holdability) throws SQLFeatureNotSupportedException {
        if (holdability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("Holdability " + holdability + " not supported");
        }
    }

    @Override
    public int getHoldability() {
        return ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public Savepoint setSavepoint() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Savepoints are not supported");
    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("Holdability " + resultSetHoldability + " not supported");
        }
        return createStatement(resultSetType, resultSetConcurrency);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        if (resultSetHoldability != ResultSet.HOLD_CURSORS_OVER_COMMIT) {
            throw new SQLFeatureNotSupportedException("Holdability " + resultSetHoldability + " not supported");
        }
        return prepareStatement(sql, resultSetType, resultSetConcurrency);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        throw new SQLFeatureNotSupportedException("Callable statements are not supported");
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        if (autoGeneratedKeys != Statement.NO_GENERATED_KEYS) {
            throw new SQLFeatureNotSupportedException("Auto-generated keys are not supported");
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        if (columnIndexes != null && columnIndexes.length > 0) {
            throw new SQLFeatureNotSupportedException("Auto-generated keys are not supported");
        }
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        if (columnNames != null && columnNames.length > 0) {
            throw new SQLFeatureNotSupportedException("Auto-generated keys are not supported");
        }
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Clob not supported");
    }

    @Override
    public Blob createBlob() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Blob not supported");
    }

    @Override
    public NClob createNClob() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("NClob not supported");
    }

    @Override
    public SQLXML createSQLXML() throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("SQLXML not supported");
    }

    @Override
    public boolean isValid(int timeout) {
        return !closed;
    }

    @Override
    public void setClientInfo(String name, String value) {
        // ignored
    }

    @Override
    public void setClientInfo(Properties properties) {
        // ignored
    }

    @Override
    public String getClientInfo(String name) {
        return config.getRawProperties().getProperty(name);
    }

    @Override
    public Properties getClientInfo() {
        Properties copy = new Properties();
        copy.putAll(config.getRawProperties());
        return copy;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Arrays are not supported");
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLFeatureNotSupportedException {
        throw new SQLFeatureNotSupportedException("Structs are not supported");
    }

    @Override
    public void setSchema(String schema) {
        // no-op
    }

    @Override
    public String getSchema() {
        return null;
    }

    @Override
    public void abort(java.util.concurrent.Executor executor) {
        close();
    }

    @Override
    public void setNetworkTimeout(java.util.concurrent.Executor executor, int milliseconds) throws SQLException {
        if (milliseconds <= 0) {
            throw new SQLException("Network timeout must be positive");
        }
    }

    @Override
    public int getNetworkTimeout() {
        return (int) config.getTimeout().toMillis();
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

    LogsqlConnectionConfig getConfig() {
        return config;
    }

    HttpClient getHttpClient() {
        return httpClient;
    }

    ObjectMapper getMapper() {
        return mapper;
    }

    private static final class InsecureTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {
            // trust all
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {
            // trust all
        }

        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
        }
    }
}
