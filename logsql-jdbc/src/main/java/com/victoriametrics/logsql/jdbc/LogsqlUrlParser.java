package com.victoriametrics.logsql.jdbc;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

final class LogsqlUrlParser {

    private static final String HEADER_PREFIX = "header.";
    private static final String LEGACY_HEADERS_PREFIX = "headers.";

    private LogsqlUrlParser() {
    }

    static LogsqlConnectionConfig parse(String url, Properties supplied) throws SQLException {
        if (url == null || !url.startsWith(LogsqlDriver.URL_PREFIX)) {
            throw new SQLException("Invalid LogSQL JDBC URL: " + url);
        }

        Properties props = new Properties();
        if (supplied != null) {
            props.putAll(supplied);
        }

        String tail = url.substring(LogsqlDriver.URL_PREFIX.length());
        String query = null;
        int queryIndex = tail.indexOf('?');
        if (queryIndex >= 0) {
            query = tail.substring(queryIndex + 1);
            tail = tail.substring(0, queryIndex);
        }

        if (query != null && !query.isEmpty()) {
            for (Map.Entry<String, String> entry : parseQuery(query).entrySet()) {
                props.setProperty(entry.getKey(), entry.getValue());
            }
        }

        URI uri = URI.create("http://" + (tail.isEmpty() ? LogsqlConnectionConfig.DEFAULT_HOST : tail));
        String hostFromUri = uri.getHost();
        if (hostFromUri == null || hostFromUri.isEmpty()) {
            hostFromUri = props.getProperty("host", LogsqlConnectionConfig.DEFAULT_HOST);
        }
        int portFromUri = uri.getPort();
        if (portFromUri <= 0) {
            portFromUri = parsePort(props.getProperty("port"));
        }

        String basePath = uri.getPath();
        if (basePath == null) {
            basePath = "";
        } else if (basePath.equals("/")) {
            basePath = "";
        }

        String scheme = props.getProperty("scheme", LogsqlConnectionConfig.DEFAULT_SCHEME);
        String endpoint = trimToNull(props.getProperty("endpoint"));
        String bearerToken = trimToNull(props.getProperty("bearerToken"));
        Duration timeout = parseTimeout(props.getProperty("timeout"));
        boolean verify = parseBoolean(props.getProperty("verify"), true);

        Map<String, String> headers = extractHeaders(props);
        Properties raw = new Properties();
        raw.putAll(props);

        return new LogsqlConnectionConfig(
                hostFromUri,
                portFromUri <= 0 ? LogsqlConnectionConfig.DEFAULT_PORT : portFromUri,
                scheme,
                basePath,
                endpoint,
                bearerToken,
                timeout,
                verify,
                headers,
                raw
        );
    }

    private static Map<String, String> parseQuery(String query) throws SQLException {
        Map<String, String> params = new LinkedHashMap<>();
        String[] parts = query.split("&");
        for (String part : parts) {
            if (part.isEmpty()) {
                continue;
            }
            String[] kv = part.split("=", 2);
            String key = decode(kv[0]);
            String value = kv.length > 1 ? decode(kv[1]) : "";
            params.put(key, value);
        }
        return params;
    }

    private static String decode(String value) throws SQLException {
        try {
            return URLDecoder.decode(value, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("Failed to decode URL parameter", e);
        }
    }

    private static int parsePort(String port) throws SQLException {
        if (port == null || port.isEmpty()) {
            return LogsqlConnectionConfig.DEFAULT_PORT;
        }
        try {
            int value = Integer.parseInt(port);
            if (value <= 0) {
                throw new SQLException("Port must be positive: " + port);
            }
            return value;
        } catch (NumberFormatException ex) {
            throw new SQLException("Invalid port: " + port, ex);
        }
    }

    private static Duration parseTimeout(String timeout) throws SQLException {
        if (timeout == null || timeout.isEmpty()) {
            return LogsqlConnectionConfig.DEFAULT_TIMEOUT;
        }
        try {
            long millis = Long.parseLong(timeout);
            if (millis <= 0) {
                return LogsqlConnectionConfig.DEFAULT_TIMEOUT;
            }
            return Duration.ofMillis(millis);
        } catch (NumberFormatException ex) {
            throw new SQLException("Invalid timeout value: " + timeout, ex);
        }
    }

    private static boolean parseBoolean(String value, boolean defaultValue) {
        if (value == null) {
            return defaultValue;
        }
        switch (value.trim().toLowerCase(Locale.ROOT)) {
            case "true":
            case "1":
            case "yes":
            case "on":
                return true;
            case "false":
            case "0":
            case "no":
            case "off":
                return false;
            default:
                return defaultValue;
        }
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Map<String, String> extractHeaders(Properties props) {
        Map<String, String> headers = new LinkedHashMap<>();
        for (String name : props.stringPropertyNames()) {
            String lower = name.toLowerCase(Locale.ROOT);
            if (lower.startsWith(HEADER_PREFIX) || lower.startsWith(LEGACY_HEADERS_PREFIX)) {
                String cleanName;
                if (lower.startsWith(HEADER_PREFIX)) {
                    cleanName = name.substring(HEADER_PREFIX.length());
                } else {
                    cleanName = name.substring(LEGACY_HEADERS_PREFIX.length());
                }
                if (!cleanName.isEmpty()) {
                    headers.put(cleanName, props.getProperty(name));
                }
            }
        }
        return headers;
    }
}
