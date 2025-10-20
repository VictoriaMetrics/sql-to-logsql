package com.victoriametrics.logsql.jdbc;

import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;

final class LogsqlConnectionConfig {

    static final String DEFAULT_HOST = "localhost";
    static final int DEFAULT_PORT = 8080;
    static final String DEFAULT_SCHEME = "http";
    static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(60);

    private final String host;
    private final int port;
    private final String scheme;
    private final String basePath;
    private final String endpoint;
    private final String bearerToken;
    private final Duration timeout;
    private final boolean verifyTls;
    private final Map<String, String> headers;
    private final Properties rawProperties;

    LogsqlConnectionConfig(
            String host,
            int port,
            String scheme,
            String basePath,
            String endpoint,
            String bearerToken,
            Duration timeout,
            boolean verifyTls,
            Map<String, String> headers,
            Properties rawProperties
    ) {
        this.host = Objects.requireNonNullElse(host, DEFAULT_HOST);
        this.port = port <= 0 ? DEFAULT_PORT : port;
        this.scheme = scheme == null || scheme.isBlank() ? DEFAULT_SCHEME : scheme.toLowerCase(Locale.ROOT);
        this.basePath = basePath == null ? "" : basePath;
        this.endpoint = endpoint;
        this.bearerToken = bearerToken;
        this.timeout = timeout == null ? DEFAULT_TIMEOUT : timeout;
        this.verifyTls = verifyTls;
        this.headers = headers == null ? Collections.emptyMap() : Collections.unmodifiableMap(new LinkedHashMap<>(headers));
        this.rawProperties = rawProperties;
    }

    String getHost() {
        return host;
    }

    int getPort() {
        return port;
    }

    String getScheme() {
        return scheme;
    }

    String getBasePath() {
        return basePath;
    }

    String getEndpoint() {
        return endpoint;
    }

    String getBearerToken() {
        return bearerToken;
    }

    Duration getTimeout() {
        return timeout;
    }

    boolean isVerifyTls() {
        return verifyTls;
    }

    Map<String, String> getHeaders() {
        return headers;
    }

    Properties getRawProperties() {
        return rawProperties;
    }

    DriverPropertyInfo[] toDriverPropertyInfo() throws SQLException {
        DriverPropertyInfo hostInfo = new DriverPropertyInfo("host", host);
        hostInfo.description = "sql-to-logsql service host";

        DriverPropertyInfo portInfo = new DriverPropertyInfo("port", Integer.toString(port));
        portInfo.description = "sql-to-logsql service port";

        DriverPropertyInfo schemeInfo = new DriverPropertyInfo("scheme", scheme);
        schemeInfo.description = "HTTP scheme (http or https)";

        DriverPropertyInfo endpointInfo = new DriverPropertyInfo("endpoint", endpoint);
        endpointInfo.description = "VictoriaLogs endpoint URL";

        DriverPropertyInfo bearerTokenInfo = new DriverPropertyInfo("bearerToken", bearerToken);
        bearerTokenInfo.description = "Bearer token for VictoriaLogs";

        DriverPropertyInfo timeoutInfo = new DriverPropertyInfo("timeout", Long.toString(timeout.toMillis()));
        timeoutInfo.description = "Request timeout in milliseconds";

        DriverPropertyInfo verifyInfo = new DriverPropertyInfo("verify", Boolean.toString(verifyTls));
        verifyInfo.description = "Verify TLS certificates when using HTTPS";

        return new DriverPropertyInfo[] {
                hostInfo,
                portInfo,
                schemeInfo,
                endpointInfo,
                bearerTokenInfo,
                timeoutInfo,
                verifyInfo
        };
    }
}
