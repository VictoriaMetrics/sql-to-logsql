# LogSQL JDBC Driver

This module provides a JDBC 4.0 compatible driver for the VictoriaLogs (via `sql-to-logsql` service). 
The driver interacts http(s) with sql-to-logsql API and exposes query results as regular JDBC result sets, 
making it possible to integrate VictoriaLogs with the broader JVM ecosystem (BI tools, JDBC-based frameworks, etc.).

## Connection URL

```
jdbc:logsql://host[:port][/basePath]?property=value&...
```

Supported properties:

- `scheme` – `http` (default) or `https`.
- `endpoint` – optional VictoriaLogs endpoint URL override.
- `bearerToken` – optional bearer token sent to the translation service.
- `timeout` – request timeout in milliseconds (default 60000).
- `verify` – when `false`, TLS certificate validation is disabled.
- `header.<name>` – additional HTTP headers to include with every request.

Example:

```
jdbc:logsql://localhost:8080?scheme=https&endpoint=https%3A%2F%2Fvictorialogs.example.com&bearerToken=secret
```

Properties provided through `java.util.Properties` when creating the connection are merged with the URL query parameters (query parameters take precedence).

## Building

```
mvn -DskipTests package
```

The standard artifact is placed in `target/logsql-jdbc-<version>.jar`, and a fat jar with all dependencies is available as `target/logsql-jdbc-<version>-all.jar`.

## Testing

```
mvn test
```

These integration tests connect to https://play-sql.victoriametrics.com. They will be marked as skipped automatically if the playground cannot be reached (for example, when outbound network access is disabled).

## Notes

- The driver performs a health check against `/healthz` when establishing a connection.
- Result sets are fully buffered in memory to simplify cursor navigation and metadata reporting. Avoid query patterns that return unbounded result sets.
- HTTPS certificate verification can be disabled for testing by setting `verify=false`, but this is not recommended for production use.
