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


## Integrations

### DBeaver

1. Add maven repo for sql-to-logsql:

- 1.1. Menu -> Settings -> Connections -> Drivers -> Maven -> Add -> `https://maven.pkg.github.com/victoriametrics/sql-to-logsql`:

  ![Add Maven repo to DBeaver](dbeaver-1.png)

- 1.2. Set username and password according to [these docs](https://docs.github.com/en/packages/working-with-a-github-packages-registry/working-with-the-apache-maven-registry#authenticating-with-a-personal-access-token) (username - your github username, password - personal access token with "read packages" permission):

  ![Set repo auth in DBEaver](dbeaver-2.png)

2. Menu -> Database -> Driver Manager -> New:

- 2.1. Tabs -> Libraries -> Add artifact:

  ![Add maven artifactor to DBEaver](dbeaver-3.png)

  ```xml
  <dependency>
      <groupId>com.victoriametrics</groupId>
      <artifactId>logsql-jdbc</artifactId>
      <version>0.7.0</version>
  </dependency>
  ```

- 2.2. Click "Download/Update" and Click "Find class":

  ![Download artifact](dbeaver-4.png)

- 2.3. Fill in the form on "Settings" tab:

  ![Fill in settings](dbeaver-5.png)

    - Driver name: `LogsQL`
    - Driver type: `Generic`
    - Class name: `com.victoriametrics.logsql.jdbc.LogsqlDriver`
    - URL template: `jdbc:logsql://{host}[:{port}]`
    - Default port: `8080`
    - No authentication: `true`

3. Add a new connection - for example, you can check it out with our [playground](https://play-sql.victoriametrics.com):

  ![Add new connection](dbeaver-6.png)

  URL: `jdbc:logsql://play-sql.victoriametrics.com:443?scheme=https`

  ![Set playgorund URL](dbeaver-7.png)

  After that you can explore the database via DBeaver UI:

  ![Explore data](dbeaver-8.png)

### JetBrains IDEs

1. Download `logsql-jdbc-0.7.0-all.jar` file from [maven package](https://github.com/VictoriaMetrics/sql-to-logsql/packages/2708242): [download file](https://github-registry-files.githubusercontent.com/1068003892/ea4eae80-adbb-11f0-9457-605837395e31?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20251020%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20251020T150325Z&X-Amz-Expires=300&X-Amz-Signature=992043563ba10abf6b5b9615e4493032c81a02bca67254291b3f1416a7fccae3&X-Amz-SignedHeaders=host&response-content-disposition=filename%3Dlogsql-jdbc-0.7.0-all.jar&response-content-type=application%2Foctet-stream)

2. "Database" panel -> Create Data Source -> Driver:

  - 2.1.
   
    ![Add driver](jetbrains-1.png)

  - 2.2. Add `logsql-jdbc-0.7.0-all.jar` file as "Custom JAR" and select driver class
   
    ![Add driver](jetbrains-2.png)

  - 2.3. Add "default" URL template: `jdbc:logsql://host[:port]`

3. Create a new data source - for example, you can check it out with our [playground](https://play-sql.victoriametrics.com):

  ![Create data source](jetbrains-3.png)

  URL: `jdbc:logsql://play-sql.victoriametrics.com:443?scheme=https`

4. Now you can explore the database via JetBrains IDEs:

  ![Explore data](jetbrains-4.png)

### Tableau

1. Download `logsql-jdbc-0.7.0-all.jar` file from [maven package](https://github.com/VictoriaMetrics/sql-to-logsql/packages/2708242): [download file](https://github-registry-files.githubusercontent.com/1068003892/ea4eae80-adbb-11f0-9457-605837395e31?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIAVCODYLSA53PQK4ZA%2F20251020%2Fus-east-1%2Fs3%2Faws4_request&X-Amz-Date=20251020T150325Z&X-Amz-Expires=300&X-Amz-Signature=992043563ba10abf6b5b9615e4493032c81a02bca67254291b3f1416a7fccae3&X-Amz-SignedHeaders=host&response-content-disposition=filename%3Dlogsql-jdbc-0.7.0-all.jar&response-content-type=application%2Foctet-stream)

2. Put the `logsql-jdbc-0.7.0-all.jar` file into the [Tableau Drivers folder](https://help.tableau.com/current/pro/desktop/en-us/examples_otherdatabases_jdbc.htm#specify-the-right-jdbc-driver)

3. Connect -> To a Server -> Other databases (JDBC):

  ![Connect to server](tableau-1.png)
  
4. Set the URL, for example, you can check it out with our [playground](https://play-sql.victoriametrics.com):

  ![Connect to server](tableau-2.png)

  URL: `jdbc:logsql://play-sql.victoriametrics.com:443?scheme=https`

5. Now you can explore the database via Tableau:

  ![Explore data](tableau-3.png)
  
