package com.victoriametrics.logsql.jdbc;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;

import static org.junit.jupiter.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class LogsqlIntegrationTest {

    private static final String PLAYGROUND_URL = "jdbc:logsql://play-sql.victoriametrics.com?scheme=https";

    private boolean playgroundAvailable;
    private String playgroundFailureMessage;

    @BeforeAll
    public void ensurePlaygroundAvailable() throws Exception {
        Class.forName("com.victoriametrics.logsql.jdbc.LogsqlDriver");
        try (Connection ignored = DriverManager.getConnection(PLAYGROUND_URL)) {
            // connection ok
            this.playgroundAvailable = true;
        } catch (SQLException e) {
            this.playgroundAvailable = false;
            this.playgroundFailureMessage = e.getMessage();
            System.err.println("[LogsqlIntegrationTest] Playground connection failed: " + e.getMessage());
        }
    }

    @org.junit.jupiter.api.BeforeEach
    public void checkPlaygroundAvailable() {
        Assumptions.assumeTrue(playgroundAvailable, () -> "play-sql.victoriametrics.com is unavailable: " + playgroundFailureMessage);
    }

    @Test
    public void simpleQueryReturnsDataAndMetadata() throws SQLException {
        try (Connection conn = DriverManager.getConnection(PLAYGROUND_URL);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT * FROM logs LIMIT 5")) {

            assertTrue(stmt instanceof LogsqlStatement);
            LogsqlStatement logsqlStmt = (LogsqlStatement) stmt;
            String translated = logsqlStmt.getTranslatedLogsql();
            assertNotNull(translated);
            assertTrue(translated.contains("limit 5"));

            assertTrue(rs.next(), "Expected at least one row from playground data");
            Timestamp timestamp = rs.getTimestamp("_time");
            assertNotNull(timestamp, "_time column should be convertible to Timestamp");

            ResultSetMetaData meta = rs.getMetaData();
            assertTrue(meta.getColumnCount() > 0);
            assertNotNull(meta.getColumnName(1));
        }
    }

    @Test
    public void preparedStatementBindsParameters() throws SQLException {
        try (Connection conn = DriverManager.getConnection(PLAYGROUND_URL)) {
            String repoName;
            try (PreparedStatement sample = conn.prepareStatement(
                    "SELECT repo.name FROM logs WHERE repo.name IS NOT NULL LIMIT 1");
                 ResultSet rs = sample.executeQuery()) {
                assertTrue(rs.next(), "Expected a repo.name value to exist");
                repoName = rs.getString(1);
                assertNotNull(repoName);
            }

            try (PreparedStatement countStmt = conn.prepareStatement(
                    "SELECT COUNT(*) AS cnt FROM logs WHERE repo.name = ?")) {
                countStmt.setString(1, repoName);
                try (ResultSet rs = countStmt.executeQuery()) {
                    assertTrue(rs.next(), "Expected at least one row for the repo");
                    long count = rs.getLong("cnt");
                    assertTrue(count >= 1, "Expected count >= 1 for repo " + repoName);
                    assertFalse(rs.next(), "Expected single row result for COUNT(*)");
                }

                LogsqlStatement logsqlStmt = (LogsqlStatement) countStmt;
                String translated = logsqlStmt.getTranslatedLogsql();
                assertNotNull(translated);
                assertTrue(translated.contains("repo.name"));
            }
        }
    }
}
