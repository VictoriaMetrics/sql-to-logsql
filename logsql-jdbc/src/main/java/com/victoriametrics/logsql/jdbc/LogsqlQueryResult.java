package com.victoriametrics.logsql.jdbc;

import java.util.Collections;
import java.util.List;
import java.util.Map;

final class LogsqlQueryResult {

    private final String logsql;
    private final List<String> columnNames;
    private final List<Map<String, Object>> rows;

    LogsqlQueryResult(String logsql, List<String> columnNames, List<Map<String, Object>> rows) {
        this.logsql = logsql;
        this.columnNames = columnNames == null ? Collections.emptyList() : Collections.unmodifiableList(columnNames);
        this.rows = rows == null ? Collections.emptyList() : Collections.unmodifiableList(rows);
    }

    String getLogsql() {
        return logsql;
    }

    List<String> getColumnNames() {
        return columnNames;
    }

    List<Map<String, Object>> getRows() {
        return rows;
    }

    int getRowCount() {
        return rows.size();
    }
}
