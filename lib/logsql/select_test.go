package logsql_test

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/VictoriaMetrics/sql-to-logsql/lib/logsql"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/sql/ast"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/sql/lexer"
	sqlparser "github.com/VictoriaMetrics/sql-to-logsql/lib/sql/parser"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/store"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/store/tablestore"
	"github.com/VictoriaMetrics/sql-to-logsql/lib/store/viewstore"
)

func parseStatement(t *testing.T, sql string) ast.Statement {
	t.Helper()

	l := lexer.New(sql)
	p := sqlparser.New(l)
	stmt := p.ParseStatement()
	if stmt == nil {
		t.Fatalf("no statement parsed for %q", sql)
	}
	if errs := p.Errors(); len(errs) > 0 {
		t.Fatalf("parser returned errors: %v", errs)
	}
	return stmt
}

func translate(t *testing.T, sql string) (string, error) {
	t.Helper()
	return translateWithTables(t, sql, map[string]string{"logs": "*"})
}

func translateWithTables(t *testing.T, sql string, tables map[string]string) (string, error) {
	t.Helper()
	return translateWithTablesAndViews(t, sql, tables, "")
}

func translateWithTablesAndViews(t *testing.T, sql string, tables map[string]string, viewsDir string) (string, error) {
	t.Helper()

	ts, err := tablestore.NewTableStore(tables)
	if err != nil {
		return "", err
	}
	var vs *viewstore.ViewStore
	if viewsDir != "" {
		vs, err = viewstore.NewViewStore(viewsDir)
		if err != nil {
			return "", err
		}
	}
	sp := store.NewStoreProvider(ts, vs)

	stmt := parseStatement(t, sql)
	si, err := logsql.GetStatementInfo(stmt, sp)
	if err != nil {
		return "", err
	}
	return si.LogsQL, nil
}

func mustTranslate(t *testing.T, sql string) string {
	t.Helper()
	return mustTranslateWithTables(t, sql, map[string]string{"logs": "*"})
}

func mustTranslateWithTables(t *testing.T, sql string, tables map[string]string) string {
	t.Helper()
	return mustTranslateWithTablesAndViews(t, sql, tables, "")
}

func mustTranslateWithTablesAndViews(t *testing.T, sql string, tables map[string]string, viewsDir string) string {
	t.Helper()
	result, err := translateWithTablesAndViews(t, sql, tables, viewsDir)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestToLogsQLSuccess(t *testing.T) {
	tests := []struct {
		name     string
		sql      string
		expected string
	}{
		{
			name:     "select all logs",
			sql:      "SELECT * FROM logs",
			expected: "*",
		},
		{
			name:     "simple equality",
			sql:      "SELECT * FROM logs WHERE level = 'error'",
			expected: "level:error",
		},
		{
			name:     "not equal or",
			sql:      "SELECT * FROM logs WHERE level != 'info' OR status = 500",
			expected: "(-level:info OR status:500)",
		},
		{
			name:     "alias and like prefix",
			sql:      "SELECT * FROM logs AS l WHERE l.level = 'error' AND l.message LIKE 'fail%'",
			expected: "(level:error AND message:fail*)",
		},
		{
			name:     "range order and limit",
			sql:      "SELECT * FROM logs WHERE _time >= '2024-01-01' ORDER BY _time DESC LIMIT 10",
			expected: "_time:>=2024-01-01 | sort by (_time desc) | limit 10",
		},
		{
			name:     "limit with offset",
			sql:      "SELECT * FROM logs ORDER BY _time LIMIT 20 OFFSET 5",
			expected: "* | sort by (_time) | offset 5 | limit 20",
		},
		{
			name:     "offset only",
			sql:      "SELECT * FROM logs OFFSET 3",
			expected: "* | offset 3",
		},
		{
			name:     "select literal without from",
			sql:      "SELECT 1",
			expected: "* | limit 1 | delete * | format 1",
		},
		{
			name:     "select literal with alias without from",
			sql:      "SELECT 1 AS one",
			expected: "* | limit 1 | delete * | format 1 as one | fields one",
		},
		{
			name:     "select literal with from",
			sql:      "SELECT 1 FROM logs",
			expected: "* | format 1 as literal_1 | fields literal_1",
		},
		{
			name:     "select string literal without from",
			sql:      "SELECT 'hello'",
			expected: "* | limit 1 | delete * | format \"hello\"",
		},
		{
			name:     "select string literal with alias without from",
			sql:      "SELECT 'hello' AS greeting",
			expected: "* | limit 1 | delete * | format \"hello\" as greeting | fields greeting",
		},
		{
			name:     "select string literal with from",
			sql:      "SELECT 'hello' FROM logs",
			expected: "* | format \"hello\" as literal_hello | fields literal_hello",
		},
		{
			name:     "select qualified star",
			sql:      "SELECT l.* FROM logs l",
			expected: "*",
		},
		{
			name:     "in list",
			sql:      "SELECT * FROM logs WHERE service IN ('api', 'worker')",
			expected: "service:(api OR worker)",
		},
		{
			name:     "is null",
			sql:      "SELECT * FROM logs WHERE host IS NULL",
			expected: "host:\"\"",
		},
		{
			name:     "is not null",
			sql:      "SELECT * FROM logs WHERE host IS NOT NULL",
			expected: "host:*",
		},
		{
			name:     "projection with fields",
			sql:      "SELECT level, message FROM logs",
			expected: "* | fields level, message",
		},
		{
			name:     "projection with rename",
			sql:      "SELECT host AS instance FROM logs",
			expected: "* | rename host as instance | fields instance",
		},
		{
			name:     "distinct single column",
			sql:      "SELECT DISTINCT level FROM logs",
			expected: "* | fields level | uniq by (level)",
		},
		{
			name:     "distinct multi column",
			sql:      "SELECT DISTINCT host, service FROM logs",
			expected: "* | fields host, service | uniq by (host, service)",
		},
		{
			name:     "function projection with alias",
			sql:      "SELECT UPPER(level) AS lvl FROM logs",
			expected: "* | format \"<uc:level>\" as lvl | fields lvl",
		},
		{
			name:     "function projection auto alias",
			sql:      "SELECT LOWER(service) FROM logs",
			expected: "* | format \"<lc:service>\" as lower_service | fields lower_service",
		},
		{
			name:     "count without alias",
			sql:      "SELECT COUNT(*) FROM logs",
			expected: "* | stats count()",
		},
		{
			name:     "count numeric literal",
			sql:      "SELECT COUNT(1) FROM logs",
			expected: "* | format 1 as __const_1 | stats count(__const_1)",
		},
		{
			name:     "sum numeric literal",
			sql:      "SELECT SUM(1) FROM logs",
			expected: "* | format 1 as __const_1 | stats sum(__const_1)",
		},
		{
			name:     "trim function",
			sql:      "SELECT TRIM(message) AS trimmed FROM logs",
			expected: "* | extract_regexp '(?s)^\\s*(?P<trimmed>.*?\\S)?\\s*$' from message | fields trimmed",
		},
		{
			name:     "ltrim auto alias",
			sql:      "SELECT LTRIM(message) FROM logs",
			expected: "* | extract_regexp '(?s)^\\s*(?P<ltrim_message>.*)$' from message | fields ltrim_message",
		},
		{
			name:     "between range",
			sql:      "SELECT * FROM logs WHERE latency BETWEEN 100 AND 200",
			expected: "latency:[100, 200]",
		},
		{
			name:     "like to regex",
			sql:      "SELECT * FROM logs WHERE message LIKE '%error_%'",
			expected: "message:~\"^.*error..*$\"",
		},
		{
			name:     "like underscore",
			sql:      "SELECT * FROM logs WHERE message LIKE '_foo'",
			expected: "message:~\"^.foo$\"",
		},
		{
			name:     "compare fields equality",
			sql:      "SELECT * FROM logs WHERE user_id = customer_id",
			expected: "user_id:eq_field(customer_id)",
		},
		{
			name:     "compare fields inequality",
			sql:      "SELECT * FROM logs WHERE duration != max_duration",
			expected: "-duration:eq_field(max_duration)",
		},
		{
			name:     "compare fields less than",
			sql:      "SELECT * FROM logs WHERE duration < max_duration",
			expected: "duration:lt_field(max_duration)",
		},
		{
			name:     "compare fields less or equal",
			sql:      "SELECT * FROM logs WHERE duration <= max_duration",
			expected: "duration:le_field(max_duration)",
		},
		{
			name:     "compare fields greater than",
			sql:      "SELECT * FROM logs WHERE duration > max_duration",
			expected: "-duration:le_field(max_duration)",
		},
		{
			name:     "compare fields greater or equal",
			sql:      "SELECT * FROM logs WHERE duration >= max_duration",
			expected: "-duration:lt_field(max_duration)",
		},
		{
			name:     "compare function fields equality",
			sql:      "SELECT * FROM logs WHERE LOWER(user) = LOWER(customer)",
			expected: "* | format \"<lc:user>\" as __filter_expr_1 | format \"<lc:customer>\" as __filter_expr_2 | filter __filter_expr_1:eq_field(__filter_expr_2) | delete __filter_expr_1, __filter_expr_2",
		},
		{
			name:     "arithmetic projection",
			sql:      "SELECT (duration_ms / 1000) AS duration_s FROM logs",
			expected: "* | math (duration_ms / 1000) as duration_s | fields duration_s",
		},
		{
			name:     "abs math function",
			sql:      "SELECT ABS(delta) FROM logs",
			expected: "* | math abs(delta) as expr_abs_delta | fields expr_abs_delta",
		},
		{
			name:     "substr projection",
			sql:      "SELECT SUBSTR(message, 2, 5) AS snippet FROM logs",
			expected: "* | extract_regexp '(?s)^.{1}(?P<snippet>.{0,5})' from message | fields snippet",
		},
		{
			name:     "upper function in where",
			sql:      "SELECT * FROM logs WHERE UPPER(level) = 'ERROR'",
			expected: "* | format \"<uc:level>\" as __filter_expr_1 | filter __filter_expr_1:ERROR | delete __filter_expr_1",
		},
		{
			name:     "lower function not equal",
			sql:      "SELECT * FROM logs WHERE LOWER(service) != 'api'",
			expected: "* | format \"<lc:service>\" as __filter_expr_1 | filter -__filter_expr_1:api | delete __filter_expr_1",
		},
		{
			name:     "lower function like prefix",
			sql:      "SELECT * FROM logs WHERE LOWER(level) LIKE 'warn%'",
			expected: "* | format \"<lc:level>\" as __filter_expr_1 | filter __filter_expr_1:warn* | delete __filter_expr_1",
		},
		{
			name:     "substr function in where",
			sql:      "SELECT * FROM logs WHERE SUBSTR(message, 1, 3) = 'foo'",
			expected: "* | extract_regexp '(?s)^.{0}(?P<__filter_expr_1>.{0,3})' from message | filter __filter_expr_1:foo | delete __filter_expr_1",
		},
		{
			name:     "concat projection",
			sql:      "SELECT CONCAT(host, ':', service) AS endpoint FROM logs",
			expected: "* | format \"<host>:<service>\" as endpoint | fields endpoint",
		},
		{
			name:     "replace function",
			sql:      "SELECT REPLACE(message, 'foo', 'bar') AS updated FROM logs",
			expected: "* | format \"<message>\" as updated | replace ('foo', 'bar') at updated | fields updated",
		},
		{
			name: "case expression",
			sql: `SELECT CASE
    WHEN score >= 90 THEN 'critical'
    WHEN score >= 75 THEN 'high'
    WHEN score >= 50 THEN 'medium'
    WHEN score >= 25 THEN 'low'
    ELSE 'info'
END AS severity FROM logs`,
			expected: "* | format \"info\" as severity | format if (score:>=25) \"low\" as severity | format if (score:>=50) \"medium\" as severity | format if (score:>=75) \"high\" as severity | format if (score:>=90) \"critical\" as severity | fields severity",
		},
		{
			name:     "case expression with operand",
			sql:      "SELECT CASE level WHEN 'error' THEN 'critical' WHEN 'warn' THEN 'warning' ELSE 'info' END AS sev FROM logs",
			expected: "* | format \"info\" as sev | format if (level:warn) \"warning\" as sev | format if (level:error) \"critical\" as sev | fields sev",
		},
		{
			name:     "json value simple path with alias",
			sql:      "SELECT JSON_VALUE(payload, '$.ip') AS ip FROM logs",
			expected: "* | unpack_json from payload fields (ip) | fields ip",
		},
		{
			name:     "json value nested path",
			sql:      "SELECT JSON_VALUE(payload, '$.user.id') AS user_id FROM logs",
			expected: "* | unpack_json from payload fields (user.id) | rename user.id as user_id | fields user_id",
		},
		{
			name:     "json value nested path without alias",
			sql:      "SELECT JSON_VALUE(payload, '$.user.id') FROM logs",
			expected: "* | unpack_json from payload fields (user.id) | rename user.id as json_value_payload_user_id | fields json_value_payload_user_id",
		},
		{
			name:     "window sum partition",
			sql:      "SELECT SUM(duration_ms) OVER (PARTITION BY service ORDER BY _time) AS running_sum FROM logs",
			expected: "* | sort by (_time) | running_stats by (service) sum(duration_ms) as running_sum | fields running_sum",
		},
		{
			name:     "window count star",
			sql:      "SELECT COUNT(*) OVER (ORDER BY _time) AS running_count FROM logs",
			expected: "* | sort by (_time) | running_stats count() as running_count | fields running_count",
		},
		{
			name:     "window count numeric literal",
			sql:      "SELECT COUNT(1) OVER (ORDER BY _time) AS running_count FROM logs",
			expected: "* | sort by (_time) | format 1 as __const_1 | running_stats count(__const_1) as running_count | fields running_count",
		},
		{
			name:     "window sum numeric literal",
			sql:      "SELECT SUM(1) OVER (ORDER BY _time) AS running_total FROM logs",
			expected: "* | sort by (_time) | format 1 as __const_1 | running_stats sum(__const_1) as running_total | fields running_total",
		},
		{
			name:     "ceil function",
			sql:      "SELECT CEIL(duration_ms / 1000.0) AS duration FROM logs",
			expected: "* | math ceil((duration_ms / 1000.0)) as duration | fields duration",
		},
		{
			name:     "greatest function",
			sql:      "SELECT GREATEST(cpu_usage, memory_usage, 50) AS max_usage FROM logs",
			expected: "* | math max(cpu_usage, memory_usage, 50) as max_usage | fields max_usage",
		},
		{
			name:     "least function",
			sql:      "SELECT LEAST(cpu_usage, 10) AS min_usage FROM logs",
			expected: "* | math min(cpu_usage, 10) as min_usage | fields min_usage",
		},
		{
			name:     "current timestamp",
			sql:      "SELECT CURRENT_TIMESTAMP FROM logs",
			expected: "* | math now() as current_timestamp_nanos | format '<time:current_timestamp_nanos>' as current_timestamp | delete current_timestamp_nanos | fields current_timestamp",
		},
		{
			name:     "current date",
			sql:      "SELECT CURRENT_DATE FROM logs",
			expected: "* | math now() as current_date_nanos | format '<time:current_date_nanos>' as current_date_formatted | extract_regexp '^(?P<current_date>[0-9]{4}-[0-9]{2}-[0-9]{2})' from current_date_formatted | delete current_date_nanos, current_date_formatted | fields current_date",
		},
		{
			name: "union all",
			sql: `SELECT * FROM logs WHERE level = 'error'
UNION ALL
SELECT * FROM logs WHERE level = 'warn'`,
			expected: "level:error | union (level:warn)",
		},
		{
			name:     "group by with having",
			sql:      "SELECT level, COUNT(*) AS total FROM logs GROUP BY level HAVING COUNT(*) > 10",
			expected: "* | stats by (level) count() total | filter total:>10",
		},
		{
			name:     "group by count numeric literal",
			sql:      "SELECT service, COUNT(1) AS total FROM logs GROUP BY service",
			expected: "* | format 1 as __const_1 | stats by (service) count(__const_1) total",
		},
		{
			name:     "group by sum numeric literal",
			sql:      "SELECT service, SUM(1) AS total FROM logs GROUP BY service",
			expected: "* | format 1 as __const_1 | stats by (service) sum(__const_1) total",
		},
		{
			name:     "group by without aggregates",
			sql:      "SELECT kubernetes.container_name FROM logs GROUP BY kubernetes.container_name",
			expected: "* | uniq by (kubernetes.container_name) | fields kubernetes.container_name",
		},
		{
			name: "having aggregate constant",
			sql:  "SELECT SUM(1) AS \"cnt_slack_079B451E84304DF1AAA4188E26F02806_ok\" FROM logs HAVING COUNT(1) > 0",
			expected: "* | format 1 as __const_1 | stats sum(__const_1) cnt_slack_079B451E84304DF1AAA4188E26F02806_ok, count(__const_1) | " +
				"filter \"count(__const_1)\":>0 | delete \"count(__const_1)\"",
		},
		{
			name: "with simple cte",
			sql: `WITH recent_errors AS (
    SELECT * FROM logs WHERE level = 'error'
)
SELECT * FROM recent_errors`,
			expected: "level:error",
		},
		{
			name: "with cte additional filter",
			sql: `WITH recent_errors AS (
    SELECT * FROM logs WHERE level = 'error'
)
SELECT user FROM recent_errors WHERE service = 'api'`,
			expected: "level:error | filter service:api | fields user",
		},
		{
			name:     "group by function expression",
			sql:      "SELECT LOWER(user) AS user_lower, COUNT(*) AS total FROM logs GROUP BY LOWER(user)",
			expected: "* | format \"<lc:user>\" as group_1 | stats by (group_1) count() total | rename group_1 as user_lower",
		},
		{
			name:     "group by select alias",
			sql:      "SELECT user AS usr, COUNT(*) AS total FROM logs GROUP BY usr",
			expected: "* | stats by (user) count() total | rename user as usr",
		},
		{
			name:     "group by function alias",
			sql:      "SELECT LOWER(user) AS user_lower, COUNT(*) AS total FROM logs GROUP BY user_lower",
			expected: "* | format \"<lc:user>\" as group_1 | stats by (group_1) count() total | rename group_1 as user_lower",
		},
		{
			name: "subquery as base table",
			sql: `SELECT *
FROM (
    SELECT *
    FROM logs
    WHERE level = 'error'
) AS recent_errors`,
			expected: "level:error",
		},
		{
			name: "subquery as base table without alias",
			sql: `SELECT *
FROM (
    SELECT *
    FROM logs
    WHERE level = 'error'
)`,
			expected: "level:error",
		},
		{
			name: "subquery as base with filter",
			sql: `SELECT recent.user, recent.fail_count
FROM (
    SELECT user, COUNT(*) AS fail_count
    FROM logs
    WHERE level = 'error'
    GROUP BY user
) AS recent
WHERE recent.fail_count > 10
ORDER BY recent.fail_count DESC
LIMIT 5`,
			expected: "level:error | stats by (user) count() fail_count | filter fail_count:>10 | fields user, fail_count | sort by (fail_count desc) | limit 5",
		},
		{
			name: "subquery as base with filter without alias",
			sql: `SELECT user, fail_count
FROM (
    SELECT user, COUNT(*) AS fail_count
    FROM logs
    WHERE level = 'error'
    GROUP BY user
)
WHERE fail_count > 10
ORDER BY fail_count DESC
LIMIT 5`,
			expected: "level:error | stats by (user) count() fail_count | filter fail_count:>10 | fields user, fail_count | sort by (fail_count desc) | limit 5",
		},
		{
			name: "join with subquery",
			sql: `SELECT l.user, m.fail_count
FROM logs AS l
INNER JOIN (
    SELECT user, COUNT(*) AS fail_count
    FROM logs
    WHERE level = 'error'
    GROUP BY user
) AS m ON l.user = m.user
WHERE l.level = 'error'
ORDER BY m.fail_count DESC
LIMIT 5`,
			expected: "level:error | join by (user) (level:error | stats by (user) count() fail_count) inner | fields user, fail_count | sort by (fail_count desc) | limit 5",
		},
		{
			name: "join with subquery without alias",
			sql: `SELECT l.user, fail_count
FROM logs AS l
INNER JOIN (
    SELECT user, COUNT(*) AS fail_count
    FROM logs
    WHERE level = 'error'
    GROUP BY user
) ON l.user = user
WHERE l.level = 'error'
ORDER BY fail_count DESC
LIMIT 5`,
			expected: "level:error | join by (user) (level:error | stats by (user) count() fail_count) inner | fields user, fail_count | sort by (fail_count desc) | limit 5",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mustTranslate(t, tt.sql)
			if got != tt.expected {
				t.Fatalf("translate mismatch:\nexpected: %s\n      got: %s", tt.expected, got)
			}
		})
	}
}

func TestToLogsQLWithConfig(t *testing.T) {
	tables := map[string]string{
		"logs":   "*",
		"errors": "* | level:ERROR",
		"api":    "service:api",
	}

	t.Run("pipeline mapping without filters", func(t *testing.T) {
		got := mustTranslateWithTables(t, "SELECT * FROM errors", tables)
		if got != "* | level:ERROR" {
			t.Fatalf("unexpected query: %s", got)
		}
	})

	t.Run("pipeline mapping with filters", func(t *testing.T) {
		got := mustTranslateWithTables(t, "SELECT * FROM errors WHERE status = 500", tables)
		if got != "* | level:ERROR | filter status:500" {
			t.Fatalf("unexpected query: %s", got)
		}
	})

	t.Run("filter mapping merges with where", func(t *testing.T) {
		got := mustTranslateWithTables(t, "SELECT * FROM api WHERE level = 'warn'", tables)
		if got != "(service:api AND level:warn)" {
			t.Fatalf("unexpected query: %s", got)
		}
	})

	t.Run("join with subquery base", func(t *testing.T) {
		sql := `SELECT recent.user, a.level
FROM (
    SELECT user
    FROM logs
    WHERE level = 'error'
) AS recent
INNER JOIN api AS a ON recent.user = a.user`
		got := mustTranslateWithTables(t, sql, tables)
		expected := "level:error | fields user | join by (user) (service:api) inner | fields user, level"
		if got != expected {
			t.Fatalf("unexpected query:\nexpected: %s\n     got: %s", expected, got)
		}
	})

	t.Run("unknown table", func(t *testing.T) {
		_, err := translateWithTables(t, "SELECT * FROM missing", tables)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "not configured") {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}

func TestToLogsQLErrors(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{
			name: "unsupported distinct",
			sql:  "SELECT DISTINCT * FROM logs",
		},
		{
			name: "non logs table",
			sql:  "SELECT * FROM users",
		},
		{
			name: "unsupported scalar function",
			sql:  "SELECT REVERSE(message) FROM logs",
		},
		{
			name: "distinct with aggregate unsupported",
			sql:  "SELECT DISTINCT COUNT(*) FROM logs",
		},
		{
			name: "union distinct unsupported",
			sql:  `SELECT * FROM logs UNION SELECT * FROM logs`,
		},
		{
			name: "join missing alias",
			sql: `SELECT *
FROM logs l
JOIN logs ON l.user = logs.user`,
		},
		{
			name: "unsupported window function",
			sql:  "SELECT AVG(duration_ms) OVER (ORDER BY _time) FROM logs",
		},
		{
			name: "window distinct unsupported",
			sql:  "SELECT SUM(DISTINCT duration_ms) OVER (ORDER BY _time) FROM logs",
		},
		{
			name: "json value array unsupported",
			sql:  "SELECT JSON_VALUE(payload, '$.items[0]') FROM logs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := translate(t, tt.sql); err == nil {
				t.Fatalf("expected error for %q", tt.sql)
			}
		})
	}
}

func TestJSONValueTranslationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		sql     string
		message string
	}{
		{
			name:    "missing second argument",
			sql:     "SELECT JSON_VALUE(payload) FROM logs",
			message: "translator: JSON_VALUE expects two arguments",
		},
		{
			name:    "non identifier first argument",
			sql:     "SELECT JSON_VALUE('payload', '$.ip') FROM logs",
			message: "translator: JSON_VALUE only supports identifiers as first argument",
		},
		{
			name:    "non string literal path",
			sql:     "SELECT JSON_VALUE(payload, 1) FROM logs",
			message: "translator: JSON_VALUE path must be string literal",
		},
		{
			name:    "invalid json path",
			sql:     "SELECT JSON_VALUE(payload, 'user.id') FROM logs",
			message: "translator: JSON path must start with $",
		},
		{
			name:    "array not supported",
			sql:     "SELECT JSON_VALUE(payload, '$.items[0]') FROM logs",
			message: "translator: JSON_VALUE path with arrays is not supported",
		},
		{
			name:    "unsupported characters in path segment",
			sql:     "SELECT JSON_VALUE(payload, '$.user.name$') FROM logs",
			message: `translator: JSON_VALUE path segment "name$" contains unsupported characters`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := translate(t, tt.sql)
			if err == nil {
				t.Fatalf("expected error for %q", tt.sql)
			}
			var te *logsql.TranslationError
			if !errors.As(err, &te) {
				t.Fatalf("expected TranslationError, got %T", err)
			}
			if te.Message != tt.message {
				t.Fatalf("unexpected error message: want %q, got %q", tt.message, te.Message)
			}
		})
	}
}

func TestCreateViewStoresFile(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]string{"logs": "*"}
	sql := "CREATE VIEW error_logs AS SELECT * FROM logs WHERE level = 'error'"
	out := mustTranslateWithTablesAndViews(t, sql, tables, dir)
	if out != "level:error" {
		t.Fatalf("expected 'level:error', got %q", out)
	}
	viewPath := filepath.Join(dir, "error_logs.logsql")
	data, err := os.ReadFile(viewPath)
	if err != nil {
		t.Fatalf("failed to read view file: %v", err)
	}
	if string(data) != "level:error\n" {
		t.Fatalf("unexpected view contents: %q", string(data))
	}
	lockPath := filepath.Join(dir, "error_logs.lock")
	if _, err := os.Stat(lockPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected lock file to be removed, got err: %v", err)
	}
}

func TestSelectFromView(t *testing.T) {
	dir := t.TempDir()
	viewPath := filepath.Join(dir, "error_logs.logsql")
	if err := os.WriteFile(viewPath, []byte("level:error\n"), 0o644); err != nil {
		t.Fatalf("failed to seed view file: %v", err)
	}
	out := mustTranslateWithTablesAndViews(t, "SELECT * FROM error_logs", nil, dir)
	if out != "level:error" {
		t.Fatalf("unexpected translation result: %q", out)
	}
}

func TestSelectFromViewWithAliasAndFilter(t *testing.T) {
	dir := t.TempDir()
	viewPath := filepath.Join(dir, "recent_errors.logsql")
	if err := os.WriteFile(viewPath, []byte("_time:>=2024-01-01\n"), 0o644); err != nil {
		t.Fatalf("failed to seed view file: %v", err)
	}
	sql := "SELECT * FROM recent_errors AS r WHERE r.level = 'warn'"
	out := mustTranslateWithTablesAndViews(t, sql, nil, dir)
	if out != "_time:>=2024-01-01 | filter level:warn" {
		t.Fatalf("unexpected translation result: %q", out)
	}
}

func TestSelectFromViewMissing(t *testing.T) {
	dir := t.TempDir()
	if _, err := translateWithTablesAndViews(t, "SELECT * FROM missing_view", nil, dir); err == nil || !strings.Contains(err.Error(), "view missing_view not found") {
		t.Fatalf("expected missing view error, got %v", err)
	}
}

func TestCreateViewOrReplace(t *testing.T) {
	dir := t.TempDir()
	tables := map[string]string{"logs": "*"}
	mustTranslateWithTablesAndViews(t, "CREATE VIEW v AS SELECT * FROM logs", tables, dir)
	out := mustTranslateWithTablesAndViews(t, "CREATE OR REPLACE VIEW v AS SELECT level FROM logs", tables, dir)
	if out != "* | fields level" {
		t.Fatalf("expected '* | fields level' query, got %q", out)
	}
	viewPath := filepath.Join(dir, "v.logsql")
	data, err := os.ReadFile(viewPath)
	if err != nil {
		t.Fatalf("failed to read view file: %v", err)
	}
	if string(data) != "* | fields level\n" {
		t.Fatalf("unexpected view contents after replace: %q", string(data))
	}
}

func TestCreateViewIfNotExists(t *testing.T) {
	dir := t.TempDir()
	viewPath := filepath.Join(dir, "v.logsql")
	if err := os.WriteFile(viewPath, []byte("original\n"), 0o644); err != nil {
		t.Fatalf("failed to seed view file: %v", err)
	}
	out := mustTranslateWithTablesAndViews(t, "CREATE VIEW IF NOT EXISTS v AS SELECT * FROM logs WHERE level = 'error'", map[string]string{"logs": "*"}, dir)
	if out != "level:error" {
		t.Fatalf("expected 'level:error' query, got %q", out)
	}
	data, err := os.ReadFile(viewPath)
	if err != nil {
		t.Fatalf("failed to read view file: %v", err)
	}
	if string(data) != "original\n" {
		t.Fatalf("view file should remain unchanged, got %q", string(data))
	}
}

func TestDropViewRemovesFile(t *testing.T) {
	dir := t.TempDir()
	viewPath := filepath.Join(dir, "error_logs.logsql")
	if err := os.WriteFile(viewPath, []byte("level:error\n"), 0o644); err != nil {
		t.Fatalf("failed to seed view file: %v", err)
	}
	out := mustTranslateWithTablesAndViews(t, "DROP VIEW error_logs", nil, dir)
	if out != "" {
		t.Fatalf("expected empty query, got %q", out)
	}
	if _, err := os.Stat(viewPath); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("expected view file to be removed, got err: %v", err)
	}
}

func TestDropViewIfExists(t *testing.T) {
	dir := t.TempDir()
	out := mustTranslateWithTablesAndViews(t, "DROP VIEW IF EXISTS missing_view", nil, dir)
	if out != "" {
		t.Fatalf("expected empty query, got %q", out)
	}
}

func TestDropViewErrors(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		needsDir   bool
		prepare    func(t *testing.T, dir string)
		wantSubstr string
	}{
		{
			name:       "no views dir",
			sql:        "DROP VIEW v",
			needsDir:   false,
			wantSubstr: "requires configured views directory",
		},
		{
			name:       "materialized view",
			sql:        "DROP MATERIALIZED VIEW v",
			needsDir:   true,
			wantSubstr: "DROP MATERIALIZED VIEW",
		},
		{
			name:     "locked view",
			sql:      "DROP VIEW v",
			needsDir: true,
			prepare: func(t *testing.T, dir string) {
				lockPath := filepath.Join(dir, "v.lock")
				if err := os.WriteFile(lockPath, []byte("locked"), 0o600); err != nil {
					t.Fatalf("failed to create lock file: %v", err)
				}
				viewPath := filepath.Join(dir, "v.logsql")
				if err := os.WriteFile(viewPath, []byte("data\n"), 0o644); err != nil {
					t.Fatalf("failed to seed view file: %v", err)
				}
			},
			wantSubstr: "locked",
		},
		{
			name:       "missing view",
			sql:        "DROP VIEW v",
			needsDir:   true,
			wantSubstr: "does not exist",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var dir string
			if tc.needsDir {
				dir = t.TempDir()
			}
			if tc.prepare != nil {
				if dir == "" {
					t.Fatal("prepare requires directory")
				}
				tc.prepare(t, dir)
			}
			_, err := translateWithTablesAndViews(t, tc.sql, nil, dir)
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCreateViewErrors(t *testing.T) {
	baseSQL := "CREATE VIEW v AS SELECT * FROM logs"
	tests := []struct {
		name       string
		sql        string
		needsDir   bool
		prepare    func(t *testing.T, dir string)
		wantSubstr string
	}{
		{
			name:       "no views dir",
			sql:        baseSQL,
			needsDir:   false,
			wantSubstr: "configured views directory",
		},
		{
			name:       "materialized view",
			sql:        "CREATE MATERIALIZED VIEW v AS SELECT * FROM logs",
			needsDir:   true,
			wantSubstr: "MATERIALIZED VIEW",
		},
		{
			name:     "locked view",
			sql:      baseSQL,
			needsDir: true,
			prepare: func(t *testing.T, dir string) {
				lockPath := filepath.Join(dir, "v.lock")
				if err := os.WriteFile(lockPath, []byte("locked"), 0o600); err != nil {
					t.Fatalf("failed to create lock file: %v", err)
				}
			},
			wantSubstr: "locked",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var dir string
			if tc.needsDir {
				dir = t.TempDir()
			}
			if tc.prepare != nil {
				if dir == "" {
					t.Fatal("prepare function requires directory")
				}
				tc.prepare(t, dir)
			}
			if _, err := translateWithTablesAndViews(t, tc.sql, map[string]string{"logs": "*"}, dir); err == nil || !strings.Contains(err.Error(), tc.wantSubstr) {
				t.Fatalf("expected error containing %q, got %v", tc.wantSubstr, err)
			}
		})
	}
}

func TestRouteTranslate(t *testing.T) {
	ts, err := tablestore.NewTableStore(map[string]string{"logs": "*"})
	if err != nil {
		t.Fatal(err)
	}
	sp := store.NewStoreProvider(ts, nil)
	stmt := parseStatement(t, "SELECT * FROM logs")
	res, err := logsql.GetStatementInfo(stmt, sp)
	if err != nil {
		t.Fatalf("GetStatementInfo failed: %v", err)
	}
	if res.Kind != logsql.StatementTypeSelect {
		t.Fatalf("unexpected route kind: %s", res.Kind)
	}
	if res.LogsQL != "*" {
		t.Fatalf("unexpected LogsQL: %s", res.LogsQL)
	}
}

func TestRouteDescribeTable(t *testing.T) {
	ts, err := tablestore.NewTableStore(map[string]string{"errors": "* | level:ERROR"})
	if err != nil {
		t.Fatal(err)
	}
	sp := store.NewStoreProvider(ts, nil)
	stmt := parseStatement(t, "DESCRIBE TABLE errors")
	res, err := logsql.GetStatementInfo(stmt, sp)
	if err != nil {
		t.Fatalf("GetStatementInfo failed: %v", err)
	}
	if res.Kind != logsql.StatementTypeDescribe {
		t.Fatalf("unexpected route kind: %s", res.Kind)
	}
	if res.LogsQL != "* | level:ERROR" {
		t.Fatalf("unexpected LogsQL: %s", res.LogsQL)
	}
}

func TestRouteDescribeView(t *testing.T) {
	dir := t.TempDir()
	ts, err := tablestore.NewTableStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	vs, err := viewstore.NewViewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	sp := store.NewStoreProvider(ts, vs)
	if err := os.WriteFile(filepath.Join(dir, "errors.logsql"), []byte("* | level:ERROR\n"), 0o644); err != nil {
		t.Fatalf("failed to write view: %v", err)
	}
	stmt := parseStatement(t, "DESCRIBE VIEW errors")
	res, err := logsql.GetStatementInfo(stmt, sp)
	if err != nil {
		t.Fatalf("GetStatementInfo failed: %v", err)
	}
	if res.Kind != logsql.StatementTypeDescribe {
		t.Fatalf("unexpected route kind: %s", res.Kind)
	}
	if res.LogsQL != "* | level:ERROR" {
		t.Fatalf("unexpected LogsQL: %s", res.LogsQL)
	}
}

func TestRouteShowTables(t *testing.T) {
	ts, err := tablestore.NewTableStore(map[string]string{
		"logs":   "*",
		"errors": "* | level:ERROR",
	})
	if err != nil {
		t.Fatal(err)
	}
	sp := store.NewStoreProvider(ts, nil)
	stmt := parseStatement(t, "SHOW TABLES")
	res, err := logsql.GetStatementInfo(stmt, sp)
	if err != nil {
		t.Fatalf("GetStatementInfo failed: %v", err)
	}
	if res.Kind != logsql.StatementTypeShowTables {
		t.Fatalf("unexpected route kind: %s", res.Kind)
	}
	if res.LogsQL != "" {
		t.Fatalf("expected empty LogsQL for SHOW TABLES, got %q", res.LogsQL)
	}
	const expected = "{\"table_name\":\"errors\",\"query\":\"* | level:ERROR\"}\n{\"table_name\":\"logs\",\"query\":\"*\"}\n"
	if res.Data != expected {
		t.Fatalf("unexpected SHOW TABLES payload:\nexpected: %s\nactual: %s", expected, res.Data)
	}
}

func TestRouteShowViews(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "errors.logsql"), []byte("* | level:ERROR\n"), 0o644); err != nil {
		t.Fatalf("failed to write errors view: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "requests.logsql"), []byte("count(*)\n"), 0o644); err != nil {
		t.Fatalf("failed to write requests view: %v", err)
	}
	ts, err := tablestore.NewTableStore(nil)
	if err != nil {
		t.Fatal(err)
	}
	vs, err := viewstore.NewViewStore(dir)
	if err != nil {
		t.Fatal(err)
	}
	sp := store.NewStoreProvider(ts, vs)
	stmt := parseStatement(t, "SHOW VIEWS")
	res, err := logsql.GetStatementInfo(stmt, sp)
	if err != nil {
		t.Fatalf("GetStatementInfo failed: %v", err)
	}
	if res.Kind != logsql.StatementTypeShowViews {
		t.Fatalf("unexpected route kind: %s", res.Kind)
	}
	if res.LogsQL != "" {
		t.Fatalf("expected empty LogsQL for SHOW VIEWS, got %q", res.LogsQL)
	}
	const expected = "{\"view_name\":\"errors\",\"query\":\"* | level:ERROR\"}\n{\"view_name\":\"requests\",\"query\":\"count(*)\"}\n"
	if res.Data != expected {
		t.Fatalf("unexpected SHOW VIEWS payload:\nexpected: %s\nactual: %s", expected, res.Data)
	}
}
