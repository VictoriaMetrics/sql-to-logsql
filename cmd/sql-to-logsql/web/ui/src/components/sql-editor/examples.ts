export const DEFAULT_EXAMPLE_ID = "last_100";

export const EXAMPLES = [
    {
        id: "list_tables",
        title: "Show tables",
        sql: `SHOW TABLES;`,
    },
    {
        id: "last_100",
        title: "Last 100 items",
        sql: `SELECT _time, _msg
FROM logs
ORDER BY _time DESC
LIMIT 100`,
    },
    {
        id: "last_100_warnings",
        title: "Last 100 warnings",
        sql: `SELECT
  _time AS "Timestamp",
  SUBSTR(log.level, 1, 4) AS "Level",
  _msg AS "Message"
FROM logs
WHERE log.level LIKE 'warn%'
ORDER BY _time DESC
LIMIT 100`,
    },
    {
        id: "grouping",
        title: "Grouping",
        sql: `SELECT 
    kubernetes.container_name AS container, 
    COUNT(*) AS messages_count
FROM logs
WHERE kubernetes.container_name IS NOT NULL
GROUP BY container
HAVING messages_count > 10
ORDER BY messages_count DESC`
    },
    {
        id: "create_view",
        title: "Create view",
        sql: `CREATE OR REPLACE VIEW slack_messages AS 
    SELECT 
        _time as ts, 
        channel_name, 
        display_name as user, 
        _msg as message
    FROM logs
    WHERE   channel_id IS NOT NULL
        AND channel_name IS NOT NULL
    ORDER BY ts DESC`,
    },
    {
        id: "list_views",
        title: "Show views",
        sql: `SHOW VIEWS;`,
    },
    {
        id: 'select_from_view',
        title: 'Select view',
        sql: `SELECT * 
    FROM slack_messages 
    LIMIT 100`,
    },
    {
        id: 'describe_view',
        title: 'Describe view',
        sql: `DESCRIBE VIEW slack_messages`,
    },
    {
        id: "drop_view",
        title: "Drop view",
        sql: `DROP VIEW IF EXISTS slack_messages`,
    },
    {
        id: "group_cte",
        title: "Group + Subquery (CTE)",
        sql: `WITH container_stats AS (
    SELECT kubernetes.container_name AS container, COUNT(*) AS total
    FROM logs
    GROUP BY kubernetes.container_name
    LIMIT 20
)
SELECT UPPER(container), total
FROM container_stats
WHERE container IS NOT NULL
ORDER BY total DESC`,
    },
    {
        id: "case",
        title: "Case expression",
        sql: `SELECT
    x.severity,
    COUNT(x.severity)
FROM (
    SELECT
        CASE log.level
            WHEN 'dpanic' THEN 'critical'
            WHEN 'panic' THEN 'critical'
            WHEN 'error' THEN 'high'
            WHEN 'warn' THEN 'medium'
            WHEN 'info' THEN 'low'
            ELSE 'unknown'
        END as severity
    FROM logs
    WHERE log.level IS NOT NULL
) x
GROUP BY x.severity`,
    },
    {
        id: "subqueries",
        title: "Subqueries",
        sql: `SELECT UPPER(container), total
FROM (
    SELECT kubernetes.container_name AS container, COUNT(*) AS total
    FROM logs
    GROUP BY kubernetes.container_name
    LIMIT 20
) container_stats
WHERE container IS NOT NULL
ORDER BY total DESC`,
    },
    {
        id: "window_functions",
        title: "Window (analytic) functions",
        sql: `SELECT 
    _time as ts, 
    channel_name, 
    display_name as user, 
    thread_id as thread_id,
    COUNT(*) OVER (PARTITION BY thread_id ORDER BY ts) as thread_message_num,
    _msg as message
FROM logs
WHERE   channel_id IS NOT NULL
    AND channel_name IS NOT NULL
ORDER BY thread_id, ts
LIMIT 100`
    },
    {
        id: 'describe_table',
        title: 'Describe table',
        sql: `DESCRIBE TABLE logs`,
    },
];