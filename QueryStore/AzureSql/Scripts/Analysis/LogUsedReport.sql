SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'

SELECT
	total_log_used_mb = SUM(avg_log_bytes_used * count_executions) / 1024 / 1024
	, total_log_used_gb = SUM(avg_log_bytes_used * count_executions) / 1024 / 1024 / 1024
	, total_query_count = (
		SELECT COUNT(*)
		FROM (
			SELECT DISTINCT database_name, query_hash, schema_name, object_name
			FROM ##QueryStorePerf
			WHERE start_time >= @start_time
			AND end_time <= @end_time
		) a
	  )
FROM ##QueryStorePerf
WHERE start_time >= @start_time
AND end_time <= @end_time;

WITH used_cte
AS (
	SELECT
		database_name
		, query_hash
		, object_name = CASE WHEN object_name = N'NULL' OR object_name IS NULL THEN N'' ELSE schema_name + N'.' + object_name END
		, execution_count = SUM(count_executions)
		, average_rowcount = SUM(avg_rowcount * count_executions) / SUM(count_executions)
		, min_log_used_mb = MIN(min_log_bytes_used) / 1024 / 1024
		, avg_log_used_mb = SUM(avg_log_bytes_used * count_executions) / SUM(count_executions) / 1024 / 1024
		, max_log_used_mb = MAX(max_log_bytes_used) / 1024 / 1024
		, total_log_used_mb = SUM(avg_log_bytes_used * count_executions) / 1024 / 1024
	FROM ##QueryStorePerf
	WHERE start_time >= @start_time
	AND end_time <= @end_time
	GROUP BY
		database_name
		, query_hash
		, schema_name
		, object_name
)
SELECT
	database_name
	, query_hash
	, object_name
	, execution_count
	, average_rowcount
	, min_log_used_mb
	, avg_log_used_mb
	, max_log_used_mb
	, total_log_used_mb
	, log_used_pct =
		CASE total_log_used_mb WHEN 0 THEN 0 ELSE
			CAST(CAST(total_log_used_mb AS DECIMAL(15,2)) * 100 / (
					SELECT SUM(avg_log_bytes_used * count_executions) / 1024 / 1024
					FROM ##QueryStorePerf
					WHERE start_time >= @start_time
					AND end_time <= @end_time
				) AS DECIMAL(5,2)
			)
		END
FROM used_cte
ORDER BY
	total_log_used_mb DESC
	, database_name ASC
	, query_hash ASC;
