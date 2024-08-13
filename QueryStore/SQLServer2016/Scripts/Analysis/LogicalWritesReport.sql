SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'

SELECT
	total_logical_writes_gb = SUM(avg_logical_io_writes * count_executions) / 128 / 1024
	, total_logical_writes_tb = SUM(avg_logical_io_writes * count_executions) / 128 / 1024 / 1024
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

WITH writes_cte
AS (
	SELECT
		database_name
		, query_hash
		, object_name = CASE WHEN object_name = N'NULL' OR object_name IS NULL THEN N'' ELSE schema_name + N'.' + object_name END
		, execution_count = SUM(count_executions)
		, average_rowcount = SUM(avg_rowcount * count_executions) / SUM(count_executions)
		, min_logical_writes_mb = MIN(min_logical_io_writes) / 128
		, avg_logical_writes_mb = SUM(avg_logical_io_writes * count_executions) / SUM(count_executions) / 128
		, max_logical_writes_mb = MAX(max_logical_io_writes) / 128
		, total_logical_writes_gb = SUM(avg_logical_io_writes * count_executions) / 128 / 1024
	FROM ##QueryStorePerf
	WHERE start_time >= @start_time
	AND end_time <= @end_time
	GROUP BY
		database_name
		, query_hash
		, schema_name
		, object_name
)
SELECT TOP 50
	database_name
	, query_hash
	, object_name
	, execution_count
	, average_rowcount
	, min_logical_writes_mb
	, avg_logical_writes_mb
	, max_logical_writes_mb
	, total_logical_writes_gb
	, logical_writes_pct =
		CASE total_logical_writes_gb WHEN 0 THEN 0 ELSE
			CAST(CAST(total_logical_writes_gb AS DECIMAL(10,2)) * 100 / (
					SELECT SUM(avg_logical_io_writes * count_executions) / 128 / 1024 
					FROM ##QueryStorePerf
					WHERE start_time >= @start_time
					AND end_time <= @end_time
				) AS DECIMAL(5,2)
			)
		END
FROM writes_cte
ORDER BY
	total_logical_writes_gb DESC
	, database_name ASC
	, query_hash ASC;
