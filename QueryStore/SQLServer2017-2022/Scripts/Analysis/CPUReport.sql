SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'
	, @vCores int = 24

SELECT
	total_cpu_minutes = SUM(avg_cpu_time * count_executions) / 1000000 / 60
	, average_cpu_pct = CAST(CAST(SUM(avg_cpu_time * count_executions) AS DECIMAL(20, 2)) * 100 / (SELECT CAST(DATEDIFF(hh, @start_time, @end_time) AS BIGINT) * 60 * 60 * 1000000 * @vCores) AS DECIMAL(5, 2))
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

WITH cpu_cte
AS (
	SELECT
		database_name
		, query_hash
		, object_name = CASE WHEN object_name = N'NULL' OR object_name IS NULL THEN N'' ELSE schema_name + N'.' + object_name END
		, execution_count = SUM(count_executions)
		, average_rowcount = SUM(avg_rowcount * count_executions) / SUM(count_executions)
		, min_cpu_microseconds = MIN(min_cpu_time)
		, avg_cpu_microseconds = SUM(avg_cpu_time * count_executions) / SUM(count_executions)
		, max_cpu_microseconds = MAX(max_cpu_time)
		, total_cpu_minutes = SUM(avg_cpu_time * count_executions) / 1000000 / 60
		, average_dop = SUM(avg_dop * count_executions) / SUM(count_executions)
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
	, min_cpu_microseconds
	, avg_cpu_microseconds
	, max_cpu_microseconds
	, total_cpu_minutes
	, cpu_pct = 
		CASE WHEN total_cpu_minutes = 0 THEN 0 ELSE
			CAST(CAST(total_cpu_minutes AS DECIMAL(10,2)) * 100 / (
					SELECT SUM(avg_cpu_time * count_executions) / 1000000 / 60 
					FROM ##QueryStorePerf
					WHERE start_time >= @start_time
					AND end_time <= @end_time
				) AS DECIMAL(5,2)
			)
		END
	, average_dop
FROM cpu_cte
ORDER BY
	total_cpu_minutes DESC
	, database_name ASC
	, query_hash ASC;
