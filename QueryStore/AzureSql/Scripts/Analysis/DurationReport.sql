SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'

SELECT
	total_duration_minutes = SUM(avg_duration * count_executions) / 1000000 / 60
	, total_duration_hours = SUM(avg_duration * count_executions) / 1000000 / 3600
	, total_query_count = (
		SELECT COUNT(*)
		FROM (
			SELECT DISTINCT database_name, query_hash, object_name
			FROM ##QueryStorePerf
			WHERE start_time >= @start_time
			AND end_time <= @end_time
		) a
	  )
FROM ##QueryStorePerf
WHERE start_time >= @start_time
AND end_time <= @end_time;

WITH duration_cte
AS (
	SELECT
		database_name
		, query_hash
		, object_name = CASE WHEN object_name = N'NULL' OR object_name IS NULL THEN N'' ELSE schema_name + N'.' + object_name END
		, execution_count = SUM(count_executions)
		, average_rowcount = AVG(avg_rowcount)
		, min_duration_microseconds = MIN(min_duration)
		, avg_duration_microseconds = AVG(avg_duration)
		, max_duration_microseconds = MAX(max_duration)
		, total_duration_minutes = SUM(avg_duration * count_executions) / 1000000 / 60
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
	, min_duration_microseconds
	, avg_duration_microseconds
	, max_duration_microseconds
	, total_duration_minutes
	, duration_pct = 
		CASE WHEN total_duration_minutes = 0 THEN 0 ELSE
			CAST(CAST(total_duration_minutes AS DECIMAL(10,2)) * 100 / (
					SELECT SUM(avg_duration * count_executions) / 1000000 / 60 
					FROM ##QueryStorePerf
					WHERE start_time >= @start_time
					AND end_time <= @end_time
				) AS DECIMAL(5,2)
			)
		END
FROM duration_cte
ORDER BY
	total_duration_minutes DESC
	, database_name ASC
	, query_hash ASC;
