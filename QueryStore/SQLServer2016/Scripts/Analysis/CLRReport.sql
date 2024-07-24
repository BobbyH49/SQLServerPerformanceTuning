SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'

SELECT
	total_clr_minutes = SUM(avg_clr_time * count_executions) / 1000000 / 60
	, total_clr_hours = SUM(avg_clr_time * count_executions) / 1000000 / 3600
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

WITH clr_cte
AS (
	SELECT
		database_name
		, query_hash
		, object_name = CASE WHEN object_name = N'NULL' OR object_name IS NULL THEN N'' ELSE schema_name + N'.' + object_name END
		, execution_count = SUM(count_executions)
		, average_rowcount = AVG(avg_rowcount)
		, min_clr_microseconds = MIN(min_clr_time)
		, avg_clr_microseconds = AVG(avg_clr_time)
		, max_clr_microseconds = MAX(max_clr_time)
		, total_clr_minutes = SUM(avg_clr_time * count_executions) / 1000000 / 60
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
	, min_clr_microseconds
	, avg_clr_microseconds
	, max_clr_microseconds
	, total_clr_minutes
	, clr_pct = 
		CASE WHEN total_clr_minutes = 0 THEN 0 ELSE
			CAST(CAST(total_clr_minutes AS DECIMAL(10,2)) * 100 / (
					SELECT SUM(avg_clr_time * count_executions) / 1000000 / 60 
					FROM ##QueryStorePerf
					WHERE start_time >= @start_time
					AND end_time <= @end_time
				) AS DECIMAL(5,2)
			)
		END
FROM clr_cte
ORDER BY
	total_clr_minutes DESC
	, database_name ASC
	, query_hash ASC;
