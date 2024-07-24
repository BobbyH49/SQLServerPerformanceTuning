SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'

SELECT
	total_tempdb_space_used_mb = SUM(avg_tempdb_space_used * count_executions) / 128
	, total_tempdb_space_used_gb = SUM(avg_tempdb_space_used * count_executions) / 128 / 1024
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

WITH used_cte
AS (
	SELECT
		database_name
		, query_hash
		, object_name = CASE WHEN object_name = N'NULL' OR object_name IS NULL THEN N'' ELSE schema_name + N'.' + object_name END
		, execution_count = SUM(count_executions)
		, average_rowcount = AVG(avg_rowcount)
		, min_tempdb_space_used_mb = MIN(min_tempdb_space_used) / 128
		, avg_tempdb_space_used_mb = AVG(avg_tempdb_space_used) / 128
		, max_tempdb_space_used_mb = MAX(max_tempdb_space_used) / 128
		, total_tempdb_space_used_mb = SUM(avg_tempdb_space_used * count_executions) / 128
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
	, min_tempdb_space_used_mb
	, avg_tempdb_space_used_mb
	, max_tempdb_space_used_mb
	, total_tempdb_space_used_mb
	, tempdb_space_used_pct =
		CASE total_tempdb_space_used_mb WHEN 0 THEN 0 ELSE
			CAST(CAST(total_tempdb_space_used_mb AS DECIMAL(10,2)) * 100 / (
					SELECT SUM(avg_tempdb_space_used * count_executions) / 128
					FROM ##QueryStorePerf
					WHERE start_time >= @start_time
					AND end_time <= @end_time
				) AS DECIMAL(5,2)
			)
		END
FROM used_cte
ORDER BY
	total_tempdb_space_used_mb DESC
	, database_name ASC
	, query_hash ASC;
