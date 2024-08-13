SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'

SELECT
	total_physical_reads_io = SUM(avg_num_physical_io_reads * count_executions)
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

WITH physical_cte
AS (
	SELECT
		database_name
		, query_hash
		, object_name = CASE WHEN object_name = N'NULL' OR object_name IS NULL THEN N'' ELSE schema_name + N'.' + object_name END
		, execution_count = SUM(count_executions)
		, average_rowcount = SUM(avg_rowcount * count_executions) / SUM(count_executions)
		, min_physical_reads_io = MIN(min_num_physical_io_reads)
		, avg_physical_reads_io = SUM(avg_num_physical_io_reads * count_executions) / SUM(count_executions)
		, max_physical_reads_io = MAX(max_num_physical_io_reads)
		, total_physical_reads_io = SUM(avg_num_physical_io_reads * count_executions)
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
	, min_physical_reads_io
	, avg_physical_reads_io
	, max_physical_reads_io
	, total_physical_reads_io
	, physical_reads_pct =
		CASE total_physical_reads_io WHEN 0 THEN 0 ELSE
			CAST(CAST(total_physical_reads_io AS DECIMAL(10,2)) * 100 / (
					SELECT SUM(avg_num_physical_io_reads * count_executions)
					FROM ##QueryStorePerf
					WHERE start_time >= @start_time
					AND end_time <= @end_time
				) AS DECIMAL(5,2)
			)
		END
FROM physical_cte
ORDER BY
	total_physical_reads_io DESC
	, database_name ASC
	, query_hash ASC;
