SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'

SELECT
	total_physical_reads_gb = SUM(avg_physical_io_reads * count_executions) / 128 / 1024
	, total_physical_reads_tb = SUM(avg_physical_io_reads * count_executions) / 128 / 1024 / 1024
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
		, min_physical_reads_mb = MIN(min_physical_io_reads) / 128
		, avg_physical_reads_mb = SUM(avg_physical_io_reads * count_executions) / SUM(count_executions) / 128
		, max_physical_reads_mb = MAX(max_physical_io_reads) / 128
		, total_physical_reads_gb = SUM(avg_physical_io_reads * count_executions) / 128 / 1024
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
	, min_physical_reads_mb
	, avg_physical_reads_mb
	, max_physical_reads_mb
	, total_physical_reads_gb
	, physical_reads_pct =
		CASE total_physical_reads_gb WHEN 0 THEN 0 ELSE
			CAST(CAST(total_physical_reads_gb AS DECIMAL(10,2)) * 100 / (
					SELECT SUM(avg_physical_io_reads * count_executions) / 128 / 1024 
					FROM ##QueryStorePerf
					WHERE start_time >= @start_time
					AND end_time <= @end_time
				) AS DECIMAL(5,2)
			)
		END
FROM physical_cte
ORDER BY
	total_physical_reads_gb DESC
	, database_name ASC
	, query_hash ASC;
