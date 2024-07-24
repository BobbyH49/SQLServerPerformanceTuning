SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'
	-- Add 5 to the score if the Priority Threshold is breached
	-- Set your own priority thresholds here based on results in each tab of your spreadsheet
	, @total_duration_priority_threshold BIGINT = 10 -- Minutes
	, @total_cpu_time_priority_threshold BIGINT = 10 -- Minutes
	, @total_clr_time_priority_threshold BIGINT = 10 -- Minutes
	, @total_logical_reads_priority_threshold BIGINT = 1000 -- GB
	, @total_memory_grants_priority_threshold BIGINT = 500 -- GB
	, @total_physical_reads_priority_threshold BIGINT = 20 -- GB
	, @total_physical_reads_io_priority_threshold BIGINT = 20 -- Operations
	, @total_logical_writes_priority_threshold BIGINT = 20 -- GB
	, @total_log_used_priority_threshold BIGINT = 1024 -- MB
	, @total_tempdb_space_used_priority_threshold BIGINT = 1024 -- MB
	, @total_wait_time_priority_threshold BIGINT = 10 -- Minutes

DECLARE @WorstPerformingQueries TABLE (
	query_rank TINYINT IDENTITY(1, 1)
	, database_id SMALLINT
	, database_name NVARCHAR(128)
	, query_hash BINARY(8)
);

DECLARE @WorstPerformingQueryPlans TABLE (
	query_plan_rank TINYINT IDENTITY(1, 1)
	, database_id SMALLINT
	, database_name NVARCHAR(128)
	, query_hash BINARY(8)
	, query_plan_hash BINARY(8)
);

INSERT INTO @WorstPerformingQueries (database_id, database_name, query_hash)
SELECT TOP 10 database_id, database_name, query_hash
FROM (
	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_duration * count_executions) DESC))
				* CASE WHEN SUM(avg_duration * count_executions) / 60000000 >= @total_duration_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_duration * count_executions) DESC
			, query_hash
	) Duration

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_cpu_time * count_executions) DESC))
				* CASE WHEN SUM(avg_cpu_time * count_executions) / 60000000 >= @total_cpu_time_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_cpu_time * count_executions) DESC
			, query_hash
	) CpuTime

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_clr_time * count_executions) DESC))
				* CASE WHEN SUM(avg_clr_time * count_executions) / 60000000 >= @total_clr_time_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_clr_time * count_executions) DESC
			, query_hash
	) ClrTime

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_logical_io_reads * count_executions) DESC))
				* CASE WHEN SUM(avg_logical_io_reads * count_executions) / 128 / 1024 >= @total_logical_reads_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_logical_io_reads * count_executions) DESC
			, query_hash
	) LogicalReads

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_query_max_used_memory * count_executions) DESC))
				* CASE WHEN SUM(avg_query_max_used_memory * count_executions) / 128 / 1024 >= @total_memory_grants_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_query_max_used_memory * count_executions) DESC
			, query_hash
	) MemoryGrants

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_physical_io_reads * count_executions) DESC))
				* CASE WHEN SUM(avg_physical_io_reads * count_executions) / 128 / 1024 >= @total_physical_reads_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_physical_io_reads * count_executions) DESC
			, query_hash
	) PhysicalReads

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_num_physical_io_reads * count_executions) DESC))
				* CASE WHEN SUM(avg_num_physical_io_reads * count_executions)4 >= @total_physical_reads_io_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_num_physical_io_reads * count_executions) DESC
			, query_hash
	) PhysicalReadsIO

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_logical_io_writes * count_executions) DESC))
				* CASE WHEN SUM(avg_logical_io_writes * count_executions) / 128 / 1024 >= @total_logical_writes_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_logical_io_writes * count_executions) DESC
			, query_hash
	) LogicalWrites

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_log_bytes_used * count_executions) DESC))
				* CASE WHEN SUM(avg_log_bytes_used * count_executions) / 128 >= @total_log_used_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_log_bytes_used * count_executions) DESC
			, query_hash
	) LogUsed

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = (11 - RANK() OVER(ORDER BY SUM(avg_tempdb_space_used * count_executions) DESC))
				* CASE WHEN SUM(avg_tempdb_space_used * count_executions) / 128 >= @total_tempdb_space_used_priority_threshold THEN 2 ELSE 0 END
		FROM ##QueryStorePerf
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(avg_tempdb_space_used * count_executions) DESC
			, query_hash
	) TempdbSpaceUsed

	UNION ALL

	SELECT database_id, database_name, query_hash, score
	FROM (
		SELECT TOP 10
			database_id
			, database_name
			, query_hash
			, score = 11 - RANK() OVER(ORDER BY SUM(total_query_wait_time_ms) DESC)
		FROM ##QueryStoreWaits
		WHERE start_time >= @start_time
		AND end_time <= @end_time
		GROUP BY
			database_id
			, database_name
			, query_hash
		ORDER BY
			SUM(total_query_wait_time_ms) DESC
			, query_hash
	) Waits
) Scores
GROUP BY database_id, database_name, query_hash
ORDER BY SUM(score) DESC;

INSERT INTO @WorstPerformingQueryPlans (database_id, database_name, query_hash, query_plan_hash)
SELECT wpq.database_id, wpq.database_name, wpq.query_hash, qsf.query_plan_hash
FROM @WorstPerformingQueries wpq
JOIN ##QueryStorePerf qsf ON qsf.database_id = wpq.database_id AND qsf.query_hash = wpq.query_hash
WHERE qsf.start_time >= @start_time
AND qsf.end_time <= @end_time
GROUP BY wpq.query_rank, wpq.database_id, wpq.database_name, wpq.query_hash, qsf.query_plan_hash
ORDER BY query_rank ASC;

SELECT database_name, query_hash, capture_sql_statement = N'SELECT database_name = N''' + QUOTENAME(database_name) + N''', q.query_hash, t.query_sql_text FROM ' + QUOTENAME(database_name) + N'.sys.query_store_query q JOIN ' + QUOTENAME(database_name) + N'.sys.query_store_query_text t ON t.query_text_id = q.query_text_id WHERE q.query_hash = ' + CONVERT(NVARCHAR(20), query_hash, 1) + N';'
FROM @WorstPerformingQueries
ORDER BY query_rank ASC;

SELECT database_name, query_hash, query_plan_hash, capture_plan_statement = N'SELECT database_name = N''' + QUOTENAME(database_name) + N''', query_plan_hash, query_plan = TRY_CAST(query_plan AS XML) FROM ' + QUOTENAME(database_name) + N'.sys.query_store_plan WHERE query_plan_hash = ' + CONVERT(NVARCHAR(20), query_plan_hash, 1) + N';'
FROM @WorstPerformingQueryPlans
ORDER BY query_plan_rank ASC, query_plan_hash;

SELECT
	qsp.database_name
	, qsp.query_hash
	, object_name = CASE WHEN qsp.object_name = N'NULL' OR qsp.object_name IS NULL THEN N'' ELSE qsp.schema_name + N'.' + qsp.object_name END
	, qsp.query_plan_hash
	, execution_count = SUM(qsp.count_executions)
	, average_rowcount = AVG(qsp.avg_rowcount)
	, average_duration_microseconds = AVG(qsp.avg_duration)
	, total_duration_minutes = SUM(qsp.avg_duration * qsp.count_executions) / 1000000 / 60
	, total_cpu_minutes = SUM(qsp.avg_cpu_time * qsp.count_executions) / 1000000 / 60
	, total_clr_minutes = SUM(qsp.avg_clr_time * qsp.count_executions) / 1000000 / 60
	, average_dop = AVG(qsp.avg_dop)
	, total_logical_reads_gb = SUM(qsp.avg_logical_io_reads * qsp.count_executions) / 128 / 1024
	, total_memory_grants_reads_gb = SUM(qsp.avg_query_max_used_memory * qsp.count_executions) / 128 / 1024
	, total_physical_reads_gb = SUM(qsp.avg_physical_io_reads * qsp.count_executions) / 128 / 1024
	, total_physical_reads_io = SUM(qsp.avg_num_physical_io_reads * qsp.count_executions)
	, total_logical_writes_gb = SUM(qsp.avg_logical_io_writes * qsp.count_executions) / 128 / 1024
	, total_log_used_mb = SUM(qsp.avg_log_bytes_used * qsp.count_executions) / 1024 / 1024
	, total_tempdb_space_used_mb = SUM(qsp.avg_tempdb_space_used * qsp.count_executions) / 128
	, qsw.total_wait_minutes
FROM ##QueryStorePerf qsp
JOIN @WorstPerformingQueryPlans wpp ON wpp.database_id = qsp.database_id AND wpp.query_hash = qsp.query_hash AND wpp.query_plan_hash = qsp.query_plan_hash
LEFT JOIN (
	SELECT
		w.database_id
		, w.database_name
		, w.query_hash
		, w.query_plan_hash
		, total_wait_minutes = SUM(w.total_query_wait_time_ms) / 1000 / 60
	FROM ##QueryStoreWaits w
	JOIN @WorstPerformingQueryPlans wpp ON wpp.database_id = w.database_id AND wpp.query_plan_hash = w.query_plan_hash
	WHERE start_time >= @start_time
	AND end_time <= @end_time
	GROUP BY
		w.database_id
		, w.database_name
		, w.query_hash
		, w.query_plan_hash
) qsw ON qsw.database_id = qsp.database_id AND qsw.query_hash = qsp.query_hash AND qsw.query_plan_hash = qsp.query_plan_hash
WHERE qsp.start_time >= @start_time
AND qsp.end_time <= @end_time
GROUP BY
	qsp.database_name
	, qsp.query_hash
	, qsp.schema_name
	, qsp.object_name
	, qsp.query_plan_hash
	, wpp.query_plan_rank
	, qsw.total_wait_minutes
ORDER BY query_plan_rank ASC;
