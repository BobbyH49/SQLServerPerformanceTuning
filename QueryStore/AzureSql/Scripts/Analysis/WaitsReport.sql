SET NOCOUNT ON;

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 10:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-10 11:00:00.000 + 00:00'

SELECT
	total_query_wait_time_minutes = SUM(total_query_wait_time_ms) / 1000 / 60
	, total_query_wait_time_hours = SUM(total_query_wait_time_ms) / 1000 / 60 / 60
FROM ##QueryStoreWaits
WHERE start_time >= @start_time
AND end_time <= @end_time;

WITH logical_reads_cte
AS (
	SELECT
		database_name
		, query_hash
		, min_query_wait_time_ms = MIN(min_query_wait_time_ms)
		, avg_query_wait_time_ms = AVG(avg_query_wait_time_ms)
		, max_query_wait_time_ms = MAX(max_query_wait_time_ms)
		, total_query_wait_time_minutes = SUM(total_query_wait_time_ms) / 1000 / 60
		, total_parallelism_wait_minutes = SUM(CASE wait_category_desc WHEN N'Parallelism' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_network_io_wait_minutes = SUM(CASE wait_category_desc WHEN N'Network IO' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_cpu_wait_minutes = SUM(CASE wait_category_desc WHEN N'CPU' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_unknown_wait_minutes = SUM(CASE wait_category_desc WHEN N'Unknown' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_buffer_io_wait_minutes = SUM(CASE wait_category_desc WHEN N'Buffer IO' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_lock_wait_minutes = SUM(CASE wait_category_desc WHEN N'Lock' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_buffer_latch_wait_minutes = SUM(CASE wait_category_desc WHEN N'Buffer Latch' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_memory_wait_minutes = SUM(CASE wait_category_desc WHEN N'Memory' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_worker_thread_wait_minutes = SUM(CASE wait_category_desc WHEN N'Worker Thread' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_latch_wait_minutes = SUM(CASE wait_category_desc WHEN N'Latch' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_compilation_wait_minutes = SUM(CASE wait_category_desc WHEN N'Compilation' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_sql_clr_wait_minutes = SUM(CASE wait_category_desc WHEN N'SQL CLR' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_mirroring_wait_minutes = SUM(CASE wait_category_desc WHEN N'Mirroring' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_transaction_wait_minutes = SUM(CASE wait_category_desc WHEN N'Transaction' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_idle_wait_minutes = SUM(CASE wait_category_desc WHEN N'Idle' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_preemptive_wait_minutes = SUM(CASE wait_category_desc WHEN N'Preemptive' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_service_broker_wait_minutes = SUM(CASE wait_category_desc WHEN N'Service Broker' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_tran_log_io_wait_minutes = SUM(CASE wait_category_desc WHEN N'Tran Log IO' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_user_wait_wait_minutes = SUM(CASE wait_category_desc WHEN N'User Wait' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_tracing_wait_minutes = SUM(CASE wait_category_desc WHEN N'Tracing' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_full_text_search_wait_minutes = SUM(CASE wait_category_desc WHEN N'Full Text Search' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_other_disk_io_wait_minutes = SUM(CASE wait_category_desc WHEN N'Other Disk IO' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_replication_wait_minutes = SUM(CASE wait_category_desc WHEN N'Replication' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
		, total_log_rate_governor_wait_minutes = SUM(CASE wait_category_desc WHEN N'Log Rate Governor' THEN total_query_wait_time_ms ELSE 0 END) / 1000 / 60
	FROM ##QueryStoreWaits
	WHERE start_time >= @start_time
	AND end_time <= @end_time
	GROUP BY
		database_name
		, query_hash
)
SELECT TOP 50
	database_name
	, query_hash
	, min_query_wait_time_ms
	, avg_query_wait_time_ms
	, max_query_wait_time_ms
	, total_query_wait_time_minutes
	, wait_time_pct =
		CASE total_query_wait_time_minutes WHEN 0 THEN 0 ELSE
			CAST(CAST(total_query_wait_time_minutes AS DECIMAL(10,2)) * 100 / (
					SELECT SUM(total_query_wait_time_ms) / 1000 / 60
					FROM ##QueryStoreWaits
					WHERE start_time >= @start_time
					AND end_time <= @end_time
				) AS DECIMAL(5,2)
			)
		END
	, total_parallelism_wait_minutes
	, total_network_io_wait_minutes
	, total_cpu_wait_minutes
	, total_unknown_wait_minutes
	, total_buffer_io_wait_minutes
	, total_lock_wait_minutes
	, total_buffer_latch_wait_minutes
	, total_memory_wait_minutes
	, total_worker_thread_wait_minutes
	, total_latch_wait_minutes
	, total_compilation_wait_minutes
	, total_sql_clr_wait_minutes
	, total_mirroring_wait_minutes
	, total_transaction_wait_minutes
	, total_idle_wait_minutes
	, total_preemptive_wait_minutes
	, total_service_broker_wait_minutes
	, total_tran_log_io_wait_minutes
	, total_user_wait_wait_minutes
	, total_tracing_wait_minutes
	, total_full_text_search_wait_minutes
	, total_other_disk_io_wait_minutes
	, total_replication_wait_minutes
	, total_log_rate_governor_wait_minutes
FROM logical_reads_cte
ORDER BY
	total_query_wait_time_minutes DESC
	, database_name ASC
	, query_hash ASC;
