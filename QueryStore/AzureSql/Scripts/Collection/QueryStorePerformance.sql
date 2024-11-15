SET NOCOUNT ON;

DROP TABLE IF EXISTS ##QueryStorePerf;

CREATE TABLE ##QueryStorePerf (
	database_id SMALLINT
	, database_name NVARCHAR(128)
	, query_id BIGINT
	, query_hash BINARY(8)
	, object_id BIGINT
	, schema_name NVARCHAR(128)
	, object_name NVARCHAR(128)
	, object_type CHAR(2)
	, plan_id BIGINT
	, query_plan_hash BINARY(8)
	, runtime_stats_interval_id BIGINT
	, start_time DATETIMEOFFSET
	, end_time DATETIMEOFFSET
	, runtime_stats_id BIGINT
	, execution_type_desc NVARCHAR(60)
	, count_executions BIGINT
	, avg_duration FLOAT
	, min_duration FLOAT
	, max_duration FLOAT
	, avg_cpu_time FLOAT
	, min_cpu_time FLOAT
	, max_cpu_time FLOAT
	, avg_logical_io_reads FLOAT
	, min_logical_io_reads FLOAT
	, max_logical_io_reads FLOAT
	, avg_logical_io_writes FLOAT
	, min_logical_io_writes FLOAT
	, max_logical_io_writes FLOAT
	, avg_physical_io_reads FLOAT
	, min_physical_io_reads FLOAT
	, max_physical_io_reads FLOAT
	, avg_clr_time FLOAT
	, min_clr_time FLOAT
	, max_clr_time FLOAT
	, avg_dop FLOAT
	, min_dop FLOAT
	, max_dop FLOAT
	, avg_query_max_used_memory FLOAT
	, min_query_max_used_memory FLOAT
	, max_query_max_used_memory FLOAT
	, avg_rowcount FLOAT
	, min_rowcount FLOAT
	, max_rowcount FLOAT
	, avg_num_physical_io_reads FLOAT
	, min_num_physical_io_reads FLOAT
	, max_num_physical_io_reads FLOAT
	, avg_log_bytes_used FLOAT
	, min_log_bytes_used FLOAT
	, max_log_bytes_used FLOAT
	, avg_tempdb_space_used FLOAT
	, min_tempdb_space_used FLOAT
	, max_tempdb_space_used FLOAT
	, avg_page_server_io_reads FLOAT
	, min_page_server_io_reads FLOAT
	, max_page_server_io_reads FLOAT
);

DECLARE
	@start_time DATETIMEOFFSET = '2024-10-28 00:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-10-29 00:00:00.000 + 00:00';

DECLARE @current_database_id SMALLINT, @max_database_id SMALLINT, @dbname NVARCHAR(128), @sqlcmd NVARCHAR(MAX);
SET @current_database_id = 5;
SELECT @max_database_id = MAX(database_id) from sys.databases;

WHILE @current_database_id <= @max_database_id
BEGIN
	SELECT @dbname = name FROM sys.databases WHERE database_id = @current_database_id AND state = 0;

	IF @dbname IS NOT NULL
	BEGIN
		SET @sqlcmd = N'
			USE ' + QUOTENAME(@dbname) + N';

			INSERT INTO ##QueryStorePerf
			SELECT
				database_id = db_id()
				, database_name = db_name()
				, q.query_id
				, q.query_hash
				, q.object_id
				, schema_name = object_schema_name(o.object_id)
				, object_name = object_name(o.object_id)
				, object_type = o.type
				, p.plan_id
				, p.query_plan_hash
				, i.runtime_stats_interval_id
				, i.start_time
				, i.end_time
				, r.runtime_stats_id
				, r.execution_type_desc
				, r.count_executions
				, r.avg_duration
				, r.min_duration
				, r.max_duration
				, r.avg_cpu_time
				, r.min_cpu_time
				, r.max_cpu_time
				, r.avg_logical_io_reads
				, r.min_logical_io_reads
				, r.max_logical_io_reads
				, r.avg_logical_io_writes
				, r.min_logical_io_writes
				, r.max_logical_io_writes
				, r.avg_physical_io_reads
				, r.min_physical_io_reads
				, r.max_physical_io_reads
				, r.avg_clr_time
				, r.min_clr_time
				, r.max_clr_time
				, r.avg_dop
				, r.min_dop
				, r.max_dop
				, r.avg_query_max_used_memory
				, r.min_query_max_used_memory
				, r.max_query_max_used_memory
				, r.avg_rowcount
				, r.min_rowcount
				, r.max_rowcount
				, r.avg_num_physical_io_reads
				, r.min_num_physical_io_reads
				, r.max_num_physical_io_reads
				, r.avg_log_bytes_used
				, r.min_log_bytes_used
				, r.max_log_bytes_used
				, r.avg_tempdb_space_used
				, r.min_tempdb_space_used
				, r.max_tempdb_space_used
				, r.avg_page_server_io_reads
				, r.min_page_server_io_reads
				, r.max_page_server_io_reads
			FROM sys.query_store_query q
			JOIN sys.query_store_plan p ON p.query_id = q.query_id
			JOIN sys.query_store_runtime_stats r ON r.plan_id = p.plan_id
			JOIN sys.query_store_runtime_stats_interval i ON i.runtime_stats_interval_id = r.runtime_stats_interval_id
			LEFT JOIN sys.objects o ON o.object_id = q.object_id
			WHERE i.start_time >= @start_time
			AND i.end_time <= @end_time
			ORDER BY runtime_stats_id;
		'
		EXEC sp_executesql @sqlcmd, N'@start_time DATETIMEOFFSET, @end_time DATETIMEOFFSET', @start_time, @end_time
	END
	SET @current_database_id += 1;
END
