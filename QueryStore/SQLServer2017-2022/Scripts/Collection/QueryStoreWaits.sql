SET NOCOUNT ON;

DROP TABLE IF EXISTS ##QueryStoreWaits;

CREATE TABLE ##QueryStoreWaits (
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
	, wait_stats_id BIGINT
	, wait_category_desc NVARCHAR(60)
	, total_query_wait_time_ms FLOAT
	, avg_query_wait_time_ms FLOAT
	, min_query_wait_time_ms FLOAT
	, max_query_wait_time_ms FLOAT
);

DECLARE
	@start_time DATETIMEOFFSET = '2024-07-10 00:00:00.000 + 00:00'
	, @end_time DATETIMEOFFSET = '2024-07-11 00:00:00.000 + 00:00';

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

			INSERT INTO ##QueryStoreWaits
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
				, w.wait_stats_id
				, w.wait_category_desc
				, w.total_query_wait_time_ms
				, w.avg_query_wait_time_ms
				, w.min_query_wait_time_ms
				, w.max_query_wait_time_ms
			FROM sys.query_store_query q
			JOIN sys.query_store_plan p ON p.query_id = q.query_id
			JOIN sys.query_store_wait_stats w ON w.plan_id = p.plan_id
			JOIN sys.query_store_runtime_stats_interval i ON i.runtime_stats_interval_id = w.runtime_stats_interval_id
			LEFT JOIN sys.objects o ON o.object_id = q.object_id
			WHERE i.start_time >= @start_time
			AND i.end_time <= @end_time
			ORDER BY wait_stats_id;
		'
		EXEC sp_executesql @sqlcmd, N'@start_time DATETIMEOFFSET, @end_time DATETIMEOFFSET', @start_time, @end_time
	END
	SET @current_database_id += 1;
END
