SET NOCOUNT ON;

SELECT * FROM sys.time_zone_info;

DECLARE
	@vCores int = 24
	, @timezone nvarchar(200) = N'UTC';

SELECT
	p.database_name
	, p.runtime_stats_interval_id
	, start_time_local = p.start_time AT TIME ZONE @timezone
	, start_time_utc = p.start_time
	, end_time_utc = p.end_time
	, day_of_week = DATEPART(WEEKDAY, p.start_time)
	, hour = DATEPART(hh, p.start_time) + 1
	, day_of_week_hour = DATEPART(WEEKDAY, p.start_time) * 100 + DATEPART(hh, p.start_time) + 1
	, total_executions = SUM(p.count_executions)
	, average_duration_microseconds = AVG(p.avg_duration)
	, total_duration_minutes = SUM(p.avg_duration * p.count_executions) / 1000000 / 60
	, total_cpu_minutes_inc_func = SUM(p.avg_cpu_time * p.count_executions) / 1000000 / 60
	, cpu_percent_inc_func = (SUM(p.avg_cpu_time * p.count_executions) * 100) / 1000000 / 3600 / @vCores
	, total_cpu_minutes_exc_func = SUM(CASE WHEN p.object_type IN ('FN', 'TF') THEN 0 ELSE p.avg_cpu_time END * p.count_executions) / 1000000 / 60
	, cpu_percent_exc_func = (SUM(CASE WHEN p.object_type IN ('FN', 'TF') THEN 0 ELSE p.avg_cpu_time END * p.count_executions) * 100) / 1000000 / 3600 / @vCores
	, clr_minutes = SUM(p.avg_clr_time * p.count_executions) / 1000000 / 60
	, logical_reads_gb = SUM(p.avg_logical_io_reads * p.count_executions) / 128 / 1024
	, memory_grants_gb = SUM(p.avg_query_max_used_memory * p.count_executions) / 128 / 1024
	, physical_reads_gb = SUM(p.avg_physical_io_reads * p.count_executions) / 128 / 1024
	, logical_writes_gb = SUM(p.avg_logical_io_writes * p.count_executions) / 128 / 1024
FROM ##QueryStorePerf p
GROUP BY
	p.database_name
	, p.runtime_stats_interval_id
	, p.start_time
	, p.end_time
ORDER BY
	database_name
	, start_time;
