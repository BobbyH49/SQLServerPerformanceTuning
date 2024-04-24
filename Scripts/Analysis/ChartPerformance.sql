SELECT * FROM sys.time_zone_info;

declare
	@vCores int = 4
	, @timezone nvarchar(200) = N'GMT Standard Time'

if (select SUBSTRING(CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR), 1, 2)) = N'13'
begin
	select
		p.database_name
		, p.runtime_stats_interval_id
		, start_time_local = p.start_time AT TIME ZONE @timezone
		, start_time_utc = p.start_time
		, end_time_utc = p.end_time
		, day_of_week = datepart(weekday, p.start_time)
		, hour = datepart(hh, p.start_time) + 1
		, day_of_week_hour = datepart(weekday, p.start_time) * 100 + datepart(hh, p.start_time) + 1
		, total_executions = sum(p.count_executions)
		, duration_minutes = sum(p.avg_duration * p.count_executions) / 1000000 / 60
		, cpu_minutes = sum(p.avg_cpu_time * p.count_executions) / 1000000 / 60
		, cpu_percent = (sum(p.avg_cpu_time * p.count_executions) * 100) / 1000000 / 3600 / @vCores
		, logical_reads_gb = sum(p.avg_logical_io_reads * p.count_executions) / 128 / 1024
		, physical_reads_gb = sum(p.avg_physical_io_reads * p.count_executions) / 128 / 1024
		, logical_writes_gb = sum(p.avg_logical_io_writes * p.count_executions) / 128 / 1024
	from ##QueryStorePerf p
	group by
		p.database_name
		, p.runtime_stats_interval_id
		, p.start_time
		, p.end_time
	order by
		database_name
		, start_time;
end
else
begin
	select
		p.database_name
		, p.runtime_stats_interval_id
		, start_time_local = p.start_time AT TIME ZONE @timezone
		, start_time_utc = p.start_time
		, end_time_utc = p.end_time
		, day_of_week = datepart(weekday, p.start_time)
		, hour = datepart(hh, p.start_time) + 1
		, day_of_week_hour = datepart(weekday, p.start_time) * 100 + datepart(hh, p.start_time) + 1
		, total_executions = sum(p.count_executions)
		, duration_minutes = sum(p.avg_duration * p.count_executions) / 1000000 / 60
		, cpu_minutes = sum(p.avg_cpu_time * p.count_executions) / 1000000 / 60
		, cpu_percent = (sum(p.avg_cpu_time * p.count_executions) * 100) / 1000000 / 3600 / @vCores
		, logical_reads_gb = sum(p.avg_logical_io_reads * p.count_executions) / 128 / 1024
		, physical_reads_gb = sum(p.avg_physical_io_reads * p.count_executions) / 128 / 1024
		, logical_writes_gb = sum(p.avg_logical_io_writes * p.count_executions) / 128 / 1024
		, w.total_wait_minutes = coalesce(w.total_wait_minutes, 0)
	from ##QueryStorePerf p
	left join (
		select
			database_id
			, runtime_stats_interval_id
			, total_wait_minutes = sum(total_query_wait_time_ms) / 1000 / 60
		from ##QueryStoreWaits
		group by
			database_id
			, runtime_stats_interval_id
	)w
		on w.database_id = p.database_id
		and w.runtime_stats_interval_id = p.runtime_stats_interval_id
	group by
		p.database_name
		, p.runtime_stats_interval_id
		, p.start_time
		, p.end_time
		, w.total_wait_minutes
	order by
		database_name
		, start_time;
end
