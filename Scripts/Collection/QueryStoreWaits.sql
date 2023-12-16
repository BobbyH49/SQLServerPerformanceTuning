-- Comment out after first execution
drop table if exists ##QueryStoreWaits

declare
	@start_time datetimeoffset = '2023-12-14 12:00:00.000 + 00:00'
	, @end_time datetimeoffset = '2023-12-14 16:00:00.000 + 00:00';

if (select SUBSTRING(CAST(SERVERPROPERTY(N'ProductVersion') AS NVARCHAR), 1, 2)) = N'13'
begin
	print N'SQL Server 2016 does not support Wait Statistics'
end
else
begin
	if not exists (select 1 from tempdb.sys.tables where name = N'##QueryStoreWaits')
	begin
		select
			database_id = db_id()
			, database_name = db_name()
			, q.query_id
			, q.query_text_id
			, q.context_settings_id
			, q.object_id
			, p.plan_id
			, i.runtime_stats_interval_id
			, i.start_time
			, i.end_time
			, w.wait_stats_id
			, w.wait_category_desc
			, w.execution_type_desc
			, w.total_query_wait_time_ms
			, w.avg_query_wait_time_ms
			, w.last_query_wait_time_ms
			, w.min_query_wait_time_ms
			, w.max_query_wait_time_ms
			, w.stdev_query_wait_time_ms
		into ##QueryStoreWaits
		from sys.query_store_query q
		join sys.query_store_plan p on p.query_id = q.query_id
		join sys.query_store_wait_stats w on w.plan_id = p.plan_id
		join sys.query_store_runtime_stats_interval i on i.runtime_stats_interval_id = w.runtime_stats_interval_id
		where i.start_time >= @start_time
		and i.end_time <= @end_time
		order by wait_stats_id;
	end
	else
	begin
		insert into ##QueryStoreWaits
		select
			database_id = db_id()
			, database_name = db_name()
			, q.query_id
			, q.query_text_id
			, q.context_settings_id
			, q.object_id
			, p.plan_id
			, i.runtime_stats_interval_id
			, i.start_time
			, i.end_time
			, w.wait_stats_id
			, w.wait_category_desc
			, w.execution_type_desc
			, w.total_query_wait_time_ms
			, w.avg_query_wait_time_ms
			, w.last_query_wait_time_ms
			, w.min_query_wait_time_ms
			, w.max_query_wait_time_ms
			, w.stdev_query_wait_time_ms
		from sys.query_store_query q
		join sys.query_store_plan p on p.query_id = q.query_id
		join sys.query_store_wait_stats w on w.plan_id = p.plan_id
		join sys.query_store_runtime_stats_interval i on i.runtime_stats_interval_id = w.runtime_stats_interval_id
		where i.start_time >= @start_time
		and i.end_time <= @end_time
		order by wait_stats_id;
	end
end
