declare
	@database_name nvarchar(200) = N'Contoso_Main_1'
	, @start_time_id int = 634
	, @end_time_id int = 637

declare @query_ids table (query_id bigint)
insert into @query_ids (query_id)
select distinct query_id
from ##QueryStorePerf
where database_name = @database_name
and query_id in (
	77
)

select distinct 
	database_name, query_id, plan_id
from ##QueryStorePerf
where database_name = @database_name
and runtime_stats_interval_id between @start_time_id and @end_time_id
and query_id in (select query_id from @query_ids)

select
	p.database_name
	, p.query_id
	, object_name = case when object_name = N'NULL' or object_name is null then N'' else p.schema_name + N'.' + p.object_name end
	, p.plan_id
	, execution_count = sum(p.count_executions)
	, average_rowcount = avg(p.avg_rowcount)
	, average_dop = avg(p.avg_dop)
	, average_memory_grant_mb = avg(p.avg_query_max_used_memory) / 128
	, total_duration_minutes = sum(p.avg_duration * p.count_executions) / 1000000 / 60
	, total_cpu_minutes = sum(p.avg_cpu_time * p.count_executions) / 1000000 / 60
	, total_logical_reads_gb = sum(p.avg_logical_io_reads * p.count_executions) / 128 / 1024
	, total_physical_reads_gb = sum(p.avg_physical_io_reads * p.count_executions) / 128 / 1024
	, total_logical_writes_gb = sum(p.avg_logical_io_writes * p.count_executions) / 128 / 1024
	, total_tempdb_space_used_gb = sum(p.avg_tempdb_space_used * p.count_executions) / 128 / 1024
	, w.total_wait_minutes
from ##QueryStorePerf p
join (
	select
		database_name
		, query_id
		, plan_id
		, total_wait_minutes = sum(total_query_wait_time_ms) / 1000 / 60
	from ##QueryStoreWaits
	where database_name = @database_name
	and runtime_stats_interval_id between @start_time_id and @end_time_id
	and query_id in (select query_id from @query_ids)
	group by
		database_name
		, query_id
		, plan_id
) w on w.database_name = p.database_name and w.query_id = p.query_id and w.plan_id = p.plan_id
where p.database_name = @database_name
and p.runtime_stats_interval_id between @start_time_id and @end_time_id
and p.query_id in (select query_id from @query_ids)
group by
	p.database_name
	, p.query_id
	, p.schema_name
	, p.object_name
	, p.plan_id
	, w.total_wait_minutes
order by
	query_id
	, plan_id;

