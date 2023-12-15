declare
	@database_name nvarchar(200) = N'Contoso_Main_1'
	, @start_time_id int = 634
	, @end_time_id int = 637

select
	total_logical_reads_gb = sum(avg_logical_io_reads * count_executions) / 128 / 1024
	, total_logical_reads_tb = sum(avg_logical_io_reads * count_executions) / 128 / 1024 / 1024
	, total_query_count = count(distinct query_id)
from ##QueryStorePerf
where database_name = @database_name
and runtime_stats_interval_id between @start_time_id and @end_time_id;

with logical_reads_cte
as (
	select
		database_name
		, query_id
		, object_name = case when object_name = N'NULL' or object_name is null then N'' else schema_name + N'.' + object_name end
		, execution_count = sum(count_executions)
		, average_rowcount = avg(avg_rowcount)
		, total_logical_reads_gb = sum(avg_logical_io_reads * count_executions) / 128 / 1024
		, average_memory_grant_mb = avg(avg_query_max_used_memory) / 128
	from ##QueryStorePerf
	where database_name = @database_name
	and runtime_stats_interval_id between @start_time_id and @end_time_id
	group by
		database_name
		, query_id
		, schema_name
		, object_name
)
select top 50
	database_name
	, query_id
	, object_name
	, execution_count
	, average_rowcount
	, total_logical_reads_gb
	, logical_reads_pct =
		cast(cast(total_logical_reads_gb as decimal(10,2)) * 100 / (
				select sum(avg_logical_io_reads * count_executions) / 128 / 1024 
				from ##QueryStorePerf
				where database_name = @database_name
				and runtime_stats_interval_id between @start_time_id and @end_time_id
			) as decimal(5,2)
		)
	, average_memory_grant_mb
from logical_reads_cte
order by
	total_logical_reads_gb desc
	, query_id;
