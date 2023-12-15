declare
	@database_name nvarchar(200) = N'Contoso_Main_1'
	, @start_time_id int = 634
	, @end_time_id int = 637

select
	total_physical_reads_gb = sum(avg_physical_io_reads * count_executions) / 128 / 1024
	, total_physical_reads_tb = sum(avg_physical_io_reads * count_executions) / 128 / 1024 / 1024
	, total_query_count = count(distinct query_id)
from ##QueryStorePerf
where database_name = @database_name
and runtime_stats_interval_id between @start_time_id and @end_time_id;

with physical_cte
as (
	select
		database_name
		, query_id
		, object_name = case when object_name = N'NULL' or object_name is null then N'' else schema_name + N'.' + object_name end
		, execution_count = sum(count_executions)
		, average_rowcount = avg(avg_rowcount)
		, total_physical_reads_gb = sum(avg_physical_io_reads * count_executions) / 128 / 1024
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
	, total_physical_reads_gb
	, physical_reads_pct =
		case total_physical_reads_gb when 0 then 0 else
			cast(cast(total_physical_reads_gb as decimal(10,2)) * 100 / (
					select sum(avg_physical_io_reads * count_executions) / 128 / 1024 
					from ##QueryStorePerf
					where database_name = @database_name
					and runtime_stats_interval_id between @start_time_id and @end_time_id
				) as decimal(5,2)
			)
		end
from physical_cte
order by
	total_physical_reads_gb desc
	, query_id;
