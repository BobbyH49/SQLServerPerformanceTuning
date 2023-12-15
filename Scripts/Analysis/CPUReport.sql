declare
	@database_name nvarchar(200) = N'Contoso_Main_1'
	, @start_time_id int = 634
	, @end_time_id int = 637
	, @vCores int = 4

select
	total_cpu_minutes = sum(avg_cpu_time * count_executions) / 1000000 / 60
	, average_cpu_pct = cast(cast(sum(avg_cpu_time * count_executions) as decimal(20, 2)) * 100 / (select cast(@end_time_id - @start_time_id + 1 as bigint) * 60 * 60 * 1000000 * @vCores) as decimal(5, 2))
	, total_query_count = count(distinct query_id)
from ##QueryStorePerf
where database_name = @database_name
and runtime_stats_interval_id between @start_time_id and @end_time_id;

with cpu_cte
as (
	select
		database_name
		, query_id
		, object_name = case when object_name = N'NULL' or object_name is null then N'' else schema_name + N'.' + object_name end
		, execution_count = sum(count_executions)
		, average_rowcount = avg(avg_rowcount)
		, total_cpu_minutes = sum(avg_cpu_time * count_executions) / 1000000 / 60
		, average_dop = avg(avg_dop)
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
	, total_cpu_minutes
	, cpu_pct = 
		cast(cast(total_cpu_minutes as decimal(10,2)) * 100 / (
				select sum(avg_cpu_time * count_executions) / 1000000 / 60 
				from ##QueryStorePerf
				where database_name = @database_name
				and runtime_stats_interval_id between @start_time_id and @end_time_id
			) as decimal(5,2)
		)
	, average_dop
from cpu_cte
order by
	total_cpu_minutes desc
	, query_id;
