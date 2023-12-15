declare
	@database_name nvarchar(200) = N'Contoso_Main_1'
	, @start_time_id int = 634
	, @end_time_id int = 637

select
	total_duration_minutes = sum(avg_duration * count_executions) / 1000000 / 60
	, total_duration_hours = sum(avg_duration * count_executions) / 1000000 / 3600
	, total_query_count = count(distinct query_id)
from ##QueryStorePerf
where database_name = @database_name
and runtime_stats_interval_id between @start_time_id and @end_time_id;

with duration_cte
as (
	select
		database_name
		, query_id
		, object_name = case when object_name = N'NULL' or object_name is null then N'' else schema_name + N'.' + object_name end
		, execution_count = sum(count_executions)
		, average_rowcount = avg(avg_rowcount)
		, total_duration_minutes = sum(avg_duration * count_executions) / 1000000 / 60
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
	, total_duration_minutes
	, duration_pct = 
		cast(cast(total_duration_minutes as decimal(10,2)) * 100 / (
				select sum(avg_duration * count_executions) / 1000000 / 60 
				from ##QueryStorePerf
				where database_name = @database_name
				and runtime_stats_interval_id between @start_time_id and @end_time_id
			) as decimal(5,2)
		)
from duration_cte
order by
	total_duration_minutes desc
	, query_id;
