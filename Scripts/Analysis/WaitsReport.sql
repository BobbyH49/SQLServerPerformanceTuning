declare
	@database_name nvarchar(200) = N'Contoso_Main_1'
	, @start_time_id int = 634
	, @end_time_id int = 637

select
	total_query_wait_time_minutes = sum(total_query_wait_time_ms) / 1000 / 60
	, total_query_wait_time_hours = sum(total_query_wait_time_ms) / 1000 / 60 / 60
from ##QueryStoreWaits
where database_name = @database_name
and runtime_stats_interval_id between @start_time_id and @end_time_id;

with logical_reads_cte
as (
	select
		database_name
		, query_id
		, total_query_wait_time_minutes = sum(total_query_wait_time_ms) / 1000 / 60
		, total_parallelism_waits_minutes = sum(case wait_category_desc when N'Parallelism' then total_query_wait_time_ms else 0 end) / 1000 / 60
		, total_network_waits_minutes = sum(case wait_category_desc when N'Network IO' then total_query_wait_time_ms else 0 end) / 1000 / 60
		, total_cpu_waits_minutes = sum(case wait_category_desc when N'CPU' then total_query_wait_time_ms else 0 end) / 1000 / 60
		, total_unknown_waits_minutes = sum(case wait_category_desc when N'Unknown' then total_query_wait_time_ms else 0 end) / 1000 / 60
		, total_disk_waits_minutes = sum(case wait_category_desc when N'Buffer IO' then total_query_wait_time_ms else 0 end) / 1000 / 60
		, total_lock_waits_minutes = sum(case wait_category_desc when N'Lock' then total_query_wait_time_ms else 0 end) / 1000 / 60
		, total_buffer_latch_waits_minutes = sum(case wait_category_desc when N'Buffer Latch' then total_query_wait_time_ms else 0 end) / 1000 / 60
	from ##QueryStoreWaits
	where database_name = @database_name
	and runtime_stats_interval_id between @start_time_id and @end_time_id
	group by
		database_name
		, query_id
)
select top 50
	database_name
	, query_id
	, total_query_wait_time_minutes
	, wait_time_pct = cast(cast(total_query_wait_time_minutes as decimal(10,2)) * 100 / (select sum(total_query_wait_time_ms) / 1000 / 60 from ##QueryStoreWaits) as decimal(5,2))
	, total_parallelism_waits_minutes
	, total_network_waits_minutes
	, total_cpu_waits_minutes
	, total_unknown_waits_minutes
	, total_disk_waits_minutes
	, total_lock_waits_minutes
	, total_buffer_latch_waits_minutes
from logical_reads_cte
order by
	total_query_wait_time_minutes desc
	, database_name
	, query_id;