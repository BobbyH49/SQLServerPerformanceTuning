declare
	@schema_name nvarchar(200) = N'HumanResources'
	, @table_name nvarchar(200) = N'Employee';

declare @object_id bigint
select
	@object_id = object_id
from sys.schemas s
join sys.tables t on t.schema_id = s.schema_id
where s.name = @schema_name
and t.name = @table_name;

select
	database_id = db_id()
	, t.schema_id
	, schema_name = schema_name(t.schema_id)
	, t.object_id
	, object_name = t.name
	, s.stats_id
	, stats_name = s.name
	, s.auto_created
	, s.user_created
	, s.no_recompute
	, s.has_filter
	, s.filter_definition
	, s.is_temporary
	, s.is_incremental
	, stats_columns = substring(convert(nvarchar(4000), ((
		select N', ' + quotename(c.name)
		from sys.stats_columns sc
		join sys.columns c on c.object_id = sc.object_id and c.column_id = sc.column_id
		where sc.object_id = s.object_id and sc.stats_id = s.stats_id
		order by sc.stats_column_id
		for xml path(N'')
	  ))), 2, 4000)
	, sp.last_updated
	, sp.rows
	, sp.rows_sampled
	, sp.steps
	, sp.unfiltered_rows
	, sp.modification_counter
	, sp.persisted_sample_percent
from sys.tables t with (nolock)
join sys.stats s with (nolock) on s.object_id = t.object_id
cross apply sys.dm_db_stats_properties(s.object_id, s.stats_id) sp
where t.object_id = @object_id;

select
	database_id = db_id()
	, t.schema_id
	, schema_name = schema_name(t.schema_id)
	, t.object_id
	, object_name = t.name
	, s.stats_id
	, stats_name = s.name
	, sh.step_number
	, sh.range_high_key
	, sh.range_rows
	, sh.equal_rows
	, sh.distinct_range_rows
	, sh.average_range_rows
from sys.tables t with (nolock)
join sys.stats s with (nolock) on s.object_id = t.object_id
cross apply sys.dm_db_stats_histogram(s.object_id, s.stats_id) sh
where t.object_id = @object_id;
