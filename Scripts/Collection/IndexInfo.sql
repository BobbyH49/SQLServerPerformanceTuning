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
	, i.index_id
	, index_name = i.name
	, index_type = i.type_desc
	, i.is_primary_key
	, i.is_unique
	, i.is_unique_constraint
	, i.has_filter
	, i.fill_factor
	, index_keys = substring(convert(nvarchar(4000), ((
		select N', ' + quotename(c.name)
		from sys.index_columns ic
		join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
		where ic.object_id = i.object_id and ic.index_id = i.index_id
		and ic.is_included_column = 0
		order by ic.index_column_id
		for xml path(N'')
	  ))), 2, 4000)
	, included_columns = substring(convert(nvarchar(4000), ((
		select N', ' + quotename(c.name)
		from sys.index_columns ic
		join sys.columns c on c.object_id = ic.object_id and c.column_id = ic.column_id
		where ic.object_id = i.object_id and ic.index_id = i.index_id
		and ic.is_included_column = 1
		order by ic.index_column_id
		for xml path(N'')
	  ))), 2, 4000)
from sys.tables t with (nolock)
join sys.indexes i with (nolock) on i.object_id = t.object_id
where t.object_id = @object_id;

select * from sys.dm_db_index_usage_stats where database_id = db_id() and object_id = @object_id;
select * from sys.dm_db_index_operational_stats(db_id(), @object_id, null, null);
