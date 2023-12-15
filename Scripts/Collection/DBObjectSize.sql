select
	server_name = @@servername
	, database_id = db_id()
	, database_name = db_name()
	, o.schema_id
	, schema_name(o.schema_id) as schema_name
	, o.object_id
	, o.name as object_name
	, o.type_desc as object_type
	, schema_name(o.schema_id) + N'.' + o.name as schema_object_name
	, i.index_id
	, index_name = i.name
	, index_type = i.type_desc
	, index_data_space_id = i.data_space_id
	, p.partition_number
	, p.rows
	, data_rows = case when i.type in (0, 1) then case when au.type = 1 then p.rows else 0 end else 0 end
	, compression_type = p.data_compression_desc
	, allocation_unit_type = au.type_desc
	, allocation_data_space_id = au.data_space_id
	, au.total_pages
	, size_mb = au.total_pages / 128
from sys.all_objects o with (nolock)
join sys.indexes i with (nolock) on i.object_id = o.object_id
join sys.partitions p with (nolock) on p.object_id = i.object_id and p.index_id = i.index_id
join sys.allocation_units au with (nolock) on au.type in (1, 3) and au.container_id = p.hobt_id

union all

select
	server_name = @@servername
	, database_id = db_id()
	, database_name = db_name()
	, o.schema_id
	, schema_name(o.schema_id) as schema_name
	, o.object_id
	, o.name as object_name
	, o.type_desc as object_type
	, schema_name(o.schema_id) + N'.' + o.name as schema_object_name
	, i.index_id
	, index_name = i.name
	, index_type = i.type_desc
	, index_data_space_id = i.data_space_id
	, p.partition_number
	, p.rows
	, data_rows = case when i.type = 5 then case when au.type = 2 then p.rows else 0 end else 0 end
	, compression_type = p.data_compression_desc
	, allocation_unit_type = au.type_desc
	, allocation_data_space_id = au.data_space_id
	, au.total_pages
	, size_mb = au.total_pages / 128
from sys.all_objects o with (nolock)
join sys.indexes i with (nolock) on i.object_id = o.object_id
join sys.partitions p with (nolock) on p.object_id = i.object_id and p.index_id = i.index_id
join sys.allocation_units au with (nolock) on au.type = 2 and au.container_id = p.partition_id
