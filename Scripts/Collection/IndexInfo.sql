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

select
	database_id
	, object_id
	, index_id
	, user_seeks
	, user_scans
	, user_lookups
	, user_updates
	, last_user_seek
	, last_user_scan
	, last_user_lookup
	, last_user_update
	, system_seeks
	, system_scans
	, system_lookups
	, system_updates
	, last_system_seek
	, last_system_scan
	, last_system_lookup
	, last_system_update
from sys.dm_db_index_usage_stats where database_id = db_id() and object_id = @object_id;

select
	database_id
	, object_id
	, index_id
	, partition_number
	, hobt_id
	, leaf_insert_count
	, leaf_delete_count
	, leaf_update_count
	, leaf_ghost_count
	, nonleaf_insert_count
	, nonleaf_delete_count
	, nonleaf_update_count
	, leaf_allocation_count
	, nonleaf_allocation_count
	, leaf_page_merge_count
	, nonleaf_page_merge_count
	, range_scan_count
	, singleton_lookup_count
	, forwarded_fetch_count
	, lob_fetch_in_pages
	, lob_fetch_in_bytes
	, lob_orphan_create_count
	, lob_orphan_insert_count
	, row_overflow_fetch_in_pages
	, row_overflow_fetch_in_bytes
	, column_value_push_off_row_count
	, column_value_pull_in_row_count
	, row_lock_count
	, row_lock_wait_count
	, row_lock_wait_in_ms
	, page_lock_count
	, page_lock_wait_count
	, page_lock_wait_in_ms
	, index_lock_promotion_attempt_count
	, index_lock_promotion_count
	, page_latch_wait_count
	, page_latch_wait_in_ms
	, page_io_latch_wait_count
	, page_io_latch_wait_in_ms
	, tree_page_latch_wait_count
	, tree_page_latch_wait_in_ms
	, tree_page_io_latch_wait_count
	, tree_page_io_latch_wait_in_ms
	, page_compression_attempt_count
	, page_compression_success_count
from sys.dm_db_index_operational_stats(db_id(), @object_id, null, null);
