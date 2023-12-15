-- Comment out after first execution
drop table if exists ##QueryStorePerf

declare
	@start_time datetimeoffset = '2023-12-14 12:00:00.000 + 00:00'
	, @end_time datetimeoffset = '2023-12-14 16:00:00.000 + 00:00';

if not exists (select 1 from tempdb.sys.tables where name = N'##QueryStorePerf')
begin
	select
		database_id = db_id()
		, database_name = db_name()
		, q.query_id
		, q.query_text_id
		, q.context_settings_id
		, q.object_id
		, schema_name = object_schema_name(o.object_id)
		, object_name = object_name(o.object_id)
		, q.query_parameterization_type_desc
		, initial_compile_start_time_query_level = q.initial_compile_start_time
		, last_compile_start_time_query_level = q.last_compile_start_time
		, last_execution_time_query_level = q.last_execution_time
		, q.last_compile_batch_sql_handle
		, q.last_compile_batch_offset_start
		, q.last_compile_batch_offset_end
		, count_compiles_query_level = q.count_compiles
		, avg_compile_duration_query_level = q.avg_compile_duration
		, last_compile_duration_query_level = q.last_compile_duration
		, q.avg_bind_duration
		, q.last_bind_duration
		, q.avg_bind_cpu_time
		, q.last_bind_cpu_time
		, q.avg_optimize_duration
		, q.last_optimize_duration
		, q.avg_optimize_cpu_time
		, q.last_optimize_cpu_time
		, q.avg_compile_memory_kb
		, q.last_compile_memory_kb
		, q.max_compile_memory_kb
		, c.set_options
		, c.language_id
		, c.date_format
		, c.date_first
		, c.status
		, c.required_cursor_options
		, c.acceptable_cursor_options
		, c.merge_action_type
		, c.default_schema_id
		, c.is_replication_specific
		, c.is_contained
		, p.plan_id
		, p.engine_version
		, p.compatibility_level
		, p.is_online_index_plan
		, p.is_trivial_plan
		, p.is_parallel_plan
		, p.is_forced_plan
		, p.is_natively_compiled
		, p.force_failure_count
		, p.last_force_failure_reason_desc
		, p.count_compiles
		, p.initial_compile_start_time
		, p.last_compile_start_time
		, last_execution_time_plan_level = p.last_execution_time
		, p.avg_compile_duration
		, p.last_compile_duration
		, p.has_compile_replay_script
		, p.is_optimized_plan_forcing_disabled
		, p.plan_forcing_type_desc
		, i.runtime_stats_interval_id
		, i.start_time
		, i.end_time
		, r.runtime_stats_id
		, r.execution_type_desc
		, r.first_execution_time
		, r.last_execution_time
		, r.count_executions
		, r.avg_duration
		, r.last_duration
		, r.min_duration
		, r.max_duration
		, r.stdev_duration
		, r.avg_cpu_time
		, r.last_cpu_time
		, r.min_cpu_time
		, r.max_cpu_time
		, r.stdev_cpu_time
		, r.avg_logical_io_reads
		, r.last_logical_io_reads
		, r.min_logical_io_reads
		, r.max_logical_io_reads
		, r.stdev_logical_io_reads
		, r.avg_logical_io_writes
		, r.last_logical_io_writes
		, r.min_logical_io_writes
		, r.max_logical_io_writes
		, r.stdev_logical_io_writes
		, r.avg_physical_io_reads
		, r.last_physical_io_reads
		, r.min_physical_io_reads
		, r.max_physical_io_reads
		, r.stdev_physical_io_reads
		, r.avg_clr_time
		, r.last_clr_time
		, r.min_clr_time
		, r.max_clr_time
		, r.stdev_clr_time
		, r.avg_dop
		, r.last_dop
		, r.min_dop
		, r.max_dop
		, r.stdev_dop
		, r.avg_query_max_used_memory
		, r.last_query_max_used_memory
		, r.min_query_max_used_memory
		, r.max_query_max_used_memory
		, r.stdev_query_max_used_memory
		, r.avg_rowcount
		, r.last_rowcount
		, r.min_rowcount
		, r.max_rowcount
		, r.stdev_rowcount
		, r.avg_num_physical_io_reads
		, r.last_num_physical_io_reads
		, r.min_num_physical_io_reads
		, r.max_num_physical_io_reads
		, r.stdev_num_physical_io_reads
		, r.avg_log_bytes_used
		, r.last_log_bytes_used
		, r.min_log_bytes_used
		, r.max_log_bytes_used
		, r.stdev_log_bytes_used
		, r.avg_tempdb_space_used
		, r.last_tempdb_space_used
		, r.min_tempdb_space_used
		, r.max_tempdb_space_used
		, r.stdev_tempdb_space_used
		, r.avg_page_server_io_reads
		, r.last_page_server_io_reads
		, r.min_page_server_io_reads
		, r.max_page_server_io_reads
		, r.stdev_page_server_io_reads
	into ##QueryStorePerf
	from sys.query_store_query q
	join sys.query_context_settings c on c.context_settings_id = q.context_settings_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_runtime_stats r on r.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval i on i.runtime_stats_interval_id = r.runtime_stats_interval_id
	left join sys.objects o on o.object_id = q.object_id
	where i.start_time >= @start_time
	and i.end_time <= @end_time
	order by runtime_stats_id;
end
else
begin
	insert into ##QueryStorePerf
	select
		database_id = db_id()
		, database_name = db_name()
		, q.query_id
		, q.query_text_id
		, q.context_settings_id
		, q.object_id
		, schema_name = object_schema_name(o.object_id)
		, object_name = object_name(o.object_id)
		, q.query_parameterization_type_desc
		, initial_compile_start_time_query_level = q.initial_compile_start_time
		, last_compile_start_time_query_level = q.last_compile_start_time
		, last_execution_time_query_level = q.last_execution_time
		, q.last_compile_batch_sql_handle
		, q.last_compile_batch_offset_start
		, q.last_compile_batch_offset_end
		, count_compiles_query_level = q.count_compiles
		, avg_compile_duration_query_level = q.avg_compile_duration
		, last_compile_duration_query_level = q.last_compile_duration
		, q.avg_bind_duration
		, q.last_bind_duration
		, q.avg_bind_cpu_time
		, q.last_bind_cpu_time
		, q.avg_optimize_duration
		, q.last_optimize_duration
		, q.avg_optimize_cpu_time
		, q.last_optimize_cpu_time
		, q.avg_compile_memory_kb
		, q.last_compile_memory_kb
		, q.max_compile_memory_kb
		, c.set_options
		, c.language_id
		, c.date_format
		, c.date_first
		, c.status
		, c.required_cursor_options
		, c.acceptable_cursor_options
		, c.merge_action_type
		, c.default_schema_id
		, c.is_replication_specific
		, c.is_contained
		, p.plan_id
		, p.engine_version
		, p.compatibility_level
		, p.is_online_index_plan
		, p.is_trivial_plan
		, p.is_parallel_plan
		, p.is_forced_plan
		, p.is_natively_compiled
		, p.force_failure_count
		, p.last_force_failure_reason_desc
		, p.count_compiles
		, p.initial_compile_start_time
		, p.last_compile_start_time
		, last_execution_time_plan_level = p.last_execution_time
		, p.avg_compile_duration
		, p.last_compile_duration
		, p.has_compile_replay_script
		, p.is_optimized_plan_forcing_disabled
		, p.plan_forcing_type_desc
		, i.runtime_stats_interval_id
		, i.start_time
		, i.end_time
		, r.runtime_stats_id
		, r.execution_type_desc
		, r.first_execution_time
		, r.last_execution_time
		, r.count_executions
		, r.avg_duration
		, r.last_duration
		, r.min_duration
		, r.max_duration
		, r.stdev_duration
		, r.avg_cpu_time
		, r.last_cpu_time
		, r.min_cpu_time
		, r.max_cpu_time
		, r.stdev_cpu_time
		, r.avg_logical_io_reads
		, r.last_logical_io_reads
		, r.min_logical_io_reads
		, r.max_logical_io_reads
		, r.stdev_logical_io_reads
		, r.avg_logical_io_writes
		, r.last_logical_io_writes
		, r.min_logical_io_writes
		, r.max_logical_io_writes
		, r.stdev_logical_io_writes
		, r.avg_physical_io_reads
		, r.last_physical_io_reads
		, r.min_physical_io_reads
		, r.max_physical_io_reads
		, r.stdev_physical_io_reads
		, r.avg_clr_time
		, r.last_clr_time
		, r.min_clr_time
		, r.max_clr_time
		, r.stdev_clr_time
		, r.avg_dop
		, r.last_dop
		, r.min_dop
		, r.max_dop
		, r.stdev_dop
		, r.avg_query_max_used_memory
		, r.last_query_max_used_memory
		, r.min_query_max_used_memory
		, r.max_query_max_used_memory
		, r.stdev_query_max_used_memory
		, r.avg_rowcount
		, r.last_rowcount
		, r.min_rowcount
		, r.max_rowcount
		, r.stdev_rowcount
		, r.avg_num_physical_io_reads
		, r.last_num_physical_io_reads
		, r.min_num_physical_io_reads
		, r.max_num_physical_io_reads
		, r.stdev_num_physical_io_reads
		, r.avg_log_bytes_used
		, r.last_log_bytes_used
		, r.min_log_bytes_used
		, r.max_log_bytes_used
		, r.stdev_log_bytes_used
		, r.avg_tempdb_space_used
		, r.last_tempdb_space_used
		, r.min_tempdb_space_used
		, r.max_tempdb_space_used
		, r.stdev_tempdb_space_used
		, r.avg_page_server_io_reads
		, r.last_page_server_io_reads
		, r.min_page_server_io_reads
		, r.max_page_server_io_reads
		, r.stdev_page_server_io_reads
	from sys.query_store_query q
	join sys.query_context_settings c on c.context_settings_id = q.context_settings_id
	join sys.query_store_plan p on p.query_id = q.query_id
	join sys.query_store_runtime_stats r on r.plan_id = p.plan_id
	join sys.query_store_runtime_stats_interval i on i.runtime_stats_interval_id = r.runtime_stats_interval_id
	left join sys.objects o on o.object_id = q.object_id
	where i.start_time >= @start_time
	and i.end_time <= @end_time
	order by runtime_stats_id;
end

