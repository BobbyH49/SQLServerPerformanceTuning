select
	cpu_ticks
	, ms_ticks
	, cpu_count
	, hyperthread_ratio
	, physical_memory_mb = physical_memory_kb / 1024
	, virtual_memory_mb = virtual_memory_kb / 1024
	, committed_mb = committed_kb / 1024
	, committed_target_mb = committed_target_kb / 1024
	, visible_target_mb = visible_target_kb / 1024
	, stack_size_in_bytes
	, os_quantum
	, os_error_mode
	, os_priority_class
	, max_workers_count
	, scheduler_count
	, scheduler_total_count
	, deadlock_monitor_serial_number
	, sqlserver_start_time_ms_ticks
	, sqlserver_start_time
	, affinity_type
	, affinity_type_desc
	, process_kernel_time_ms
	, process_user_time_ms
	, time_source
	, time_source_desc
	, virtual_machine_type
	, virtual_machine_type_desc
	, softnuma_configuration
	, softnuma_configuration_desc
	, sql_memory_model
	, sql_memory_model_desc
	, socket_count
	, cores_per_socket
	, numa_node_count
from sys.dm_os_sys_info;

select memory_clerk_address, type, name, memory_node_id, pages_kb / 1024 as pages_mb 
from sys.dm_os_memory_clerks 
order by pages_mb desc;
