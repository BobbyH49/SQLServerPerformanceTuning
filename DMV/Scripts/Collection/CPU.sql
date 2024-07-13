WITH XmlData
AS (
	SELECT
		Timestamp = dateadd(ms, rb.timestamp - si.ms_ticks, GETUTCDATE())
		, record = cast(rb.record as xml)
	FROM sys.dm_os_ring_buffers rb
	CROSS JOIN sys.dm_os_sys_info si
	WHERE ring_buffer_type = N'RING_BUFFER_SCHEDULER_MONITOR'
),
RelationalData
As
(
	SELECT
		Timestamp
		, RecordId = record.value(N'(/Record/@id)[1]', N'INT')
		, SqlCpu = record.value(N'(/Record/SchedulerMonitorEvent/SystemHealth/ProcessUtilization)[1]', N'INT')
		, IdleCpu = record.value(N'(/Record/SchedulerMonitorEvent/SystemHealth/SystemIdle)[1]', N'INT')
	FROM XmlData
)
SELECT
	ServerName_s = @@SERVERNAME
	, record_id_d = RecordId
	, timestamp_d = cast((datediff(d,cast('1900-01-01' as date), Timestamp)) * 4294967296 + (datediff(s,cast(Timestamp as date), Timestamp) * 300) as bigint)
	, timestamp_datetime = Timestamp
	, sql_cpu_d = SqlCpu
	, idle_cpu_d = IdleCpu
	, other_cpu_d = 100 - SqlCpu - IdleCpu
FROM RelationalData
ORDER BY Timestamp DESC
