select
	@@SERVERNAME as server_name
	, db_id() as database_id
	, db_name() as database_name
	, ds.data_space_id
	, case when ds.data_space_id is null then N'LOG' else ds.name end as file_group_name
	, case when ds.data_space_id is null then N'TRANSACTION_LOG' else ds.type_desc end as file_group_type
	, ds.is_default as is_default_file_group
	, df.file_id
	, df.name as file_name
	, df.type_desc as file_type
	, df.physical_name
	, df.size / 128 as size_mb
	, fileproperty(df.name, 'spaceused') / 128 as space_used_mb
	, is_percent_growth
	, case is_percent_growth when 1 then df.growth else df.growth / 128 end as file_growth
	, df.max_size / 128 as max_size_mb
	, vfs.NumberReads
	, vfs.BytesRead
	, vfs.IoStallReadMS
	, vfs.NumberWrites
	, vfs.BytesWritten
	, vfs.IoStallWriteMS
	, ReadIOPs = case IoStallReadMS when 0 then 0 else (NumberReads * 1000) / IoStallReadMS end
	, ReadThroughput = case IoStallReadMS when 0 then 0 else (BytesRead * 1000) / (IoStallReadMS * 1024 * 1024) end
	, ReadLatency = case NumberReads when 0 then 0 else IoStallReadMS / NumberReads end
	, WriteIOPs = case IoStallWriteMS when 0 then 0 else (NumberWrites * 1000) / IoStallWriteMS end
	, WriteThroughput = case IoStallWriteMS when 0 then 0 else (BytesWritten * 1000) / (IoStallWriteMS * 1024 * 1024) end
	, WriteLatency = case NumberWrites when 0 then 0 else IoStallWriteMS / NumberWrites end
from sys.data_spaces ds
right join sys.database_files df on df.data_space_id = ds.data_space_id
outer apply sys.fn_virtualfilestats (db_id(), df.file_id) vfs;
