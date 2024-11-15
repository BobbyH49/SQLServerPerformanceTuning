select
	database_id = db_id()
	, o.object_id
	, schema_name = sch.name
	, object_name = o.name
	, ind.index_id
	, index_name = ind.name
	, indc.key_ordinal
	, ic.column_id
	, column_name = ic.name
into #index_info
from sys.schemas sch
join sys.objects o on o.schema_id = sch.schema_id
join sys.indexes ind on ind.object_id = o.object_id
join sys.index_columns indc on indc.object_id = ind.object_id and indc.index_id = ind.index_id
join sys.columns ic on ic.object_id = indc.object_id and ic.column_id = indc.column_id
where key_ordinal > 0

select
	ind.object_id
	, ind.index_id
	, indc.column_id
	, avg_distribution = avg(stah.equal_rows)
	, max_distribution = max(stah.equal_rows)
into #index_stats_info
from sys.indexes ind
join sys.index_columns indc on indc.object_id = ind.object_id and indc.index_id = ind.index_id
cross apply sys.dm_db_stats_histogram(ind.object_id, ind.index_id) stah
where indc.key_ordinal = 1
group by
	ind.object_id
	, ind.index_id
	, indc.column_id

select
	sta.object_id
	, sta.stats_id
	, stac.column_id
	, avg_distribution = avg(stah.equal_rows)
	, max_distribution = max(stah.equal_rows)
into #stats_info
from sys.stats sta
join sys.stats_columns stac on stac.object_id = sta.object_id and stac.stats_id = sta.stats_id
cross apply sys.dm_db_stats_histogram(sta.object_id, sta.stats_id) stah
left join sys.indexes ind on ind.object_id = sta.object_id and ind.index_id = sta.stats_id
where stac.stats_column_id = 1
and ind.object_id is null
group by
	sta.object_id
	, sta.stats_id
	, stac.column_id

select
	ii.database_id
	, ii.object_id
	, ii.schema_name
	, ii.object_name
	, ii.index_id
	, ii.index_name
	, ii.key_ordinal
	, ii.column_name
	, isi.avg_distribution
	, isi.max_distribution
into #stats_dist
from #index_info ii
join #index_stats_info isi on isi.object_id = ii.object_id and isi.column_id = ii.column_id

union all

select
	ii.database_id
	, ii.object_id
	, ii.schema_name
	, ii.object_name
	, ii.index_id
	, ii.index_name
	, ii.key_ordinal
	, ii.column_name
	, si.avg_distribution
	, si.max_distribution
from #index_info ii
left join #stats_info si on si.object_id = ii.object_id and si.column_id = ii.column_id

select
	database_id
	, object_id
	, schema_name
	, object_name
	, index_id
	, index_name
	, key_ordinal
	, column_name
	, avg_distribution = max(avg_distribution)
	, max_distribution = max(max_distribution)
into #worst_case_stats_dist
from #stats_dist
group by
	database_id
	, object_id
	, schema_name
	, object_name
	, index_id
	, index_name
	, key_ordinal
	, column_name

select
	database_id
	, object_id
	, schema_name
	, object_name
	, index_id
	, index_name
	, key_ordinal
	, column_name
	, avg_distribution
	, max_distribution
	, avg_distribution_order = row_number() over(partition by object_id, index_id order by avg_distribution, key_ordinal)
	, max_distribution_order = row_number() over(partition by object_id, index_id order by max_distribution, key_ordinal)
into #ordered_distribution
from #worst_case_stats_dist

select
	od.database_id
	, od.object_id
	, od.schema_name
	, od.object_name
	, od.index_id
	, od.index_name
	, od.key_ordinal
	, od.column_name
	, si.sqlserver_start_time
	, user_seeks = case when ius.user_seeks is NULL then 0 else ius.user_seeks end
	, user_scans = case when ius.user_scans is NULL then 0 else ius.user_scans end
	, user_lookups = case when ius.user_lookups is NULL then 0 else ius.user_lookups end
	, user_updates = case when ius.user_updates is NULL then 0 else ius.user_updates end
	, ius.last_user_seek
	, ius.last_user_scan
	, ius.last_user_lookup
	, ius.last_system_update
	, od.avg_distribution
	, od.max_distribution
	, od.avg_distribution_order
	, od.max_distribution_order
	, order_issue_avg_dist = case when od.key_ordinal <> od.avg_distribution_order then 1 else 0 end
	, order_issue_max_dist = case when od.key_ordinal <> od.max_distribution_order then 1 else 0 end
from #ordered_distribution od
cross join sys.dm_os_sys_info si
left join sys.dm_db_index_usage_stats ius on ius.database_id = db_id() and ius.object_id = od.object_id and ius.index_id = od.index_id
order by
	od.schema_name
	, od.object_name
	, od.index_id
	, od.index_name
	, od.key_ordinal

drop table #index_info
drop table #index_stats_info
drop table #stats_info
drop table #stats_dist
drop table #worst_case_stats_dist
drop table #ordered_distribution
