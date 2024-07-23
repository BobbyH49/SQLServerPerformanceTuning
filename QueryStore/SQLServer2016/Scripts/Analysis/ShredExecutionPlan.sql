SET NOCOUNT ON;

-- Replace the query_plan_hash with your own
DECLARE @query_plan_hash BINARY(8) = 0xE6878FE32F752580;

-- Get all of the indexes used
WITH XMLNAMESPACES(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
, plans AS (
	SELECT
		qp.query_id
		, qp.plan_id
		, qp.query_plan_hash
		, query_plan = TRY_CAST(qp.query_plan AS XML)
	FROM sys.query_store_plan qp
	WHERE qp.query_plan_hash = @query_plan_hash
)
, indexes AS (
	SELECT
		p.query_id
		, p.plan_id
		, p.query_plan_hash
		, SchemaName = a.value(N'(@Schema)[1]', N'NVARCHAR(130)')
		, TableName = a.value(N'(@Table)[1]', N'NVARCHAR(130)')
		, IndexName = a.value(N'(@Index)[1]', N'NVARCHAR(130)')
	FROM plans p
	CROSS APPLY p.query_plan.nodes(N'//Object') obj (a)
)
SELECT DISTINCT query_id, plan_id, query_plan_hash, SchemaName, TableName, IndexName
FROM indexes;

-- Get all of the statistics that were analysed to generate the original plan
WITH XMLNAMESPACES(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
, plans as (
	SELECT
		qp.query_id
		, qp.plan_id
		, qp.query_plan_hash
		, query_plan = TRY_CAST(qp.query_plan AS XML)
	FROM sys.query_store_plan qp
	WHERE qp.query_plan_hash = @query_plan_hash
)
, stats AS (
	SELECT
		p.query_id
		, p.plan_id
		, p.query_plan_hash
		, SchemaName = a.value(N'(@Schema)[1]', N'NVARCHAR(130)')
		, TableName = a.value(N'(@Table)[1]', N'NVARCHAR(130)')
		, StatsName = a.value(N'(@Statistics)[1]', N'NVARCHAR(130)')
		, SamplingPercent = a.value(N'(@SamplingPercent)[1]', N'DECIMAL(5, 2)')
		, ModificationCount = a.value(N'(@ModificationCount)[1]', N'INT')
		, LastUpdate = a.value(N'(@LastUpdate)[1]', N'DATETIME')
	FROM plans p
	CROSS APPLY p.query_plan.nodes(N'/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/QueryPlan/OptimizerStatsUsage/StatisticsInfo') obj (a)
)
SELECT query_id, plan_id, query_plan_hash, SchemaName, TableName, StatsName, SamplingPercent, ModificationCount, LastUpdate
FROM stats;

-- Get all of the operations involved in the plan
WITH XMLNAMESPACES(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
, plans as (
	SELECT
		qp.query_id
		, qp.plan_id
		, qp.query_plan_hash
		, query_plan = TRY_CAST(qp.query_plan AS XML)
	FROM sys.query_store_plan qp
	WHERE qp.query_plan_hash = @query_plan_hash
)
, opNodes AS (
	SELECT
		p.query_id
		, p.plan_id
		, p.query_plan_hash
		, StatementType = a.value(N'(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@StatementType)[1]', N'NVARCHAR(10)')
		, StatementParameterizationType = a.value(N'(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@StatementParameterizationType)[1]', N'TINYINT')
		, StatementSubTreeCost = a.value(N'(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@StatementSubTreeCost)[1]', N'FLOAT')
		, StatementEstRows = a.value(N'(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@StatementEstRows)[1]', N'FLOAT')
		, StatementOptmLevel = a.value(N'(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@StatementOptmLevel)[1]', N'NVARCHAR(20)')
		, CardinalityEstimationModelVersion = a.value(N'(/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/@CardinalityEstimationModelVersion)[1]', N'SMALLINT')
		, RelOpNodeId = a.value(N'(@NodeId)[1]', N'INT')
		, PhysicalOp = a.value(N'(@PhysicalOp)[1]', N'NVARCHAR(50)')
		, LogicalOp = a.value(N'(@LogicalOp)[1]', N'NVARCHAR(50)')
		, EstimateRows = a.value(N'(@EstimateRows)[1]', N'FLOAT')
		, EstimatedRowsRead = a.value(N'(@EstimatedRowsRead)[1]', N'FLOAT')
		, EstimateIO = a.value(N'(@EstimateIO)[1]', N'FLOAT')
		, EstimateCPU = a.value(N'(@EstimateCPU)[1]', N'FLOAT')
		, AvgRowSize = a.value(N'(@AvgRowSize)[1]', N'FLOAT')
		, EstimatedTotalSubtreeCost = a.value(N'(@EstimatedTotalSubtreeCost)[1]', N'FLOAT')
		, TableCardinality = a.value(N'(@TableCardinality)[1]', N'FLOAT')
		, Parallel = a.value(N'(@Parallel)[1]', N'TINYINT')
		, EstimateRewinds = a.value(N'(@EstimateRewinds)[1]', N'FLOAT')
		, EstimateRebinds = a.value(N'(@EstimateRebinds)[1]', N'FLOAT')
		, EstimatedExecutionMode = a.value(N'(@EstimatedExecutionMode)[1]', N'NVARCHAR(10)')
		, OrderedScan = b.value(N'(@Ordered)[1]', N'TINYINT')
		, ScanDirection = b.value(N'(@ScanDirection)[1]', N'NVARCHAR(20)')
		, ForcedIndex = b.value(N'(@ForcedIndex)[1]', N'TINYINT')
		, ForceSeek = b.value(N'(@ForceSeek)[1]', N'TINYINT')
		, ForceScan = b.value(N'(@ForceScan)[1]', N'TINYINT')
		, NoExpandHint = b.value(N'(@NoExpandHint)[1]', N'TINYINT')
		, Storage = b.value(N'(@Storage)[1]', N'NVARCHAR(20)')
		, SchemaName = b.value(N'(Object/@Schema)[1]', N'NVARCHAR(130)')
		, TableName = b.value(N'(Object/@Table)[1]', N'NVARCHAR(130)')
		, IndexName = b.value(N'(Object/@Index)[1]', N'NVARCHAR(130)')
	FROM plans p
	CROSS APPLY p.query_plan.nodes(N'//RelOp') relop (a)
	OUTER APPLY a.nodes(N'IndexScan') ind (b)
)
SELECT 
	query_id
	, plan_id
	, query_plan_hash
	, StatementType
	, StatementParameterizationType
	, StatementSubTreeCost
	, StatementEstRows
	, StatementOptmLevel
	, CardinalityEstimationModelVersion
	, RelOpNodeId
	, PhysicalOp
	, LogicalOp
	, EstimateRows
	, EstimatedRowsRead
	, EstimateIO
	, EstimateCPU
	, AvgRowSize
	, EstimatedTotalSubtreeCost
	, TableCardinality
	, Parallel
	, EstimateRewinds
	, EstimateRebinds
	, EstimatedExecutionMode
	, OrderedScan
	, ScanDirection
	, ForcedIndex
	, ForceSeek
	, ForceScan
	, NoExpandHint
	, Storage
	, SchemaName
	, TableName
	, IndexName
FROM opNodes;

-- Get the parameters and values used to compile the plan (if applicable)
WITH XMLNAMESPACES(DEFAULT 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
, plans AS (
	SELECT
		qp.query_id
		, qp.plan_id
		, qp.query_plan_hash
		, query_plan = TRY_CAST(qp.query_plan AS XML)
	FROM sys.query_store_plan qp
	WHERE qp.query_plan_hash = @query_plan_hash
)
, parameters AS (
	SELECT
		p.query_id
		, p.plan_id
		, p.query_plan_hash
		, Parameter = a.value(N'(@Column)[1]', N'NVARCHAR(130)')
		, ParameterDataType = a.value(N'(@ParameterDataType)[1]', N'NVARCHAR(130)')
		, ParameterCompiledValue = a.value(N'(@ParameterCompiledValue)[1]', N'NVARCHAR(500)')
	FROM plans p
	CROSS APPLY p.query_plan.nodes(N'/ShowPlanXML/BatchSequence/Batch/Statements/StmtSimple/QueryPlan/ParameterList/ColumnReference') params (a)
)
SELECT query_id, plan_id, query_plan_hash, Parameter, ParameterDataType, ParameterCompiledValue
FROM parameters;
