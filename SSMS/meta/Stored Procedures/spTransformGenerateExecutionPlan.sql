CREATE PROCEDURE [meta].[spTransformGenerateExecutionPlan]
AS
BEGIN
	DECLARE @DWStagingSchemaName NVARCHAR(255);
	DECLARE @DWBridgeSchemaName NVARCHAR(255);
	DECLARE @DWCalcSchemaName NVARCHAR(255);
	DECLARE @DWDimSchemaName NVARCHAR(255);
	DECLARE @DWFactSchemaName NVARCHAR(255);

	/* Drop SourceObjectExecutionPlan table if exists */
	DROP TABLE IF EXISTS [dbo].[BusinessObjectExecutionPlan];
	
	/* Drop existing #temp table: #SourceObjectMetaData if exists */
	DROP TABLE IF EXISTS #SourceObjectMetaData;
	
	/* Create SourceObjectExecutionPlan table */
	CREATE TABLE [dbo].[BusinessObjectExecutionPlan](
		[DestinationSchemaName] [NVARCHAR](255) NULL
	,	[DestinationTableName] [NVARCHAR](255) NULL
	,	[LoadSequence] INT NULL
	,	[PrecedenceObjectSchema] [NVARCHAR](255) NULL
	,	[PrecedenceObjectName] [NVARCHAR](255) NULL
	,	[ExecutionStatusCode] [INT] NULL
	,	[ExecutionStatus] [NVARCHAR](255) NULL
	,	[ScheduleOk] [TINYINT] NULL				
	,	[IsEnabled] [TINYINT] NULL
	,	[MaxLoadSequence] INT NULL
	);

	/* Get Data Warehouse extended propertiers */
	SELECT 
		@DWStagingSchemaName	=	MAX(CASE WHEN (ep.name = 'DWTransformStagingSchemaName')	THEN CAST(ep.[value] AS NVARCHAR(255)) ELSE '' END)
	,	@DWBridgeSchemaName		=	MAX(CASE WHEN (ep.name = 'DWBridgePrefix')					THEN CAST(ep.[value] AS NVARCHAR(255)) ELSE '' END)
	,	@DWCalcSchemaName		=	MAX(CASE WHEN (ep.name = 'DWAppendixPrefix')				THEN CAST(ep.[value] AS NVARCHAR(255)) ELSE '' END)
	,	@DWDimSchemaName		=	MAX(CASE WHEN (ep.name = 'DWDimensionPrefix')				THEN CAST(ep.[value] AS NVARCHAR(255)) ELSE '' END)
	,	@DWFactSchemaName		=	MAX(CASE WHEN (ep.name = 'DWFactPrefix')					THEN CAST(ep.[value] AS NVARCHAR(255)) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0)
	GROUP BY (ep.major_id)

	/* Generates a metadata variable which contains all extended properties related to transformation Data Warehouse entities */
	SELECT 
		[DataWarehouseLayer]
    ,	[BusinessObjectSchema]
    ,	[BusinessObjectName]
    ,	[SourceObjectSchema]
    ,	[SourceObjectName]
    ,	[BusinessObjectSchedule]
    ,	[LastProcessingDate]
    ,	[ScheduleOk]
    ,	[LoadModeETL]
    ,	[IsReset]
    ,	[IncrementalField]
    ,	[IncrementalOffSet]
    ,	[BusinessObjectLookupKey]
    ,	[RolePlayingEntity]
    ,	[PreserveSCD2History]
    ,	[IsEnabled]
	INTO #SourceObjectMetaData
	FROM meta.businessObject;

	/* Generate new execution plan based on dependencies between TRANSFORM layer and EDW tables */
	WITH Cte_Tree AS (
		/* Find all transformation entities which do not have any precedence object dependencies - set ExecutionStatusCode = 0 (Ready) */
		SELECT
			[DestinationSchema]			=	md.BusinessObjectSchema
		,	[DestinationTable]			=	md.BusinessObjectName
		,	[SourceObjectSchema]		=	md.SourceObjectSchema	
		,	[SourceObjectName]			=	md.SourceObjectName
		,	[PrecedenceObjectSchema]	=	md.SourceObjectSchema
		,	[PrecedenceObjectName]		=	md.SourceObjectName
		,	[LoadSequence]				=	1
		,	[ExecutionStatusCode]		=	0
		,	[ScheduleOk]				=	md.ScheduleOk
		,	[IsEnabled]					=	md.IsEnabled
		FROM #SourceObjectMetaData AS md
		WHERE NOT EXISTS (
			SELECT 1 FROM sys.sql_expression_dependencies AS d WITH (NOLOCK) 
			WHERE 
				(d.referencing_id = OBJECT_ID(md.SourceObjectSchema + '.' + md.SourceObjectName)) 
			AND (d.referenced_schema_name IN (@DWBridgeSchemaName, @DWCalcSchemaName, @DWDimSchemaName, @DWFactSchemaName)) 
			AND (d.referenced_entity_name <> md.BusinessObjectName) /* This condition prevents that a stored procedure referring its own DW table */
		)

		UNION ALL

		/* Find all transformation entities which do have precedence object dependencies - set ExecutionStatusCode = -1 (NotReady) */
		SELECT 
			[DestinationSchema]			=	md.BusinessObjectSchema
		,	[DestinationTable]			=	md.BusinessObjectName
		,	[SourceObjectSchema]		=	md.SourceObjectSchema
		,	[SourceObjectName]			=	md.SourceObjectName
		,	[PrecedenceObjectSchema]	=	tree.SourceObjectSchema
		,	[PrecedenceObjectName]		=	tree.SourceObjectName
		,	[LoadSequence]				=	tree.LoadSequence + 1
		,	[ExecutionStatusCode]		=	-1
		,	[ScheduleOk]				=	tree.ScheduleOk
		,	[IsEnabled]					=	tree.IsEnabled
		FROM Cte_Tree AS tree 
		INNER JOIN sys.sql_expression_dependencies AS d WITH (NOLOCK) ON (d.referenced_schema_name IN (@DWBridgeSchemaName, @DWCalcSchemaName, @DWDimSchemaName, @DWFactSchemaName)) AND (referenced_entity_name = tree.DestinationTable)
		INNER JOIN #SourceObjectMetaData AS md ON d.referencing_id = (OBJECT_ID(md.SourceObjectSchema + '.' + md.SourceObjectName)) AND (d.referenced_entity_name <> md.BusinessObjectName)
	)

	INSERT INTO [dbo].[BusinessObjectExecutionPlan] (
		[DestinationSchemaName]		
	,	[DestinationTableName]		
	,	[LoadSequence]				
	,	[PrecedenceObjectSchema]	
	,	[PrecedenceObjectName]		
	,	[ExecutionStatusCode]		
	,	[ExecutionStatus]			
	,	[ScheduleOk]				
	,	[IsEnabled]					
	,	[MaxLoadSequence]
	)

	SELECT DISTINCT
		[DestinationSchemaName]		=	so.BusinessObjectSchema		
	,	[DestinationTableName]		=	so.BusinessObjectName
	,	[LoadSequence]				=	MAX(x.[LoadSequence]) OVER (PARTITION BY x.DestinationSchema, x.DestinationTable)
	,	[PrecedenceObjectSchema]	=	ISNULL(x.PrecedenceObjectSchema, so.SourceObjectSchema)
	,	[PrecedenceObjectName]		=	ISNULL(x.PrecedenceObjectName, so.SourceObjectName)
	,	[ExecutionStatusCode]		=	CASE WHEN so.ScheduleOk = 1 AND so.IsEnabled = 1 THEN x.ExecutionStatusCode ELSE 0 END
	,	[ExecutionStatus]			=	CASE WHEN so.ScheduleOk = 1 AND so.IsEnabled = 1 THEN '' ELSE 'Running' END
	,	[ScheduleOk]				=	x.ScheduleOk
	,	[IsEnabled]					=	x.IsEnabled
	,	[MaxLoadSequence]			=	MAX(x.[LoadSequence]) OVER ()
	FROM #SourceObjectMetaData AS so 
	LEFT JOIN Cte_Tree AS x ON so.BusinessObjectSchema = x.DestinationSchema AND so.BusinessObjectName = x.DestinationTable
END