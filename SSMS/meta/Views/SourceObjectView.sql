CREATE VIEW [meta].[SourceObjectView]
AS
WITH cte_so AS (
	SELECT 
		[DataWarehouseLayer]		=	MAX(CASE WHEN es.name = 'DataWarehouseLayer'		THEN CAST(es.value AS NVARCHAR(255)) ELSE '' END)
	,	[DestinationSchemaName]		=	CAST(s.name AS NVARCHAR(255))
	,	[DestinationTableName]		=	CAST(o.name AS NVARCHAR(255))
	,	[DataSourceName]			=	MAX(CASE WHEN es.name = 'DataSourceName'			THEN CAST(es.value AS NVARCHAR(255)) ELSE '' END)
	,	[DataSourceServerName]		=	MAX(CASE WHEN es.name = 'DataSourceServerName'		THEN CAST(es.value AS NVARCHAR(255)) ELSE '' END)
	,	[DataSourceDatabaseName]	=	MAX(CASE WHEN es.name = 'DataSourceDatabaseName'	THEN CAST(es.value AS NVARCHAR(255)) ELSE '' END)
	,	[DataSourceType]			=	MAX(CASE WHEN es.name = 'DataSourceType'			THEN CAST(es.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectSchema]		=	MAX(CASE WHEN ep.name = 'SourceObjectSchema'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectName]			=	MAX(CASE WHEN ep.name = 'SourceObjectName'			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectSchedule]		=	MAX(CASE WHEN ep.name = 'SourceObjectSchedule'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[LastProcessingDate]		=	MAX(ISNULL(DATEADD(HOUR, DATEDIFF(HOUR, 0, o.modify_date), 0), '')) 
	,	[LoadModeETL]				=	MAX(CASE WHEN ep.name = 'LoadModeETL'				THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[IsReset]					=	MAX(CASE WHEN ep.name = 'IsReset'					THEN CAST(ep.value AS TINYINT) ELSE 0 END)
	,	[IncrementalField]			=	MAX(CASE WHEN ep.name = 'IncrementalField'			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[IncrementalOffSet]			=	MAX(CASE WHEN ep.name = 'IncrementalOffSet'			THEN CAST(ep.value AS INT) ELSE 0 END)
	,	[PreserveSCD2History]		=	MAX(CASE WHEN ep.name = 'PreserveSCD2History'		THEN CAST(ep.value AS INT) ELSE 0 END)
	,	[SourceObjectFilter]		=	MAX(CASE WHEN ep.name = 'SourceObjectFilter'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[IsEnabled]					=	MAX(CASE WHEN ep.name = 'IsEnabled'					THEN CAST(ep.value AS TINYINT) ELSE 0 END)
	FROM sys.objects AS o WITH (NOLOCK)
	INNER JOIN sys.schemas AS s WITH (NOLOCK) ON (s.schema_id = o.schema_id) 
	INNER JOIN sys.extended_properties AS ep WITH (NOLOCK) ON (ep.major_id = o.object_id)
	INNER JOIN sys.extended_properties AS es WITH (NOLOCK) ON (es.major_id = s.schema_id)
	WHERE (o.type = 'U')
	GROUP BY s.name, o.name
)

SELECT TOP 10000
	[DataWarehouseLayer]		=	[DataWarehouseLayer]
,	[DestinationSchemaName]		=	[DestinationSchemaName]		
,	[DestinationTableName]		=	[DestinationTableName]
,	[DataSourceName]			=	[DataSourceName]
,	[DataSourceServerName]		=	[DataSourceServerName]	
,	[DataSourceDatabaseName]	=	[DataSourceDatabaseName]
,	[DataSourceType]			=	[DataSourceType]		
,	[SourceObjectSchema]		=	[SourceObjectSchema]	
,	[SourceObjectName]			=	[SourceObjectName]		
,	[SourceObjectSchedule]		=	[SourceObjectSchedule]
,	[LastProcessingDate]		=	[LastProcessingDate]
,	[ScheduleOk]				=	CASE WHEN [IsReset] = 0 THEN [ScheduleOk] ELSE 1 END
,	[LoadModeETL]				=	[LoadModeETL]			
,	[IsReset]					=	[IsReset]				
,	[IncrementalField]			=	[IncrementalField]		
,	[IncrementalOffSet]			=	[IncrementalOffSet]
,	[PreserveSCD2History]		=	[PreserveSCD2History]
,	[SourceObjectFilter]		=	[SourceObjectFilter]	
,	[IsEnabled]					=	[IsEnabled]				
,	[LoadSequence]				=	ABS(CHECKSUM(NEWID()) % ep.DefaultMaxDop) + 1
,	[MaxLoadSequence]			=	ep.DefaultMaxDop
FROM cte_so AS so
CROSS APPLY dbo.fnCheckSchedule(so.SourceObjectSchedule, so.LastProcessingDate) AS x
CROSS APPLY (
	SELECT DefaultMaxDop = CAST(ep.[value] AS INT)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0) AND (ep.name = 'DefaultMaxDop')
) AS ep
WHERE (so.DataWarehouseLayer = 'Source')
ORDER BY 2,3