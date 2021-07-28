CREATE VIEW [meta].[BusinessObjectView]
AS
WITH cte_ep AS (
	SELECT DWTransformStagingSchemaName = CAST(ep.value AS NVARCHAR(255))
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0) AND (ep.name = 'DWTransformStagingSchemaName')
),

cte_bo AS (
	SELECT
		[BusinessObjectSchema]		=	MAX(CASE WHEN (ep.[name] = 'BusinessObjectSchema') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[BusinessObjectName]		=	CAST(o.name AS NVARCHAR(255))
	,	[DataWarehouseLayer]		=	MAX(CASE WHEN (ep.[name] = 'DataWarehouseLayer') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectSchema]		=	MAX(CASE WHEN (ep.[name] = 'SourceObjectSchema') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END) 
	,	[SourceObjectName]			=	MAX(CASE WHEN (ep.[name] = 'SourceObjectName') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END) 
	,	[BusinessObjectSchedule]	=	MAX(CASE WHEN (ep.[name] = 'BusinessObjectSchedule') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[LastProcessingDate]		=	MAX(ISNULL(DATEADD(HOUR, DATEDIFF(HOUR, 0, o.modify_date), 0), '')) 
	,	[LoadModeETL]				=	MAX(CASE WHEN (ep.[name] = 'LoadModeETL') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[IsReset]					=	MAX(CASE WHEN (ep.[name] = 'IsReset') THEN CAST(ep.value AS TINYINT) ELSE '' END) 
	,	[IncrementalField]			=	MAX(CASE WHEN (ep.[name] = 'IncrementalField') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END) 
	,	[IncrementalOffSet]			=	MAX(CASE WHEN (ep.[name] = 'IncrementalOffSet') THEN CAST(ep.value AS INT) ELSE '' END)
	,	[BusinessObjectLookupKey]	=	MAX(CASE WHEN (ep.[name] = 'BusinessObjectLookupKey') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[RolePlayingEntity]			=	MAX(CASE WHEN (ep.[name] = 'RolePlayingEntity') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[PreserveSCD2History]		=	MAX(CASE WHEN (ep.[name] = 'PreserveSCD2History') THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END) 
	,	[IsEnabled]					=	MAX(CASE WHEN (ep.[name] = 'IsEnabled') THEN CAST(ep.value AS TINYINT) ELSE '' END) 
	FROM sys.objects AS o WITH (NOLOCK) 
	JOIN sys.schemas As s WITH (NOLOCK) ON o.schema_id = s.schema_id
	JOIN sys.extended_properties AS ep WITH (NOLOCK) ON o.object_id = ep.major_id
	WHERE (s.name = (SELECT DWTransformStagingSchemaName FROM cte_ep))
	GROUP BY s.name, o.name
)

SELECT TOP 10000
	[DataWarehouseLayer]		=	[DataWarehouseLayer]		
,	[BusinessObjectSchema]		=	[BusinessObjectSchema]		
,	[BusinessObjectName]		=	[BusinessObjectName]		
,	[SourceObjectSchema]		=	[SourceObjectSchema]		
,	[SourceObjectName]			=	[SourceObjectName]			
,	[BusinessObjectSchedule]	=	[BusinessObjectSchedule]	
,	[LastProcessingDate]		=	[LastProcessingDate]
,	[ScheduleOk]				=	CASE WHEN [IsReset] = 0 THEN [ScheduleOk] ELSE 1 END
,	[LoadModeETL]				=	[LoadModeETL]				
,	[IsReset]					=	[IsReset]					
,	[IncrementalField]			=	[IncrementalField]			
,	[IncrementalOffSet]			=	[IncrementalOffSet]			
,	[BusinessObjectLookupKey]	=	[BusinessObjectLookupKey]	
,	[RolePlayingEntity]			=	[RolePlayingEntity]			
,	[PreserveSCD2History]		=	[PreserveSCD2History]		
,	[IsEnabled]					=	[IsEnabled]					
FROM cte_bo AS bo
CROSS APPLY dbo.fnCheckSchedule(bo.BusinessObjectSchedule, bo.LastProcessingDate) AS x
ORDER BY 2,3