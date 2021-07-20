CREATE FUNCTION [meta].[fnSourceObject] ()
RETURNS @Result TABLE (
	[DestinationSchemaName]		NVARCHAR(255)
,	[DestinationTableName]		NVARCHAR(255)
,	[DataWarehouseLayer]		NVARCHAR(255)
,	[DataSourceName]			NVARCHAR(255)
,	[DataSourceServerName]		NVARCHAR(255)
,	[DataSourceDatabaseName]	NVARCHAR(255)
,	[DataSourceType]			NVARCHAR(255)
,	[SourceObjectSchema]		NVARCHAR(255)
,	[SourceObjectName]			NVARCHAR(255)
,	[SourceObjectPrefix]		NVARCHAR(255)
,	[SourceObjectLookupKey]		NVARCHAR(255)
,	[SourceObjectSchedule]		NVARCHAR(255)
,	[LastProcessingDate]		DATETIME
,	[LoadModeETL]				NVARCHAR(255)
,	[IsReset]					TINYINT
,	[IncrementalField]			NVARCHAR(255)
,	[IncrementalOffSet]			INT
,	[PreserveSCD2History]		INT
,	[RolePlayingEntity]			NVARCHAR(255)
,	[SourceObjectFilter]		NVARCHAR(255)
,	[IsEnabled]					TINYINT
,	[DefaultMaxDop]				INT
,	[LoadSequence]				INT
,	[MaxLoadSequence]			INT
)
AS

BEGIN
	DECLARE @SourceObjectMetaData TABLE (
		[DestinationSchemaName]		NVARCHAR(255)
	,	[DestinationTableName]		NVARCHAR(255)
	,	[DataWarehouseLayer]		NVARCHAR(255)
	,	[DataSourceName]			NVARCHAR(255)
	,	[DataSourceServerName]		NVARCHAR(255)
	,	[DataSourceDatabaseName]	NVARCHAR(255)
	,	[DataSourceType]			NVARCHAR(255)
	,	[SourceObjectSchema]		NVARCHAR(255)
	,	[SourceObjectName]			NVARCHAR(255)
	,	[SourceObjectPrefix]		NVARCHAR(255)
	,	[SourceObjectLookupKey]		NVARCHAR(255)
	,	[SourceObjectSchedule]		NVARCHAR(255)
	,	[LastProcessingDate]		DATETIME
	,	[LoadModeETL]				NVARCHAR(255)
	,	[IsReset]					TINYINT
	,	[IncrementalField]			NVARCHAR(255)
	,	[IncrementalOffSet]			INT
	,	[PreserveSCD2History]		INT
	,	[RolePlayingEntity]			NVARCHAR(255)
	,	[SourceObjectFilter]		NVARCHAR(255)
	,	[IsEnabled]					TINYINT	
	);

	DECLARE @LoadThread TABLE ([SourceObjectSchema] NVARCHAR(255), [SourceObjectName] NVARCHAR(255), [LoadSequence] INT, [MaxLoadSequence] INT);
	
	DECLARE @DefaultMaxDop INT;
	DECLARE @DWTransformStagingSchemaName NVARCHAR(255);
	DECLARE @DWTransformDWSchemaName NVARCHAR(255);

	SELECT 
		@DefaultMaxDop					=	MAX(CASE WHEN (ep.name = 'DefaultMaxDop')					THEN CAST(ep.[value] AS NVARCHAR(255)) ELSE '' END)
	,	@DWTransformStagingSchemaName	=	MAX(CASE WHEN (ep.name = 'DWTransformStagingSchemaName')	THEN CAST(ep.[value] AS NVARCHAR(255)) ELSE '' END)
	,	@DWTransformDWSchemaName		=	MAX(CASE WHEN (ep.name = 'DWTransformDWSchemaName')			THEN CAST(ep.[value] AS NVARCHAR(255)) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0)
	GROUP BY (ep.major_id)

	/* Generates a metadata variable which contains all extended properties related to Ingestion and transformation Data Warehouse tables and entities */
	INSERT INTO @SourceObjectMetaData 
	SELECT 
		[DestinationSchemaName]		=	CAST(s.name AS NVARCHAR(255))
	,	[DestinationTableName]		=	CAST(o.name AS NVARCHAR(255))
	,	[DataWarehouseLayer]		=	MAX(CASE WHEN ep.name = 'DataWarehouseLayer'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[DataSourceName]			=	MAX(CASE WHEN ep.name = 'DataSourceName'			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[DataSourceServerName]		=	MAX(CASE WHEN ep.name = 'DataSourceServerName'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[DataSourceDatabaseName]	=	MAX(CASE WHEN ep.name = 'DataSourceDatabaseName'	THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[DataSourceType]			=	MAX(CASE WHEN ep.name = 'DataSourceType'			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectSchema]		=	MAX(CASE WHEN ep.name = 'SourceObjectSchema'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectName]			=	MAX(CASE WHEN ep.name = 'SourceObjectName'			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectPrefix]		=	MAX(CASE WHEN ep.name = 'SourceObjectPrefix'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectLookupKey]		=	MAX(CASE WHEN ep.name = 'SourceObjectLookupKey'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectSchedule]		=	MAX(CASE WHEN ep.name = 'SourceObjectSchedule'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[LastProcessingDate]		=	MAX(ISNULL(DATEADD(HOUR, DATEDIFF(HOUR, 0, o.modify_date), 0), '')) 
	,	[LoadModeETL]				=	MAX(CASE WHEN ep.name = 'LoadModeETL'				THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[IsReset]					=	MAX(CASE WHEN ep.name = 'IsReset'					THEN CAST(ep.value AS TINYINT) ELSE 0 END)
	,	[IncrementalField]			=	MAX(CASE WHEN ep.name = 'IncrementalField'			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[IncrementalOffSet]			=	MAX(CASE WHEN ep.name = 'IncrementalOffSet'			THEN CAST(ep.value AS INT) ELSE 0 END)
	,	[PreserveSCD2History]		=	MAX(CASE WHEN ep.name = 'PreserveSCD2History'		THEN CAST(ep.value AS INT) ELSE 0 END)
	,	[RolePlayingEntity]			=	MAX(CASE WHEN ep.name = 'RolePlayingEntity'			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[SourceObjectFilter]		=	MAX(CASE WHEN ep.name = 'SourceObjectFilter'		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	[IsEnabled]					=	MAX(CASE WHEN ep.name = 'IsEnabled'					THEN CAST(ep.value AS TINYINT) ELSE 0 END)
	FROM sys.objects AS o WITH (NOLOCK)
	INNER JOIN sys.schemas AS s WITH (NOLOCK) ON (s.schema_id = o.schema_id) 
	INNER JOIN sys.extended_properties AS ep WITH (NOLOCK) ON (ep.major_id = o.object_id) OR (ep.major_id = s.schema_id)
	WHERE (o.type = 'U')
	GROUP BY s.name, o.name
	ORDER BY s.name, o.name;

	/* Generate new execution plan based on dependencies between TRANSFORM layer and EDW tables */
	WITH Cte_Tree AS (
		SELECT
			[SourceObjectSchema]	=	md.SourceObjectSchema	
		,	[SourceObjectName]		=	md.SourceObjectName
		,	[DestinationTable]		=	md.DestinationTableName
		,	[LoadSequence]			=	1
		FROM @SourceObjectMetaData AS md
		WHERE ([DestinationSchemaName] = @DWTransformStagingSchemaName) 
		AND NOT EXISTS (
			SELECT 1 FROM sys.sql_expression_dependencies AS d WITH (NOLOCK) 
			WHERE 
				(d.referencing_id = OBJECT_ID(md.SourceObjectSchema + '.' + md.SourceObjectName)) 
			AND (d.referenced_schema_name = @DWTransformDWSchemaName) 
			AND (d.referenced_entity_name <> md.DestinationTableName) /* This condition prevents that a stored procedure referring its own DW table */
		)

		UNION ALL

		SELECT 
			[SourceObjectSchema]	=	md.SourceObjectSchema
		,	[SourceObjectName]		=	md.SourceObjectName
		,	[DestinationTable]		=	md.DestinationTableName
		,	[LoadSequence]			=	tree.LoadSequence + 1
		FROM Cte_Tree AS tree 
		INNER JOIN sys.sql_expression_dependencies AS d WITH (NOLOCK) ON (d.referenced_schema_name = @DWTransformDWSchemaName) AND (referenced_entity_name = tree.DestinationTable)
		INNER JOIN @SourceObjectMetaData AS md ON d.referencing_id = (OBJECT_ID(md.SourceObjectSchema + '.' + md.SourceObjectName)) AND (d.referenced_entity_name <> md.DestinationTableName)
		WHERE (md.[DestinationSchemaName] = @DWTransformStagingSchemaName)
	)

	INSERT INTO @LoadThread ([SourceObjectSchema], [SourceObjectName], [LoadSequence], [MaxLoadSequence])
	SELECT DISTINCT
		[SourceObjectSchema]	
	,	[SourceObjectName]	
	,	[LoadSequence]		=	MAX([LoadSequence]) OVER(PARTITION BY [SourceObjectSchema], [SourceObjectName])
	,	[MaxLoadSequence]	=	MAX([LoadSequence]) OVER()
	FROM Cte_Tree AS x

	INSERT INTO @Result
	SELECT
		[DestinationSchemaName]		=	piv.[DestinationSchemaName] 
	,	[DestinationTableName]		=	piv.[DestinationTableName]
	,	[DataWarehouseLayer]		=	piv.[DataWarehouseLayer]
	,	[DataSourceName]			=	piv.[DataSourceName]
	,	[DataSourceServerName]		=	piv.[DataSourceServerName] 
	,	[DataSourceDatabaseName]	=	piv.[DataSourceDatabaseName] 
	,	[DataSourceType]			=	piv.[DataSourceType] 
	,	[SourceObjectSchema]		=	piv.[SourceObjectSchema] 
	,	[SourceObjectName]			=	piv.[SourceObjectName] 
	,	[SourceObjectPrefix]		=	piv.[SourceObjectPrefix]
	,	[SourceObjectLookupKey]		=	piv.[SourceObjectLookupKey]
	,	[SourceObjectSchedule]		=	piv.[SourceObjectSchedule]
	,	[LastProcessingDate]		=	piv.[LastProcessingDate]
	,	[LoadModeETL]				=	piv.[LoadModeETL] 
	,	[IsReset]					=	piv.[IsReset]
	,	[IncrementalField]			=	piv.[IncrementalField] 
	,	[IncrementalOffSet]			=	piv.[IncrementalOffSet]
	,	[PreserveSCD2History]		=	piv.[PreserveSCD2History]
	,	[RolePlayingEntity]			=	piv.[RolePlayingEntity]
	,	[SourceObjectFilter]		=	piv.[SourceObjectFilter] 
	,	[IsEnabled]					=	piv.[IsEnabled]
	,	[DefaultMaxDop]				=	@DefaultMaxDop
	,	[LoadSequence]				=	l.LoadSequence
	,	[MaxLoadSequence]			=	CASE 
											WHEN piv.DestinationSchemaName = @DWTransformStagingSchemaName THEN l.[MaxLoadSequence] 
											ELSE @DefaultMaxDop
										END
	FROM @SourceObjectMetaData AS piv
	LEFT JOIN @LoadThread AS l ON piv.SourceObjectSchema = l.SourceObjectSchema AND piv.SourceObjectName = l.SourceObjectName

	RETURN
END