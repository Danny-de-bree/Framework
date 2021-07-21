CREATE PROCEDURE [meta].[spLookupSourceObject]

	@DataSourceType NVARCHAR(255) = '' /* Only used in On-premises solutions */

AS
BEGIN
	DECLARE @FilterCondition NVARCHAR(255);
	DECLARE @stmt NVARCHAR(MAX);
	DECLARE @Environment NVARCHAR(255);
	DECLARE @IsCloud TINYINT = 0;

	/* Is the Data Warehouse running in cloud or on-premises ? */
	IF (OBJECT_ID('sys.database_service_objectives')) IS NOT NULL SET @IsCloud = 1

	/* If running on-premises - Drop #TempTable if exists */
	DROP TABLE IF EXISTS #TempTable ;

	/* Which environment are we running Dev, Test, PreProd or Prod */
	SELECT 
		@Environment	=	MAX(CASE WHEN (ep.name = 'Environment') THEN CAST(ep.[value] AS NVARCHAR(255)) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0)
	GROUP BY (ep.major_id)

	/* If the solution is running in cloud utilize Azure Data Factory parallel execution */
	IF (@IsCloud = 1)
	BEGIN

		/* Fetch all enabled objects in meta.SourceObject and assign a LoadSequence */
		SET @stmt = '
			SELECT
				[DestinationSchemaName]		=	so.[DestinationSchemaName]		
			,	[DestinationTableName]		=	so.[DestinationTableName]
			,	[LoadSequence]				=	so.[LoadSequence]
			,	[DataSourceType]			=	so.[DataSourceType]
			,	[MaxLoadSequence]			=	so.[MaxLoadSequence]
			FROM [meta].[SourceObjectView] AS so
			WHERE 
				(so.[IsEnabled] = 1) ' + CASE WHEN (@environment != 'Dev') THEN 'AND (so.[ScheduleOk] = 1)' ELSE '' END + ' 
			ORDER BY (SELECT NEWID())
		';

	END;

	/* If solution is on-premises apply threading which matches SSIS */
	IF (@IsCloud = 0)
	BEGIN
		
		/* The parameter @DataSourceType is only relevant in a on-premises solution - fail component if @DataSourceType = '' and DestinationSchema is Extract staging layer */
		IF (@DataSourceType = '') SET @DataSourceType = NULL

		/* Fetch all enabled objects in meta.SourceObject and assign a LoadSequence */	
		SET @stmt = '
			DECLARE @DestinationSchemaName	NVARCHAR(255);
			DECLARE @DestinationTableName	NVARCHAR(255);
			DECLARE @Output					TABLE ([DestinationSchemaName] NVARCHAR(255), [DestinationTableName] NVARCHAR(255), [SourceObjectPrefix] NVARCHAR(255));
	
			/* Generate Cte to select top 1 random row */
			WITH cte_SourceObjectExecutionPlan AS (
				SELECT TOP (1)
					[DestinationSchemaName]	
				,	[DestinationTableName]
				,	[ExecutionStatusCode]
				FROM [dbo].[SourceObjectExecutionPlan] AS so WITH (TABLOCK)
				WHERE 
					(so.[IsEnabled] = 1) ' + CASE WHEN (@environment != 'Dev') THEN 'AND (so.[ScheduleOk] = 1)' ELSE '' END + ' 
				AND (so.[DataSourceType] = ''' + @DataSourceType + ''' OR ''' + @DataSourceType + ''' = '''') 
				AND (so.[ExecutionStatusCode] = 0) 
				ORDER BY (SELECT NEWID())
			)
	
			/* Update top 1 random row from cte_SourceObjectExecutionPlan - lock fetch to prevent DWH from picking the same object twice */
			UPDATE [TARGET] WITH (TABLOCK) SET 
				[TARGET].[ExecutionStatusCode] = 1 
			OUTPUT
				INSERTED.DestinationSchemaName
			,	INSERTED.DestinationTableName	
			INTO @Output 
			FROM cte_SourceObjectExecutionPlan AS [TARGET]
	
			/* When fetched handle output and format - assign to variable (to be able to persist NULL values) */
			SELECT 
				@DestinationSchemaName	= [DestinationSchemaName]	
			,	@DestinationTableName	= [DestinationTableName]						
			FROM @Output
	
			/* Return variables account for NULL handling - update DoWork 1/0 */
			SELECT 
				[DestinationSchemaName]	=	ISNULL(@DestinationSchemaName, '''')	
			,	[DestinationTableName]	=	ISNULL(@DestinationTableName, '''')						
			,	[DoWork]				=	CAST(CASE WHEN (LEN(@DestinationSchemaName) > 0) THEN 1 ELSE 0 END AS BIT)
		';

	END;

	/* Execute Lookup Source Object statement */
	EXEC sys.sp_executesql @stmt;

END;