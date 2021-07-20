CREATE PROCEDURE [meta].[spTransformLookUpExecutionPlan]
AS
BEGIN

	DECLARE @DestinationSchemaName NVARCHAR(255);
	DECLARE @DestinationTableName NVARCHAR(255);
	DECLARE @LoadSequence INT;
	DECLARE @CountRows INT = 1;
	DECLARE @Output	TABLE ([DestinationSchemaName] NVARCHAR(255), [DestinationTableName] NVARCHAR(255), [LoadSequence] INT);

	/* Run loop as long as there is still objects which has not been loaded */
	WHILE (@DestinationTableName IS NULL) AND (@CountRows > 0)
	BEGIN

		/* Lookup Row count of available entities which has not been loaded */
		SELECT @CountRows = COUNT(DISTINCT [DestinationTableName]) 
		FROM [dbo].[BusinessObjectExecutionPlan] WITH (TABLOCKX) 
		WHERE ([IsEnabled] = 1) AND ([ExecutionStatus] = '');

		/* Generate base cte calculate next available entity */
		WITH cte_BaseCalculation AS (
			SELECT
				[DestinationSchemaName]		=	so.[DestinationSchemaName]	
			,	[DestinationTableName]		=	so.[DestinationTableName]
			,	[ExecutionStatusCode]		=	SUM(so.[ExecutionStatusCode])
			,	[ExecutionStatus]			=	MAX(so.[ExecutionStatus])
			,	[LoadSequence]				=	MAX([LoadSequence])
			FROM [dbo].[BusinessObjectExecutionPlan] AS so WITH (TABLOCKX)
			WHERE (so.[IsEnabled] = 1)
			GROUP BY so.[DestinationSchemaName], so.[DestinationTableName]
		),

		/* Generate Cte to select top 1 random row */
		Cte_FirstAvaliableEntity AS (
			SELECT TOP (1)
				[DestinationSchemaName]
			,	[DestinationTableName]
			,	[ExecutionStatus]
			,	[LoadSequence]
			FROM cte_BaseCalculation WITH (TABLOCKX)
			WHERE (ExecutionStatusCode = 0) AND (ExecutionStatus = '')
			ORDER BY (SELECT NEWID())
		)

		/* Update top 1 random row from cte_SourceObjectExecutionPlan - lock fetch to prevent DWH from picking the same object twice */
		UPDATE [TARGET] WITH (TABLOCKX) SET 
			[TARGET].[ExecutionStatus] = 'Running'
		OUTPUT
			INSERTED.DestinationSchemaName
		,	INSERTED.DestinationTableName	
		,	INSERTED.LoadSequence
		INTO @Output 
		FROM [dbo].[BusinessObjectExecutionPlan] AS [TARGET] WITH (TABLOCKX)
		WHERE EXISTS (
			SELECT 1 FROM Cte_FirstAvaliableEntity AS [SOURCE] 
			WHERE ([TARGET].[DestinationSchemaName] = [SOURCE].[DestinationSchemaName]) 
			AND	([TARGET].[DestinationTableName] = [SOURCE].[DestinationTableName])
		);

		/* When fetched handle output and format - assign to variable (to be able to persist NULL values) */
		SELECT 
			@DestinationSchemaName	= [DestinationSchemaName]	
		,	@DestinationTableName	= [DestinationTableName]		
		,	@LoadSequence			= [LoadSequence]
		FROM @Output;

		/* Delay next lookup to avoid DoS attack */
		IF (@DestinationTableName IS NULL) AND (@CountRows > 0) WAITFOR DELAY '00:00:10.000'; 

	END;

	/* Return variables account for NULL handling - update DoWork 1/0 */
	SELECT 
		[DestinationSchemaName]	=	ISNULL(@DestinationSchemaName, '')	
	,	[DestinationTableName]	=	ISNULL(@DestinationTableName, '')				
	,	[LoadSequence]			=	ISNULL(@LoadSequence, 0)
	,	[CountRows]				=	ISNULL(@CountRows, 0)

END;