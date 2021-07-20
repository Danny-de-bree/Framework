CREATE PROCEDURE [meta].[spBuildLoadComplexFromView]

	@DestinationSchemaName NVARCHAR(255)
,	@DestinationTableName NVARCHAR(255)
,	@Emulation TINYINT = 1
AS
BEGIN

	DECLARE @TransformProcedure NVARCHAR(255);
	DECLARE @SourceObjectSchema NVARCHAR(255);
	DECLARE @SourceObjectName NVARCHAR(255);
	DECLARE @DWSchema NVARCHAR(255);
	DECLARE @JobIsReset TINYINT;

	DECLARE @DWTransformDWSchemaName NVARCHAR(255);
	DECLARE @DWTransformSchemaName NVARCHAR(255);

	DECLARE @Message NVARCHAR(MAX);
	DECLARE @stmt NVARCHAR(MAX);
	DECLARE @triggerStmt NVARCHAR(MAX);
	DECLARE @dbTriggerDisabled INT;

	DECLARE @SqlCteStatement NVARCHAR(MAX) = '';
	DECLARE @SqlCteStatementDefinition NVARCHAR(MAX) = '';
	DECLARE @SqlCteStatementName NVARCHAR(MAX) = '';
	DECLARE @SqlCteStatementTempTableName NVARCHAR(MAX) = '';
	DECLARE @SqlCteStatementDropTempTable NVARCHAR(MAX) = '';
	DECLARE @SqlCteStatementCreateTempTable NVARCHAR(MAX) = '';
	DECLARE @SqlMainStatement NVARCHAR(MAX) = '';
	DECLARE @SqlDefinition NVARCHAR(MAX);
	DECLARE @SqlColumns NVARCHAR(MAX) = '';

	DECLARE @IncrementalField NVARCHAR(255) = '';
	DECLARE @IncrementalSourceField NVARCHAR(255) = '';
	DECLARE @IncrementalWhereClause NVARCHAR(MAX) = '';

	/* Check of object exists */
	IF (OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName)) IS NULL
	BEGIN
		SET @Message = 'Object: ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' does not exist';
		PRINT @Message;
		RETURN -1
	END;

	/* Prepare common Data Warehouse parameters */	
	SELECT 
		@DWTransformDWSchemaName	= MAX(CASE WHEN [name] = 'DWTransformDWSchemaName'	THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWTransformSchemaName		= MAX(CASE WHEN [name] = 'DWTransformSchemaName'	THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0)
	GROUP BY ep.major_id

	/* Prepare SourceObject parameters */
	SELECT
		@SourceObjectSchema			= MAX(CASE WHEN [name] = 'SourceObjectSchema'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@SourceObjectName			= MAX(CASE WHEN [name] = 'SourceObjectName'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@JobIsReset					= MAX(CASE WHEN [name] = 'IsReset'					THEN CONVERT(TINYINT,		[value]) ELSE 0 END)
	,	@IncrementalField			= MAX(CASE WHEN [name] = 'IncrementalField'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 1) AND (ep.major_id = OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName))
	GROUP BY ep.major_id

	/**/
	SET @DWSchema = CASE 
						WHEN LEFT(@DestinationTableName, 3) = 'Dim' THEN 'Dim'
						WHEN LEFT(@DestinationTableName, 4) = 'Fact' THEN 'Fact'
						WHEN LEFT(@DestinationTableName, 6) = 'Bridge' THEN 'Bridge'
						ELSE 'Calc'
					END;

	/* Prepare drop procedure if already exists */
	SET @stmt = 'DROP PROCEDURE IF EXISTS ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@SourceObjectSchema + '_' + @DestinationTableName) + ';' ;

	/* Convert SourceObjectSchema to Proper case */
	SET @SourceObjectSchema = UPPER(LEFT(@SourceObjectSchema, 1)) + LOWER(SUBSTRING(@SourceObjectSchema, 2, LEN(@SourceObjectSchema)));

	/* Name of the Stored Procedure */
	SET @TransformProcedure = @SourceObjectSchema + '_' + @DestinationTableName;

	/* Check of object exists */
	IF (OBJECT_ID(@SourceObjectSchema + '.' + @SourceObjectName)) IS NULL
	BEGIN
		SET @Message = 'Object: ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' does not exist';
		PRINT @Message;
		RETURN -2
	END;

	/* Is Source Object a Stored Procedure ? */
	IF (SELECT o.[type] FROM sys.objects AS o WHERE o.object_id = OBJECT_ID(@SourceObjectSchema + '.' + @SourceObjectName)) = 'P'
	BEGIN
		SET @Message = 'Object: ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' is already customized as a Stored Procedure ';
		PRINT @Message;
	/*	
		SET @SourceObjectSchema = UPPER(SUBSTRING(@SourceObjectName, 0, CHARINDEX('_', @SourceObjectName)));
		SET @SourceObjectName = SUBSTRING(@SourceObjectName, LEN(SUBSTRING(@SourceObjectName, 0, CHARINDEX('_', @SourceObjectName) + 2)), LEN(@SourceObjectName));

		EXEC sys.sp_updateextendedproperty 'SourceObjectSchema', @SourceObjectSchema, 'SCHEMA', @DestinationSchemaName, 'TABLE', @DestinationTableName;
		EXEC sys.sp_updateextendedproperty 'SourceObjectName', @SourceObjectName, 'SCHEMA', @DestinationSchemaName, 'TABLE', @DestinationTableName;
	*/
	END;

	/* Create variable to hold columns from TableEntity */
	SELECT @SqlColumns = @SqlColumns + CHAR(9) + CASE WHEN LEN(@SqlColumns) > 0 THEN ',' ELSE '' END + CHAR(9) + QUOTENAME(c.name) + CHAR(10) 
	FROM sys.columns AS c WITH (NOLOCK)
	WHERE (c.object_id = OBJECT_ID(@SourceObjectSchema + '.' + @SourceObjectName))
	ORDER BY c.column_id

	/* Parse TableEntity definition in order to get logical objects */
	BEGIN
		/* Drop #temp table: #ParseEntity if exists */
		DROP TABLE IF EXISTS #ParseEntity;

		SELECT
			[Id]			=	x.[Id]		
		,	[LogicalId]		=	x.[LogicalId]	
		,	[TypeId]		=	x.[TypeId]	
		,	[Type]			=	x.[Type]		
		,	[StartPos]		=	x.[StartPos]	
		,	[EndPos]		=	x.[EndPos]	
		,	[Definition]	=	x.[Definition]
		,	[SelectIndex]	=	x.[SelectIndex]
		,	[FieldIndex]	=	x.[FieldIndex]
		,	[Field]			=	x.[Field]		
		,	[Expression]	=	x.[Expression]
		INTO #ParseEntity
		FROM sys.sql_modules AS m WITH (NOLOCK)
		CROSS APPLY dbo.ufnParseEntity(m.definition) AS x
		WHERE m.object_id = OBJECT_ID(@SourceObjectSchema + '.' + @SourceObjectName)
	END;

	/* Create #temp table: #CteStatement to store SqlCteStatementTempTableName and SqlCteStatementName */
	BEGIN
		DROP TABLE IF EXISTS #CteStatement;
		CREATE TABLE #CteStatement (SqlCteStatementTempTableName NVARCHAR(255), SqlCteStatementName NVARCHAR(255))
	END;

	/* Loop though all CTEs from #ParseEntity and create CTE definitions as WITH #XXX AS ... SELECT * INTO #XXX FROM #XXX */
	BEGIN
		DECLARE Cur_CteStatement CURSOR LOCAL FOR
			SELECT 
				[SqlCteStatementDefinition] = 'WITH ' + [SqlCteName] + ' AS ' + [SqlCteDefinition]
			,	[SqlCteStatementName] = [SqlCteName]
			FROM (
				SELECT 
					[SqlCteLineID] = x.Id
				,	[SqlCteDefinition] = SUBSTRING(x.Definition, CHARINDEX('(', x.Definition), LEN(x.definition) - CHARINDEX(')', REVERSE(x.Definition)))
				,	[SqlCteName] = REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(SUBSTRING(x.Definition, 1 , CHARINDEX('(', x.Definition) - 1), ',',''), 'WITH',''), CHAR(13) + CHAR(10), ' '), CHAR(10), ' '), '  ', ' '), ' AS ', ''), ' ', '')
				FROM #ParseEntity AS x
				WHERE (x.TypeId = -1)
			) AS xt ORDER BY xt.SqlCteLineID
		OPEN Cur_CteStatement
		FETCH NEXT FROM Cur_CteStatement INTO @SqlCteStatementDefinition, @SqlCteStatementName
		WHILE (@@FETCH_STATUS = 0)
		BEGIN
		
			SET @SqlCteStatementTempTableName = '#' + @SqlCteStatementName;
			SET @SqlCteStatementDropTempTable = @SqlCteStatementDropTempTable + 'DROP TABLE IF EXISTS ' + @SqlCteStatementTempTableName + ';' + CHAR(10)
		
			SET @SqlCteStatementCreateTempTable = 
				'SELECT * INTO ' + @SqlCteStatementTempTableName + ' FROM ' + @SqlCteStatementTempTableName + ';' + CHAR(10) + 
				'CREATE CLUSTERED COLUMNSTORE INDEX CCI_' + @SqlCteStatementName + ' ON ' + @SqlCteStatementTempTableName + ';' + CHAR(10)

			SET @SqlCteStatement = @SqlCteStatement + CHAR(10) + 
				'/* Define CTE: ' + @SqlCteStatementName + ' and insert CTE into #Temp table: ' + @SqlCteStatementTempTableName + ' */ ' + CHAR(10) + 
				@SqlCteStatementDefinition + CHAR(10) + 
				@SqlCteStatementCreateTempTable

			INSERT INTO #CteStatement (SqlCteStatementTempTableName, SqlCteStatementName)
			SELECT @SqlCteStatementTempTableName, @SqlCteStatementName

			FETCH NEXT FROM Cur_CteStatement INTO @SqlCteStatementDefinition, @SqlCteStatementName
		END 
		CLOSE Cur_CteStatement;
		DEALLOCATE Cur_CteStatement;
	END;

	/* Only fill rest of the Sql Cte statement variable if the definition <> '' */
	IF (@SqlCteStatement IS NOT NULL) AND (@SqlCteStatement <> '')
	BEGIN
		SET @SqlCteStatement = CHAR(10) +
			  '/* Drop #temp tables if exist */ ' + CHAR(10)
			+ @SqlCteStatementDropTempTable
			+ @SqlCteStatement;
	END;

	/* Find incremental Source field and assign to variable */
	BEGIN
		SET @IncrementalSourceField = (SELECT TOP 1 [Expression] FROM #ParseEntity AS x WHERE x.Field = @IncrementalField AND x.TypeId = 201)
		SET @IncrementalWhereClause = (SELECT [Definition] FROM #ParseEntity AS x WHERE x.Type = 'WHERE')

		IF (@IncrementalField) IS NOT NULL
		BEGIN
			
			SET @IncrementalWhereClause = 
				CASE
					WHEN (SELECT 1 FROM #ParseEntity AS x WHERE x.TypeId = 7) IS NOT NULL THEN 'HAVING (' + @IncrementalSourceField + ' > @LastLoadedValue)'
					WHEN @IncrementalWhereClause IS NOT NULL THEN ' AND (' + @IncrementalSourceField + ' > @LastLoadedValue)'
					WHEN @IncrementalWhereClause IS NULL THEN 'WHERE (' + @IncrementalSourceField + ' > @LastLoadedValue)'
					ELSE ''
				END;

		END;
	END;

	/* Create Main sql statement script with all except CTEs */
	SET @SqlMainStatement = (
		SELECT
			STUFF((
				SELECT '' + REPLACE(RTRIM(LTRIM(x.Definition)), ';', '') AS [text()]
				FROM #ParseEntity AS x
				WHERE x.TypeId between 0 AND 100
				ORDER BY id
				FOR XML PATH(''), type 
			).value('.', 'nvarchar(max)'), 1, 1, '')
	);

	/* Loop though the CTE statement and Main select statement and replace all referenced CTEs with #temp Tables - also effecting nested CTEs referenced in other CTEs */
	BEGIN
		DECLARE Cur_ReplaceCtes CURSOR LOCAL FOR
			SELECT SqlCteStatementTempTableName, SqlCteStatementName
			FROM #CteStatement
		OPEN Cur_ReplaceCtes
		FETCH NEXT FROM Cur_ReplaceCtes INTO @SqlCteStatementTempTableName, @SqlCteStatementName
		WHILE (@@FETCH_STATUS = 0)
		BEGIN
		
			SELECT @SqlCteStatement  = REPLACE(@SqlCteStatement, ' ' + @SqlCteStatementName + ' ', ' ' + @SqlCteStatementTempTableName + ' ')
			SELECT @SqlMainStatement = REPLACE(@SqlMainStatement, ' ' + @SqlCteStatementName + ' ', ' ' + @SqlCteStatementTempTableName + ' ')

			FETCH NEXT FROM Cur_ReplaceCtes INTO @SqlCteStatementTempTableName, @SqlCteStatementName
		END;
		CLOSE Cur_ReplaceCtes;
		DEALLOCATE Cur_ReplaceCtes;
	END;

	/* Remove unnecessary linebreaks from the main select */
	SET @SqlMainStatement = REPLACE(@SqlMainStatement, CHAR(10) + CHAR(13), '');

	/* Create stored procedure statement based on variables */
	SET @SqlDefinition = 
		'CREATE PROCEDURE ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@TransformProcedure) + CHAR(10) + CHAR(10) +
		'	@JobIsReset TINYINT = 0' + CHAR(10) + CHAR(10) +
		'AS' + CHAR(10) +
		'BEGIN' + CHAR(10) + CHAR(10) +

		CASE /* Add Incremental Load - Get last loaded value from EDW table */
			WHEN (@IncrementalField IS NOT NULL) AND (@IncrementalField <> '')
				THEN 
				'	DECLARE @LastLoadedValue DATETIME;' + CHAR(10) + CHAR(10) +
				'	SET @LastLoadedValue = (SELECT MAX(' + QUOTENAME(@IncrementalField) + ') FROM ' + QUOTENAME(@DWSchema) + '.' + QUOTENAME(@DestinationTableName) + ');' + CHAR(10) +
				'	SET @LastLoadedValue = IIF(@JobIsReset = 0, ISNULL(@LastLoadedValue, ''1900-01-01 00:00:00.000''), ''1900-01-01 00:00:00.000'');' + CHAR(10) + CHAR(10)
			ELSE ''
		END +

		'/**********************************************************************************************************************************************************************' + CHAR(10) +
		'	1. Truncate data warehouse staging table ' + CHAR(10) +
		'***********************************************************************************************************************************************************************/' + CHAR(10) + CHAR(10) +
		'	TRUNCATE TABLE ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';' + CHAR(10) + CHAR(10) +

		CASE /* Do we have CTE's specified in the original Select Statement ?*/
			WHEN (@SqlCteStatement IS NOT NULL) AND (@SqlCteStatement <> '')
				THEN
				'/**********************************************************************************************************************************************************************' + CHAR(10) +
				'	2. Hande Common Table Expressions (CTEs) before insert into staging table - You should consider converting heavy CTEs into #temp tables and add indexes' + CHAR(10) +	
				'***********************************************************************************************************************************************************************/' + CHAR(10) +
				REPLACE(@SqlCteStatement, CHAR(10), CHAR(10) + CHAR(9)) + CHAR(10)
			ELSE ''
		END +

		'/**********************************************************************************************************************************************************************' + CHAR(10) +
		'	' + IIF(@SqlCteStatement IS NULL OR @SqlCteStatement = '', '2.', '3.') + ' Business Logik - Remember to use the input variable @JobIsReset to distinguish between full and incremental load' + CHAR(10) +	
		'***********************************************************************************************************************************************************************/' + CHAR(10) + CHAR(10) +
		'	INSERT INTO ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX) (' + CHAR(10) +
				@SqlColumns +
		'	)' + CHAR(10) + CHAR(10) + CHAR(9) + 

			REPLACE(@SqlMainStatement, CHAR(10), CHAR(10) + CHAR(9)) + CHAR(10) +
			
			/* Add Where clause */
			CASE WHEN (@IncrementalWhereClause IS NOT NULL) AND (@IncrementalWhereClause <> '') THEN CHAR(9) + @IncrementalWhereClause ELSE '' END + CHAR(10) + CHAR(13) +
		'END' 

	SELECT
		@DestinationSchemaName AS SourceObjectSchema
	,	@DestinationTableName AS SourceObjectName
	,	@SqlDefinition AS SqlDefinition
	
	IF (@Emulation = 0) 
	BEGIN TRY
		BEGIN TRANSACTION

		/* Execute drop procedure if exists */
		EXEC sys.sp_executesql @stmt; 

		/* Execute create procedure */
		EXEC sys.sp_executesql @SqlDefinition ;

		/* Add new sourceObject Schema */
		EXEC sys.sp_updateextendedproperty 'SourceObjectSchema', @DestinationSchemaName, 'SCHEMA', @DestinationSchemaName, 'TABLE', @DestinationTableName;

		/* Add new sourceObject Name */
		EXEC sys.sp_updateextendedproperty 'SourceObjectName', @TransformProcedure, 'SCHEMA', @DestinationSchemaName, 'TABLE', @DestinationTableName;

		/* Drop existing transform view replaced by the stored procedure */
		SET @stmt = 'DROP VIEW IF EXISTS ' + QUOTENAME(@DWTransformSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';';

		/* Disable view trigger on database */
		IF (dbo.ufnIsTriggerEnabled('ViewTracking') = 1)
		BEGIN
			SET @triggerStmt = 'DISABLE TRIGGER [ViewTracking] ON DATABASE;';
			EXEC sys.sp_executesql @triggerStmt;
			SET @dbTriggerDisabled = 1;
		END;

			EXEC sys.sp_executesql @stmt; 

		/* Enable if view trigger is disable on database */
		IF (@dbTriggerDisabled = 1)
		BEGIN
			SET @triggerStmt = 'ENABLE TRIGGER [ViewTracking] ON DATABASE;';
			EXEC sys.sp_executesql @triggerStmt;
			SET @dbTriggerDisabled = 0;
		END;

		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

		SET @Message = 'Failed to build load complex from view for ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' due to:' + CHAR(10) + ERROR_MESSAGE(); 
		PRINT @Message;
		EXEC dbo.spLog 'DW', 'spBuildLoadComplexFromView', 'Error', 1, @Message, @DestinationTableName;

	END CATCH

END;