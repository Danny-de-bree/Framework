CREATE PROCEDURE [meta].[spLoadSourceObjectDefinition]

	@SourceConnectionSchema NVARCHAR(255)
,	@SourceObjectTable NVARCHAR(255)
AS
BEGIN
	DECLARE @CRLF VARCHAR(2) = CHAR(13) + CHAR(10);
	DECLARE @DWCollation NVARCHAR(255);
	DECLARE @PreCopyStatement NVARCHAR(MAX);
	DECLARE @DataSourceId NVARCHAR(255);
	DECLARE @SourceObjectId NVARCHAR(255);
	DECLARE @SourceSchema NVARCHAR(255);
	DECLARE @SourceTable NVARCHAR(255);
	DECLARE @SourceObjectDefinition NVARCHAR(MAX);
	DECLARE @SourceDatabaseName NVARCHAR(255);
	DECLARE @SourceObjectChangeTracking NVARCHAR(MAX);

	/* Retrive schema, object and source database from Data Warehouse configuration table */
	SELECT
		@DataSourceId				=	sc.SourceConnectionID
	,	@SourceObjectId				=	so.SourceObjectID
	,	@SourceSchema				=	so.SourceSchema
	,	@SourceTable				=	so.SourceTable
	,	@SourceDatabaseName			=	sc.DataSourceDatabaseName
	FROM meta.SourceObject AS so WITH (NOLOCK)
	JOIN meta.SourceConnection AS sc WITH (NOLOCK) ON (so.SourceConnectionID = sc.SourceConnectionID)
	WHERE (sc.SourceConnectionSchema = @SourceConnectionSchema) AND (so.SourceObjectTable = @SourceObjectTable)
	
	/* Get Current Database default collation to avoid collation conflicts on source */
	SET @DWCollation = CAST(DATABASEPROPERTYEX(DB_NAME(), 'COLLATION') AS NVARCHAR(255));

	/* Prepare pre-copy statement */
	SET @PreCopyStatement = 'DELETE dbo.SourceObjectDefinition WHERE ([SourceObjectId] = ' + @SourceObjectId + ')';

	/* Prepare script to retrive schema definition from source */
	SET @SourceObjectDefinition = 
	'USE ' + QUOTENAME(@SourceDatabaseName) + ';' + @CRLF +
	''+ @CRLF +
	'DECLARE @stmt NVARCHAR(MAX);' + @CRLF +
	'DECLARE @SourceObjectColumnID BIGINT;' + @CRLF +
	'DECLARE @SourceObjectColumnName NVARCHAR(255);' + @CRLF +
	'DECLARE @KeyCardinalityCount BIGINT;' + @CRLF + @CRLF +

	'IF(OBJECT_ID(''tempdb..#TempTable'') IS NOT NULL) DROP TABLE #TempTable;' + @CRLF + @CRLF +

	'SELECT ' + @CRLF +
	'	[SourceObjectID]					=	' + @SourceObjectId + ' ' + @CRLF +
	',	[SourceConnectionID]				=	' + @DataSourceId + ' ' + @CRLF +
	',	[SourceObjectColumnID]				=	c.column_id ' + @CRLF +
	',	[SourceObjectColumnName]			=	c.name COLLATE ' + @DWCollation + ' ' + @CRLF +
	',	[SourceObjectColumnType]			=	CASE ' + @CRLF +
	'												WHEN t1.name COLLATE ' + @DWCollation + ' IN (''ntext'', ''text'', ''nchar'', ''char'', ''varchar'') THEN ''NVARCHAR'' ' + @CRLF +
	'												WHEN t1.name COLLATE ' + @DWCollation + ' IN (''TIMESTAMP'') THEN ''BIGINT'' ' + @CRLF +
	'												WHEN t1.name COLLATE ' + @DWCollation + ' IN (''bit'') THEN ''TINYINT'' ' + @CRLF +
	'												WHEN t1.name COLLATE ' + @DWCollation + ' IN (''money'', ''float'', ''number'') THEN ''DECIMAL'' ' + @CRLF +
	'												ELSE UPPER(t1.name COLLATE ' + @DWCollation + ') ' + @CRLF +
	'											END ' + @CRLF +
	',	[SourceObjectColumnLength]			=	CASE ' + @CRLF +
	'												WHEN t1.name COLLATE ' + @DWCollation + ' IN (''varchar'', ''char'', ''varbinary'', ''binary'', ''text'') THEN ''('' + CASE WHEN c.max_length = -1 THEN ''4000'' ELSE CAST(c.max_length AS VARCHAR(5)) END + '')'' ' + @CRLF +
	'												WHEN t1.name COLLATE ' + @DWCollation + ' IN (''nvarchar'', ''nchar'', ''ntext'') THEN ''('' + CASE WHEN c.max_length = -1 THEN ''4000'' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + '')'' ' + @CRLF +
	'												WHEN t1.name COLLATE ' + @DWCollation + ' IN (''datetime2'', ''time2'', ''datetimeoffset'') THEN ''('' + CAST(c.scale AS VARCHAR(5)) + '')'' ' + @CRLF +
	'												WHEN t1.name COLLATE ' + @DWCollation + ' IN (''numeric'', ''decimal'', ''money'', ''float'', ''number'') THEN ''(28, 12)'' ' + @CRLF +
	'												ELSE CAST('''' COLLATE ' + @DWCollation + ' AS NVARCHAR(128)) ' + @CRLF +
	'											END ' + @CRLF +
	',	[SourceObjectColumnIsNullable]		=	CASE WHEN ic.column_id IS NOT NULL THEN ''NOT NULL'' ELSE ''NULL'' END ' + @CRLF +
	',	[SourceObjectColumnIsPrimaryKey]	=	CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END ' + @CRLF +
	',	[SourceObjectPrimaryKeyNumber]		=	CASE WHEN ic.column_id IS NOT NULL THEN ic.key_ordinal ELSE 0 END ' + @CRLF +
	'INTO #TempTable ' + @CRLF +
	'FROM [sys].[columns] AS c ' + @CRLF +
	'INNER JOIN [sys].[types] AS t ON (c.user_type_id = t.user_type_id) ' + @CRLF +
	'LEFT JOIN [sys].[types] AS t1 ON (t.system_type_id = t1.user_type_id) ' + @CRLF +
	'LEFT JOIN [sys].[indexes] AS i ON (c.object_id = i.object_id) AND (i.is_primary_key = 1) ' + @CRLF +
	'LEFT JOIN [sys].[index_columns] AS ic ON (i.index_id = ic.index_id) AND (c.column_id = ic.column_id) AND (ic.object_id = c.object_id) ' + @CRLF +
	'WHERE (c.object_id = OBJECT_ID(''' + @SourceSchema + '.' + @SourceTable + ''')) ' + @CRLF +  
	';' + @CRLF + @CRLF +
	'DECLARE cur CURSOR LOCAL FOR ' + @CRLF +
	'	SELECT tt.SourceObjectColumnID, tt.SourceObjectColumnName ' + @CRLF +
	'	FROM #TempTable AS tt ' + @CRLF +
	'	WHERE (tt.SourceObjectColumnIsPrimaryKey = 1) ' + @CRLF +
	'OPEN cur ' + @CRLF +
	'FETCH NEXT FROM cur INTO @SourceObjectColumnID, @SourceObjectColumnName ' + @CRLF +
	'WHILE (@@FETCH_STATUS = 0) ' + @CRLF +
	'BEGIN ' + @CRLF +
	' ' + @CRLF +
	'	SET @stmt = ''SELECT @KeyCardinalityCount = COUNT(DISTINCT '' + QUOTENAME(@SourceObjectColumnName) + '') FROM ' + QUOTENAME(@SourceSchema) + '.' + QUOTENAME(@SourceTable) + ' '' ' + @CRLF +
	'	EXEC sys.sp_executesql @stmt, N''@KeyCardinalityCount BIGINT OUTPUT'', @KeyCardinalityCount OUTPUT; ' + @CRLF +
	' ' + @CRLF +
	'	UPDATE tt SET ' + @CRLF +
	'		SourceObjectPrimaryKeyNumber = @KeyCardinalityCount ' + @CRLF +
	'	FROM #TempTable AS tt ' + @CRLF +
	'	WHERE (tt.SourceObjectColumnID = @SourceObjectColumnID) ' + @CRLF +
	' ' + @CRLF +
	'	FETCH NEXT FROM cur INTO @SourceObjectColumnID, @SourceObjectColumnName ' + @CRLF +
	'END ' + @CRLF +
	'CLOSE cur ' + @CRLF +
	'DEALLOCATE cur; ' + @CRLF +
	' ' + @CRLF +
	'SELECT ' + @CRLF +
	'	[SourceObjectID] ' + @CRLF +					
	',	[SourceConnectionID] ' + @CRLF +				
	',	[SourceObjectColumnID] ' + @CRLF +				
	',	[SourceObjectColumnName] ' + @CRLF +			
	',	[SourceObjectColumnType] ' + @CRLF +												
	',	[SourceObjectColumnLength] ' + @CRLF +											
	',	[SourceObjectColumnIsNullable] ' + @CRLF +		
	',	[SourceObjectColumnIsPrimaryKey] ' + @CRLF +	
	',	[SourceObjectPrimaryKeyNumber] = CASE WHEN [SourceObjectColumnIsPrimaryKey] = 1 THEN ROW_NUMBER() OVER(ORDER BY [SourceObjectPrimaryKeyNumber] desc) ELSE 0 END ' + @CRLF +							
	'FROM #TempTable ' + @CRLF +
	'ORDER BY [SourceObjectColumnID] ' + @CRLF
	;

	/* Prepare script to check if it is possible to enable change tracking on the source */
	SET @SourceObjectChangeTracking = '
		USE ' + QUOTENAME(@SourceDatabaseName) + ';

		DECLARE @msg NVARCHAR(255); 
		DECLARE @LastChangeTrackingVersion BIGINT; 
		DECLARE @TableStatus INT = -1; 
		DECLARE @stmt NVARCHAR(MAX); 
		DECLARE @StopExecution TINYINT = 0;

		IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases AS ct WHERE ct.database_id = DB_ID())
		BEGIN
			BEGIN TRY
				ALTER DATABASE ' + QUOTENAME(@SourceDatabaseName) + ' SET CHANGE_TRACKING = ON (CHANGE_RETENTION = 14 DAYS, AUTO_CLEANUP = ON);
			END TRY
			BEGIN CATCH
				SET @msg = ''Failed to enable Change Tracking on database: ' + QUOTENAME(@SourceDatabaseName) + ' due to: '' + ERROR_MESSAGE();
				SET @StopExecution = 1;
			END CATCH
		END;

		IF (@StopExecution = 0)
		BEGIN
			SELECT @TableStatus	= 
				CASE 
					WHEN (o.type <> ''U'') OR (i.is_primary_key IS NULL) THEN -1 
					WHEN (ctt.object_id IS NOT NULL) THEN 0 
					ELSE 1 
				END 
			FROM [sys].[objects] AS o  
			INNER JOIN [sys].[schemas] AS s ON (o.schema_id = s.schema_id) 
			LEFT JOIN [sys].[indexes] AS i ON (o.object_id = i.object_id) AND (i.is_primary_key = 1) 
			LEFT JOIN [sys].[change_tracking_tables] AS ctt ON (o.object_id = ctt.object_id) 
			WHERE (o.object_id = OBJECT_ID(''' + @SourceSchema + '.' + @SourceTable + ''')) 

			SET @TableStatus = ISNULL(@TableStatus, -1); 

			IF (@TableStatus = 1) 
			BEGIN 
				BEGIN TRY 
					SET @stmt = ''ALTER TABLE ' + QUOTENAME(@SourceDatabaseName) + '.' + QUOTENAME(@SourceSchema) + '.' + QUOTENAME(@SourceTable) + ' ENABLE CHANGE_TRACKING;'';
					SET @msg = ''Enabling Change tracking on table: ' + QUOTENAME(@SourceDatabaseName) + '.' + QUOTENAME(@SourceSchema) + '.' + QUOTENAME(@SourceTable) + ''';

					EXEC [sys].[sp_executesql] @stmt ; 

				END TRY 
				BEGIN CATCH
					SET @msg = ''Failed to enable Change Tracking on table: ' + QUOTENAME(@SourceDatabaseName) + '.' + QUOTENAME(@SourceSchema) + '.' + QUOTENAME(@SourceTable) + ' due to: '' + ERROR_MESSAGE();
					SET @TableStatus = -1 
				END CATCH 
			END; 

			IF (@TableStatus = 0)
			BEGIN
				SET @msg = ''Change tracking is already enabled on table: ' + QUOTENAME(@SourceDatabaseName) + '.' + QUOTENAME(@SourceSchema) + '.' + QUOTENAME(@SourceTable) + ''';
				SET @TableStatus = 1;
			END;

			IF (@TableStatus = -1)
			BEGIN
				SET @msg = ''Change tracking is not supported for: ' + QUOTENAME(@SourceDatabaseName) + '.' + QUOTENAME(@SourceSchema) + '.' + QUOTENAME(@SourceTable) + ''';
				SET @TableStatus = 0;
			END;
		END;

		SELECT 
			[IsChangeTracking]	=	@TableStatus
		,	[Message]			=	@msg
	' ;

	/* Prepare source object definition script to be executed remote */
	BEGIN
		SELECT 
			[SourceObjectDefinition]		=	@SourceObjectDefinition
		,	[SourceObjectSchema]			=	@SourceSchema
		,	[SourceObjectName]				=	@SourceTable
		,	[PreCopyStatement]				=	@PreCopyStatement
		,	[SourceObjectChangeTracking]	=	@SourceObjectChangeTracking
	END;



END