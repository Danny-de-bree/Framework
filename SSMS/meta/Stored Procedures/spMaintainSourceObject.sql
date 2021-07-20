CREATE PROCEDURE [meta].[spMaintainSourceObject]

	@SourceSchema NVARCHAR(255)
,	@DestinationSchema NVARCHAR(255)
,	@DestinationTable NVARCHAR(255)
,	@emulation INT = 1 

AS 
BEGIN

	DECLARE @ColumnId INT;
	DECLARE @ColumnName NVARCHAR(255);
	DECLARE @alterColumn NVARCHAR(MAX);
	DECLARE @FromAlterColumn NVARCHAR(MAX);
	DECLARE @ToAlterColumn NVARCHAR(MAX);
	DECLARE @message NVARCHAR(MAX);
	DECLARE @ValidFromDate NVARCHAR(30);

	/* Prepare common Data Warehouse parameters */
	DECLARE @Environment NVARCHAR(255);
	DECLARE @DWBridgePrefix NVARCHAR(255);
	DECLARE @DWDimensionPrefix NVARCHAR(255);
	DECLARE @DWFactPrefix NVARCHAR(255);
	DECLARE @DWBusinessKeySuffix NVARCHAR(255);
	DECLARE @DWSurrogateKeySuffix NVARCHAR(255);
	DECLARE @DWExtractDWSchemaName NVARCHAR(255);
	DECLARE @DWExtractHistorySchemaName NVARCHAR(255);
	DECLARE @DWTransformDWSchemaName NVARCHAR(255);
	DECLARE @DWTransformDMSchemaName NVARCHAR(255);

	/* Prepare SourceObject parameters */
	DECLARE @BaseDimensionName NVARCHAR(255);
	DECLARE @RolePlayingEntity NVARCHAR(255);
	DECLARE @DimensionName NVARCHAR(255);
	DECLARE @RolePlayingDimension NVARCHAR(255);
	DECLARE @PreserveSCD2History TINYINT;	

	DECLARE @SourceObjectColumnName NVARCHAR(255);	
	DECLARE @SourceObjectColumnIsPrimaryKey	TINYINT;
	DECLARE @SourceObjectColumnIsSurrogateKey TINYINT;
	DECLARE @SourceObjectColumnIsBusinessKey TINYINT;
	DECLARE @SourceObjectRelated NVARCHAR(255);
	DECLARE @DestinationColumnName NVARCHAR(255);				

	DECLARE @DataMartColumnName NVARCHAR(MAX) = '';
	DECLARE @DataMartBusinessKeys NVARCHAR(MAX) = '';

	/* Declare variables to load HIST table with values */
	DECLARE @stmt NVARCHAR(MAX) = '';
	DECLARE @stopExecution INT = 0;
	DECLARE @updatesCnt INT;

	DECLARE @RefSchemaName NVARCHAR(255);
	DECLARE @RefTableName NVARCHAR(255);

	DECLARE @triggerStmt NVARCHAR(max);
	DECLARE @dbTriggerDisabled TINYINT;

	SET NOCOUNT ON;
	SET ANSI_WARNINGS ON;

	SET @ValidFromDate	= (SELECT CAST(CAST(GETDATE() AS DATETIME2(3)) AS NVARCHAR));

	/* Prepare common Data Warehouse parameters */	
	SELECT
		@Environment					= MAX(CASE WHEN (ep.[name] = 'Environment')						THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	,	@DWBridgePrefix					= MAX(CASE WHEN (ep.[name] = 'DWBridgePrefix')					THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	,	@DWDimensionPrefix				= MAX(CASE WHEN (ep.[name] = 'DWDimensionPrefix')				THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	,	@DWFactPrefix					= MAX(CASE WHEN (ep.[name] = 'DWFactPrefix')					THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	,	@DWBusinessKeySuffix			= MAX(CASE WHEN (ep.[name] = 'DWBusinessKeySuffix')				THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	,	@DWSurrogateKeySuffix			= MAX(CASE WHEN (ep.[name] = 'DWSurrogateKeySuffix')			THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	,	@DWExtractDWSchemaName			= MAX(CASE WHEN (ep.[name] = 'DWExtractDWSchemaName')			THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	,	@DWExtractHistorySchemaName		= MAX(CASE WHEN (ep.[name] = 'DWExtractHistorySchemaName')		THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	,	@DWTransformDWSchemaName		= MAX(CASE WHEN (ep.[name] = 'DWTransformDWSchemaName')			THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	,	@DWTransformDMSchemaName		= MAX(CASE WHEN (ep.[name] = 'DWTransformDMSchemaName')			THEN CONVERT(NVARCHAR(255), ep.[value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0)
	GROUP BY ep.major_id

	/* Prepare SourceObject parameters */
	SELECT
		@RolePlayingEntity				= MAX(CASE WHEN (ep.[name] = 'RolePlayingEntity')				THEN CONVERT(NVARCHAR(510), ep.[value]) ELSE '' END)
	,	@PreserveSCD2History			= MAX(CASE WHEN (ep.[name] = 'PreserveSCD2History')				THEN CONVERT(TINYINT,		ep.[value]) ELSE 0 END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 1) AND (ep.major_id = OBJECT_ID(@SourceSchema + '.' + @DestinationTable))
	GROUP BY ep.major_id

	/* Verify @SourceSchema and @DestinationTable */
	IF (OBJECT_ID(@SourceSchema + '.' + @DestinationTable)) IS NULL
	BEGIN
		PRINT 'The object ' + @SourceSchema + '.' + @DestinationTable + ' does not exist';
		RETURN -1;
	END

	/* Verify @SourceSchema and @DestinationTable */
	IF (OBJECT_ID(@DestinationSchema + '.' + @DestinationTable)) IS NULL
	BEGIN
		PRINT 'The object ' + @DestinationSchema + '.' + @DestinationTable + ' does not exist';
		RETURN -2;
	END

	/* Prepare field cursor to look though the field definition and update the table accordingly */
	DECLARE cur CURSOR LOCAL FOR 
		SELECT ColumnName = c.name
		FROM sys.objects AS o WITH (NOLOCK)
		INNER JOIN sys.columns AS c WITH (NOLOCK) ON o.object_id = c.object_id
		LEFT JOIN sys.indexes AS i WITH (NOLOCK) ON o.object_id = i.object_id AND i.is_primary_key = 1
		LEFT JOIN sys.index_columns AS ic WITH (NOLOCK) ON i.index_id = ic.index_id AND c.column_id = ic.column_id AND c.object_id = ic.object_id
		WHERE (o.type = 'U') AND (c.default_object_id = 0) AND (c.name != '$SharedName') AND (o.name = @DestinationTable) AND (OBJECT_SCHEMA_NAME(c.object_id) IN (@SourceSchema, @DestinationSchema))
		GROUP BY c.name
		HAVING (MAX(c.is_identity * 1) = 0) AND (MAX(CASE WHEN ic.column_id IS NOT NULL THEN 1 ELSE 0 END) = 0) /* Filter out IDENTITY and primary key columns */
		ORDER BY MAX(c.column_id)
	OPEN cur
	FETCH NEXT FROM cur INTO @ColumnName
	WHILE @@FETCH_STATUS = 0
	BEGIN

		/* Create variable to hold the current column definition */
		SELECT @FromAlterColumn = QUOTENAME(c.name) + ' ' +
			UPPER(tp.name) +
				CASE 
					WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN c.max_length = -1 THEN '4000' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
					WHEN tp.name IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN c.max_length = -1 THEN '4000' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
					WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
					WHEN tp.name IN ('numeric', 'decimal') THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ', ' + CAST(c.scale AS VARCHAR(5)) + ')'
					ELSE ''
				END +
			CASE WHEN (c.is_nullable = 1) THEN ' NULL' ELSE ' NOT NULL' END
		FROM sys.columns AS c WITH (NOLOCK)
		JOIN sys.types AS tp WITH (NOLOCK) ON c.user_type_id = tp.user_type_id
		WHERE c.name = @ColumnName AND c.object_id = OBJECT_ID(@DestinationSchema + '.' + @DestinationTable)
		
		/* Create variable to hold the new column definition */
		SELECT @ToAlterColumn = QUOTENAME(c.name) + ' ' +
			UPPER(tp.name) +
				CASE 
					WHEN tp.name IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN c.max_length = -1 THEN '4000' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
					WHEN tp.name IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN c.max_length = -1 THEN '4000' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
					WHEN tp.name IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
					WHEN tp.name IN ('numeric', 'decimal') THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ', ' + CAST(c.scale AS VARCHAR(5)) + ')'
					ELSE ''
				END +
			' NULL'
		FROM sys.columns AS c WITH (NOLOCK)
		JOIN sys.types AS tp WITH (NOLOCK) ON c.user_type_id = tp.user_type_id
		WHERE c.name = @ColumnName AND c.object_id = OBJECT_ID(@SourceSchema + '.' + @DestinationTable)

		/* Check if Column exists in the DWH table and alter/add the column */
		IF (COL_LENGTH(@DestinationSchema + '.' + @DestinationTable, @ColumnName)) IS NULL
		BEGIN
				
			SET @stmt = @stmt + 'ALTER TABLE ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ' ADD ' + @toAlterColumn + '; ' + CHAR(10)

		END ELSE

		/* Check if table is Data Warehouse layer and column does not exists in source */
		IF (COL_LENGTH(@SourceSchema + '.' + @DestinationTable, @ColumnName)) IS NULL
		BEGIN
			
			SET @stmt = @stmt + 'ALTER TABLE ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ' DROP COLUMN ' + QUOTENAME(@ColumnName) + '; ' + CHAR(10)

		END ELSE 

		/* Check if column definition has changed */
		IF @FromAlterColumn != @ToAlterColumn
		BEGIN	
			
			/*Prepare alter table alter column definition */
			SET @stmt = @stmt + 'ALTER TABLE ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ' DROP COLUMN ' + QUOTENAME(@ColumnName) + '; ' + CHAR(10)
			SET @stmt = @stmt + 'ALTER TABLE ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ' ADD ' + @toAlterColumn + '; ' + CHAR(10)

		END;

		FETCH NEXT FROM cur INTO @ColumnName
	END
	CLOSE cur
	DEALLOCATE cur

	IF (@stmt <> '')
	BEGIN TRY 
		BEGIN TRANSACTION

			IF (@emulation = 1) SELECT @DestinationSchema AS 'DestinationSchema', @DestinationTable AS 'DestinationTable', @stmt AS 'AlterDefinition';
			IF (@emulation = 0)
			BEGIN 
				/* Disable table trigger on database */
				IF (dbo.ufnIsTriggerEnabled('TableTracking') = 1)
				BEGIN
					SET @triggerStmt = 'DISABLE TRIGGER [TableTracking] ON DATABASE;';
					EXEC sys.sp_executesql @triggerStmt;
					SET @dbTriggerDisabled = 1;
				END;			

				EXEC sys.sp_executesql @stmt;
				PRINT @stmt

				SET @message = 'Successfully applied changes to table ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ': ' + CHAR(10) + @stmt ; 
				EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Info', 3, @message, @DestinationTable;

				/* Enable if table trigger is disable on database */
				IF (@dbTriggerDisabled = 1)
				BEGIN
					SET @triggerStmt = 'ENABLE TRIGGER [TableTracking] ON DATABASE;';
					EXEC sys.sp_executesql @triggerStmt;
					SET @dbTriggerDisabled = 0;
				END;

			END;


		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		/* Breake on error - stop execution */
		IF @@TRANCOUNT > 0 
			ROLLBACK TRANSACTION;

		SET @stopExecution = 1;
		SET @message = 'Failed to applied changes to table ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ' due to: ' + ERROR_MESSAGE();
		EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Error', 1, @message, @DestinationTable;
		EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Error', 1, @stmt, @DestinationTable;

	END CATCH;

	/* If schema definition has changed we then need to update all modules which dependes on the table */
	IF (@stopExecution = 0) AND (@stmt <> '')
	BEGIN
		
		DECLARE curRef CURSOR LOCAL FOR 
			SELECT s.name, o.name
			FROM sys.sql_expression_dependencies AS sed WITH (NOLOCK)
			JOIN sys.objects AS o WITH (NOLOCK) ON sed.referencing_id = o.object_id
			JOIN sys.schemas AS s WITH (NOLOCK) ON o.schema_id = s.schema_id
			WHERE sed.referenced_id = OBJECT_ID(@DestinationSchema + '.' + @DestinationTable) AND (s.name != @DWTransformDMSchemaName) AND (o.name != @DestinationTable)
		OPEN curRef
		FETCH NEXT FROM curRef INTO @refSchemaName, @refTableName
		WHILE (@@FETCH_STATUS = 0)
		BEGIN		
			BEGIN TRY
			
				SET @stmt = 'EXEC sys.sp_refreshsqlmodule ''' + @refSchemaName + '.' + @refTableName + '''; ';

				EXEC sys.sp_executesql @stmt;
				PRINT @stmt;

				SET @message = 'Refresh sql_module: ' + QUOTENAME(@refSchemaName) + '.' + QUOTENAME(@refTableName) + ' referenced by ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable)
				PRINT @message;
				EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Info', 3, @message, @DestinationTable;

			END TRY
			BEGIN CATCH				
				SET @message = 'Failed to refresh sql_module: ' + QUOTENAME(@refSchemaName) + '.' + QUOTENAME(@refTableName) + ' referenced by ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ' due to: ' + ERROR_MESSAGE();
				EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Error', 1, @message, @DestinationTable;
				EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Error', 1, @stmt, @DestinationTable;
			END CATCH
			
			FETCH NEXT FROM curRef INTO @refSchemaName, @refTableName
		END
		CLOSE curRef
		DEALLOCATE curRef

	END;

	/* Update Dimension Data Mart views */
	BEGIN TRY
		IF (@DestinationSchema IN (@DWDimensionPrefix, @DWBridgePrefix, @DWFactPrefix))
		BEGIN
			
			/* Prepare cursor to loop though source fields and data mart fields */
			DECLARE DataMartCur CURSOR LOCAL FOR 
				SELECT 
					[SourceObjectColumnName]			=	c.name									
				,	[DestinationColumnName]				=	CASE WHEN c.is_identity = 1 THEN IIF(LEFT(@DestinationTable, LEN(@DWDimensionPrefix)) = @DWDimensionPrefix, '', @DWDimensionPrefix) + fs.FieldName + @DWSurrogateKeySuffix ELSE c.name END
				,	[ColumnIsBusinessKey]				=	CASE WHEN c.name LIKE (@DWDimensionPrefix + '%' + @DWBusinessKeySuffix) THEN 1 ELSE 0 END
				FROM sys.columns AS c WITH (NOLOCK)
				INNER JOIN sys.columns AS cx WITH (NOLOCK) ON (cx.object_id = OBJECT_ID(@SourceSchema + '.' + @DestinationTable)) AND (c.name = cx.name)
				LEFT JOIN sys.indexes AS i WITH (NOLOCK) ON c.object_id = i.object_id AND i.is_primary_key = 1
				LEFT JOIN sys.index_columns AS ic WITH (NOLOCK) ON i.object_id = ic.object_id AND i.index_id = ic.index_id AND c.column_id = ic.column_id
				CROSS APPLY (SELECT FieldName FROM dbo.fnSingularize(OBJECT_NAME(c.object_id))) AS fs
				WHERE (c.name NOT LIKE ('DW%')) AND (c.object_id = object_id(@DestinationSchema + '.' + @DestinationTable))
				ORDER BY cx.column_id
			OPEN DataMartCur
			FETCH NEXT FROM DataMartCur INTO @SourceObjectColumnName, @DestinationColumnName, @SourceObjectColumnIsBusinessKey
			WHILE (@@FETCH_STATUS = 0)
			BEGIN
				
				IF (@DataMartColumnName != '')
				BEGIN
					SET @DataMartColumnName = @DataMartColumnName + ',';
				END;

				SET @DataMartColumnName = @DataMartColumnName + CHAR(9) + QUOTENAME(@DestinationColumnName) + ' = ' + QUOTENAME(@DestinationTable) + '.' + QUOTENAME(@SourceObjectColumnName) + CHAR(10);

				FETCH NEXT FROM DataMartCur INTO @SourceObjectColumnName, @DestinationColumnName, @SourceObjectColumnIsBusinessKey
			END
			CLOSE DataMartCur
			DEALLOCATE DataMartCur

			SET @message = CASE WHEN (OBJECT_ID(@DWTransformDMSchemaName + '.' + @DestinationTable) IS NOT NULL) THEN 'Updating' ELSE 'Creating' END + ' Data Mart view ' + QUOTENAME(@DWTransformDMSchemaName) + '.' + QUOTENAME(@DestinationTable) + ' based on entity ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable);
			PRINT CHAR(9) + CHAR(9) + @message; 

			/* Update main dimension */
			SET @stmt =
				CASE WHEN (OBJECT_ID(@DWTransformDMSchemaName + '.' + @DestinationTable) IS NOT NULL) THEN 'ALTER ' ELSE 'CREATE ' END + 
				'VIEW ' + QUOTENAME(@DWTransformDMSchemaName) + '.' + QUOTENAME(@DestinationTable) + CHAR(10) + 
				'AS' + CHAR(10) +
				'SELECT' + CHAR(10) + @DataMartColumnName +
				'FROM ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ' AS ' + QUOTENAME(@DestinationTable) +
				CASE 
					WHEN (@DestinationSchema = @DWFactPrefix) THEN CHAR(10) + 'WHERE (' + QUOTENAME(@DestinationTable) + '.[DWIsDeleted] = 0)' 
					WHEN (@DestinationSchema = @DWDimensionPrefix) THEN CHAR(10) + 'WHERE (' + QUOTENAME(@DestinationTable) + '.[DWIsCurrent] = 1)'
					WHEN (@DestinationSchema = @DWBridgePrefix) THEN CHAR(10) + 'WHERE (' + QUOTENAME(@DestinationTable) + '.[DWIsCurrent] = 1)'
					ELSE '' 
				END
			;

			IF (@emulation = 1) SELECT @DestinationSchema AS 'DestinationSchema', @DestinationTable AS 'DestinationTable', @stmt AS 'AlterDefinition';
			IF (@emulation = 0) 
			BEGIN

				/* Disable view trigger on database */
				IF (dbo.ufnIsTriggerEnabled('ViewTracking') = 1)
				BEGIN
					SET @triggerStmt = 'DISABLE TRIGGER [ViewTracking] ON DATABASE;';
					EXEC sys.sp_executesql @triggerStmt;
					SET @dbTriggerDisabled = 1;
				END;

				EXEC sys.sp_executesql @stmt;
				EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Info', 3, @message, @DestinationTable;

				/* Enable if view trigger is disable on database */
				IF (@dbTriggerDisabled = 1)
				BEGIN
					SET @triggerStmt = 'ENABLE TRIGGER [ViewTracking] ON DATABASE;';
					EXEC sys.sp_executesql @triggerStmt;
					SET @dbTriggerDisabled = 0;
				END;

			END;
		END;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 
			ROLLBACK TRANSACTION;

		SET @message = 'Updating Data Mart view ' + QUOTENAME(@DWTransformDMSchemaName) + '.' + QUOTENAME(@DestinationTable) + ' based on entity ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ' due to ' + ERROR_MESSAGE();
		EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Error', 1, @message, @DestinationTable;
		EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Error', 1, @stmt, @DestinationTable;
	END CATCH

	/* Maintain Data Mart views for Role-playing dimensions if exists */
	IF (@DestinationSchema = @DWDimensionPrefix) AND (@RolePlayingEntity != '')
	BEGIN
		
		/* Replace spacing and remove '[' and ']' from @RolePlayingEntity */
		SET @RolePlayingEntity = REPLACE(REPLACE(REPLACE(@RolePlayingEntity, ' ', ''), '[',''), ']', '');

		/* Loop though all role-playing dimensions based on imput table to create Data Mart view script from SourceColumns and DestinationColumns */
		DECLARE RolePlayingDimensionCur CURSOR LOCAL FOR 
		SELECT
			[BaseDimensionName]		=	(SELECT FieldName FROM dbo.fnSingularize(REPLACE(@DestinationTable, @DWDimensionPrefix, '')))
		,	[RolePlayingDimension]	=	(SELECT FieldName FROM dbo.fnSingularize(REPLACE(rpd.part, @DWDimensionPrefix, '')))
		,	[DimensionName]			=	rpd.part
		FROM dbo.fnSplit(',', @RolePlayingEntity) AS rpd
		OPEN RolePlayingDimensionCur
		FETCH NEXT FROM RolePlayingDimensionCur INTO @BaseDimensionName, @RolePlayingDimension, @DimensionName 
		WHILE (@@FETCH_STATUS = 0)
		BEGIN
			
			SET @message = CASE WHEN (OBJECT_ID(@DWTransformDMSchemaName + '.' + @DimensionName) IS NOT NULL) THEN 'Updating' ELSE 'Creating' END + ' Data Mart view ' + QUOTENAME(@DWTransformDMSchemaName) + '.' + QUOTENAME(@DimensionName) + ' based on entity ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable);
			PRINT CHAR(9) + CHAR(9) + @message; 

			/* Drop #tempTable #RolePlayingDimension if exists */
			DROP TABLE IF EXISTS #RolePlayingDimension;

			SELECT
				[SourceObjectColumnName]		=	c.name
			,	[SourceObjectOrdinalPosition]	=	c.column_id
			,	[DestinationColumnName]			=	CASE
														WHEN c.is_identity = 1 THEN @DWDimensionPrefix + @RolePlayingDimension + @DWSurrogateKeySuffix
														WHEN c.name LIKE (@DWDimensionPrefix + '%' + @DWBusinessKeySuffix) THEN REPLACE(c.name, o.name, @DimensionName)
														ELSE REPLACE(c.name, @BaseDimensionName, @RolePlayingDimension)
													END
			INTO #RolePlayingDimension
			FROM sys.objects AS o WITH (NOLOCK)
			JOIN sys.columns AS c WITH (NOLOCK) ON o.object_id = c.object_id
			WHERE (c.name NOT LIKE ('DW%')) AND o.object_id = OBJECT_ID(@SourceSchema + '.' + @DestinationTable)
			ORDER BY c.column_id

			/* Update main dimension */
			SET @stmt =
				CASE WHEN (OBJECT_ID(@DWTransformDMSchemaName + '.' + @DimensionName) IS NOT NULL) THEN 'ALTER ' ELSE 'CREATE ' END + 
				'VIEW ' + QUOTENAME(@DWTransformDMSchemaName) + '.' + QUOTENAME(@DimensionName) + CHAR(10) + 
				'AS' + CHAR(10) +
				'SELECT' + CHAR(10) + 
					STUFF((
						SELECT ',' + CHAR(9) + QUOTENAME(DestinationColumnName) +  ' = ' + QUOTENAME(@DestinationTable) + '.' + QUOTENAME(SourceObjectColumnName) + CHAR(10) AS [text()]
						FROM #RolePlayingDimension
						ORDER BY SourceObjectOrdinalPosition
						FOR XML PATH ('')
					), 1, 1, '') + 
				'FROM ' + QUOTENAME(@DestinationSchema) + '.' + QUOTENAME(@DestinationTable) + ' AS ' + QUOTENAME(@DestinationTable) +
				CASE 
					WHEN (@DestinationSchema = @DWDimensionPrefix) THEN CHAR(10) + 'WHERE (' + QUOTENAME(@DestinationTable) + '.[DWIsCurrent] = 1)' 
					ELSE '' 
				END
			;

			IF (@emulation = 1) SELECT @DestinationSchema AS 'DestinationSchema', @DestinationTable AS 'DestinationTable', @stmt AS 'AlterDefinition';
			IF (@emulation = 0) 
			BEGIN

				/* Disable view trigger on database */
				IF (dbo.ufnIsTriggerEnabled('ViewTracking') = 1)
				BEGIN
					SET @triggerStmt = 'DISABLE TRIGGER [ViewTracking] ON DATABASE;';
					EXEC sys.sp_executesql @triggerStmt;
					SET @dbTriggerDisabled = 1;
				END;

				EXEC sys.sp_executesql @stmt;
				EXEC dbo.spLog 'DW', 'spMaintainSourceObject', 'Info', 3, @message, @DestinationTable;

				/* Enable if view trigger is disable on database */
				IF (@dbTriggerDisabled = 1)
				BEGIN
					SET @triggerStmt = 'ENABLE TRIGGER [ViewTracking] ON DATABASE;';
					EXEC sys.sp_executesql @triggerStmt;
					SET @dbTriggerDisabled = 0;
				END;

			END;

			FETCH NEXT FROM RolePlayingDimensionCur INTO @BaseDimensionName, @RolePlayingDimension, @DimensionName
		END
		CLOSE RolePlayingDimensionCur
		DEALLOCATE RolePlayingDimensionCur

	END

END