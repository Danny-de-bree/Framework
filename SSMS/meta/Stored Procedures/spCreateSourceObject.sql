CREATE PROCEDURE [meta].[spCreateSourceObject]

	@SourceObjectSchema NVARCHAR(255)
,	@SourceObjectTable NVARCHAR(255)
,	@emulation TINYINT = 1

AS 
BEGIN
	SET NOCOUNT ON;
	DECLARE @SourceConnectionID BIGINT;
	DECLARE @SourceObjectID BIGINT;
	DECLARE @DWSourceObjectTable NVARCHAR(255)
	DECLARE @DWSourceObjectSchema NVARCHAR(255)
	DECLARE @SourceObjectColumnName NVARCHAR(255);
	DECLARE @SourceObjectColumnType	NVARCHAR(255);
	DECLARE @SourceObjectColumnLength NVARCHAR(255);
	DECLARE @SourceObjectColumnIsNullable NVARCHAR(255);
	DECLARE @SourceObjectColumnIsPrimaryKey TINYINT;

	DECLARE @PreserveSCD2History TINYINT;
	DECLARE @DWExtractDWSchemaName NVARCHAR(255);
	DECLARE @DWExtractStagingSchemaName NVARCHAR(255);
	DECLARE @DWExtractHistorySchemaName NVARCHAR(255);
	
	DECLARE @DWDestinationSchemaName NVARCHAR(255);
	DECLARE @Message NVARCHAR(MAX);
	DECLARE @PackageName NVARCHAR(255);
	DECLARE @triggerStmt NVARCHAR(MAX);
	DECLARE @dbTriggerDisabled TINYINT;

	DECLARE @SourceObjectDefinition NVARCHAR(MAX) = '';
	DECLARE @SourceObjectColumns NVARCHAR(MAX) = '';
	DECLARE @SourceObjectPrimaryKey NVARCHAR(MAX) = '';
	DECLARE @PrimaryKey NVARCHAR(MAX) = '';
	DECLARE @stmt NVARCHAR(MAX);
	
	SET @PackageName					=	OBJECT_NAME(@@PROCID);
	
	/* Prepare common Data Warehouse parameters */	
	SELECT
		@DWExtractDWSchemaName			=	MAX(CASE WHEN (ep.[name] = 'DWExtractDWSchemaName')			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWExtractStagingSchemaName		=	MAX(CASE WHEN (ep.[name] = 'DWExtractStagingSchemaName')	THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWExtractHistorySchemaName		=	MAX(CASE WHEN (ep.[name] = 'DWExtractHistorySchemaName')	THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0)
	GROUP BY (ep.major_id)

	/* Should we preserve SCD2 history in extract layer - lookup in meta.SourceObject */
	BEGIN	
		SELECT 
			@PreserveSCD2History	=	CASE WHEN @DWExtractHistorySchemaName != '' THEN so.PreserveSCD2History ELSE 0 END
		,	@SourceObjectSchema		=	sc.SourceConnectionSchema
		,	@DWSourceObjectSchema	=	sc.SourceConnectionSchema
		,	@SourceObjectTable		=	so.SourceObjectTable
		,	@DWSourceObjectTable	=	so.SourceObjectTable
		,	@SourceObjectID			=	so.SourceObjectID
		,	@SourceConnectionID		=	COALESCE(sp.SourcePartitionCode, so.SourceConnectionID)
		FROM [meta].[SourceObject] AS so WITH (NOLOCK)
		INNER JOIN [meta].[SourceConnection] AS sc WITH (NOLOCK) ON so.SourceConnectionID = sc.SourceConnectionID
		LEFT JOIN [meta].[SourcePartition] AS sp WITH (NOLOCK) ON sc.SourceConnectionID = sp.SourceConnectionID 
		WHERE (sc.SourceConnectionSchema = @SourceObjectSchema) AND (so.SourceObjectTable = @SourceObjectTable)
	END;

	/* For each schema in the EXTRACT layer generate create table script */
	DECLARE SchemaCur CURSOR LOCAL FOR
		SELECT s.SchemaName FROM (VALUES (@DWExtractStagingSchemaName), (@DWExtractDWSchemaName)) AS s (SchemaName) UNION
		SELECT s.SchemaName FROM (VALUES (@DWExtractHistorySchemaName)) AS s (SchemaName) WHERE (@PreserveSCD2History = 1)
	OPEN SchemaCur
	FETCH NEXT FROM SchemaCur INTO @DWDestinationSchemaName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		/* TODO: Add Description */
		SET @SourceObjectSchema	=	CASE 
										WHEN (@DWDestinationSchemaName IN (@DWExtractDWSchemaName, @DWExtractHistorySchemaName)) THEN @DWSourceObjectSchema + '_' + @DWDestinationSchemaName  
										ELSE @DWSourceObjectSchema
									END;

		/* Check if schema exists in Data Warehouse if not Create Schema */
		IF (SCHEMA_ID(@SourceObjectSchema) IS NULL) 
		BEGIN
			SELECT @stmt =
				'EXEC (''CREATE SCHEMA ' + QUOTENAME(@SourceObjectSchema) + ' AUTHORIZATION [meta];'');' + CHAR(13) + CHAR(10) + CHAR(10) +
				
					/* Only add extended properties to source connection schema */
					CASE WHEN (@DWDestinationSchemaName NOT IN (@DWExtractDWSchemaName, @DWExtractHistorySchemaName))
						THEN 'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + CAST(s.SourceConnectionSchema AS NVARCHAR(255)) + ''', @name = N''DataWarehouseLayer'', @value = N''Source''; ' + CHAR(10) +
							 'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + CAST(s.SourceConnectionSchema AS NVARCHAR(255)) + ''', @name = N''DataSourceName'', @value = N''' + s.datasourceName + '''; ' + CHAR(10) +
							 'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + CAST(s.SourceConnectionSchema AS NVARCHAR(255)) + ''', @name = N''DataSourceServerName'', @value = N''' + s.DataSourceServerName + '''; ' + CHAR(10) +
							 'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + CAST(s.SourceConnectionSchema AS NVARCHAR(255)) + ''', @name = N''DataSourceDatabaseName'', @value = N''' + s.DataSourceDatabaseName + '''; ' + CHAR(10) +
							 'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + CAST(s.SourceConnectionSchema AS NVARCHAR(255)) + ''', @name = N''DataSourceType'', @value = N''' + s.DataSourceType + '''; ' + CHAR(10)
						ELSE ''
					END
			FROM [meta].[SourceConnection] AS s
			WHERE (s.SourceConnectionSchema = @DWSourceObjectSchema)

			PRINT @stmt;
			IF (@emulation = 0) EXEC sys.sp_executesql @stmt;
		END;

		/* Clear variables for each generate create table script */
		SET @SourceObjectDefinition = '';
		SET @SourceObjectColumns = '';
		SET @SourceObjectPrimaryKey = '';
		SET @PrimaryKey = '';

		IF (@DWDestinationSchemaName NOT IN (@DWExtractDWSchemaName, @DWExtractHistorySchemaName))
		BEGIN
			SET @SourceObjectDefinition = 		
						CHAR(9) + '[DWOperation] [NVARCHAR](15) NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWOperation] DEFAULT (''I'') ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWTrackingVersion] [BIGINT] NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWTrackingVersion] DEFAULT (0) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWSourceConnectionID] [BIGINT] NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWSourceConnectionID] DEFAULT (' + CAST(@SourceConnectionID AS NVARCHAR(10)) + ') ' + CHAR(10)
		END;

		IF (@DWDestinationSchemaName = @DWExtractDWSchemaName)
		BEGIN
			SET @SourceObjectDefinition = 
					    CHAR(9) + '[DWCreatedDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWCreatedDate] DEFAULT (GETDATE()) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWModifiedDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWModifiedDate] DEFAULT (GETDATE()) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWIsDeleted] [TINYINT] NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWIsDeleted] DEFAULT (0) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWTrackingVersion] [BIGINT] NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWTrackingVersion] DEFAULT (0) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWSourceConnectionID] [BIGINT] NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWSourceConnectionID] DEFAULT (' + CAST(@SourceConnectionID AS NVARCHAR(10)) + ') ' + CHAR(10)
		END;

		IF (@DWDestinationSchemaName = @DWExtractHistorySchemaName)
		BEGIN
			SET @SourceObjectDefinition = 						  
					    CHAR(9) + '[DWCreatedDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWCreatedDate] DEFAULT (GETDATE()) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWModifiedDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWModifiedDate] DEFAULT (GETDATE()) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWIsDeleted] [TINYINT] NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWIsDeleted] DEFAULT (0) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWIsCurrent] [TINYINT] NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWIsCurrent] DEFAULT (1) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWOperation] [NVARCHAR](15) NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWOperation] DEFAULT (''I'') ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWValidFromDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWValidFromDate] DEFAULT (''1900-01-01 00:00:00.000'') ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWValidToDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWValidToDate] DEFAULT (''9999-12-31 23:59:59.000'') ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWTrackingVersion] [BIGINT] NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWTrackingVersion] DEFAULT (0) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWSourceConnectionID] [BIGINT] NOT NULL CONSTRAINT [DF_' + @SourceObjectSchema + '_' + @SourceObjectTable + '_DWSourceConnectionID] DEFAULT (' + CAST(@SourceConnectionID AS NVARCHAR(10)) + ') ' + CHAR(10)
		END;

		/* Is the destination schema associated with the Extract layer in the Data Warehouse */
		DECLARE cur CURSOR LOCAL READ_ONLY FOR
			SELECT 
				[SourceObjectColumnName]			=	sod.[SourceObjectColumnName]
			,	[SourceObjectColumnType]			=	sod.[SourceObjectColumnType]
			,	[SourceObjectColumnLength]			=	sod.[SourceObjectColumnLength]
			,	[SourceObjectColumnIsNullable]		=	CASE WHEN (Keys.SourceObjectKeyColumnId IS NULL) THEN sod.SourceObjectColumnIsNullable ELSE 'NOT NULL' END
			,	[SourceObjectColumnIsPrimaryKey]	=	CASE WHEN (Keys.SourceObjectKeyColumnId IS NULL) THEN sod.SourceObjectColumnIsPrimaryKey ELSE 1 END
			FROM [meta].[SourceObjectDefinition] AS sod WITH (NOLOCK)
			LEFT JOIN [meta].[SourceObjectKeyColumn] AS Keys WITH (NOLOCK) ON (Keys.SourceObjectId = sod.SourceObjectID) AND (Keys.SourceObjectKeyColumnName = sod.SourceObjectColumnName)
			WHERE (sod.SourceObjectID = @SourceObjectID)
			ORDER BY [SourceObjectColumnIsPrimaryKey] desc, [SourceObjectPrimaryKeyNumber], [SourceObjectColumnID]
		OPEN cur 
		FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnType, @SourceObjectColumnLength, @SourceObjectColumnIsNullable, @SourceObjectColumnIsPrimaryKey
		WHILE (@@FETCH_STATUS = 0)
		BEGIN
			
			/* Generate list of all source columns */
			IF (@SourceObjectColumns <> '')
			BEGIN
				SET @SourceObjectColumns = @SourceObjectColumns + ', ';
			END

			/* Generate list of primary keys from source */
			IF (@SourceObjectPrimaryKey <> '') AND (@SourceObjectColumnIsPrimaryKey = 1)
			BEGIN
				SET @SourceObjectPrimaryKey = @SourceObjectPrimaryKey + ', ';
			END;
			
			/* Create column definition script */
			SET @SourceObjectDefinition = @SourceObjectDefinition + ',' + CHAR(9) + QUOTENAME(@SourceObjectColumnName) + ' ' + QUOTENAME(@SourceObjectColumnType) + @SourceObjectColumnLength + ' ' + @SourceObjectColumnIsNullable + CHAR(10);
			SET @SourceObjectColumns = @SourceObjectColumns + QUOTENAME(@SourceObjectColumnName);

			/* Create Primary keys and Lookup keys used in PK and LK indexes */
			IF (@SourceObjectColumnIsPrimaryKey = 1)
			BEGIN 
				SET @SourceObjectPrimaryKey = @SourceObjectPrimaryKey + QUOTENAME(@SourceObjectColumnName);
				SET @PrimaryKey = @PrimaryKey + 'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @SourceObjectSchema + ''', @level1type = N''TABLE'', @level1name = N''' + @SourceObjectTable + ''', @level2type = N''COLUMN'', @level2name = N''' + @SourceObjectColumnName + ''', @name = N''IsPrimaryKey'', @value = 1;' + CHAR(10)
			END;
		
			FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnType, @SourceObjectColumnLength, @SourceObjectColumnIsNullable, @SourceObjectColumnIsPrimaryKey
		END 
		CLOSE cur
		DEALLOCATE cur

		/* If we are using sp_execute_remote we need to have an additional column */
		IF (@DWDestinationSchemaName NOT IN (@DWExtractDWSchemaName, @DWExtractHistorySchemaName))
		BEGIN
			SET @SourceObjectDefinition = @SourceObjectDefinition + ',' + CHAR(9) + '[$SharedName] [NVARCHAR] (255) NULL' + CHAR(10);
		END;

		/* Vertify columnlist beside Data Warehouse fields - does the table have source columns */
		IF (@SourceObjectColumns = '') OR (@SourceObjectColumns IS NULL)
		BEGIN
			SET @Message = 'Failed to create entity ' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@SourceObjectTable) + ' in ' + @DWDestinationSchemaName + ' layer as it does not contain a valid statement';
			PRINT CHAR(9) + CHAR(9) + @Message;
			RAISERROR(@Message, 1, 1);
			RETURN -2 
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @SourceObjectTable;
		END;

		/* Skip drop/create table if @DWDestinationSchemaName is ODS/HIST as we don't remove ODS/HIST tables unless done manually */
		IF (OBJECT_ID(@SourceObjectSchema + '.' + @SourceObjectTable)) IS NOT NULL AND (@DWDestinationSchemaName IN (@DWExtractHistorySchemaName, @DWExtractDWSchemaName)) AND (@emulation = 0)
		BEGIN
			SET @Message = 'Data Warehouse table: ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectTable) + ' already exists';
			PRINT CHAR(9) + CHAR(9) + @Message;
		END ELSE
		BEGIN TRY
			BEGIN TRANSACTION

			/* Generate create table statement based on @SourceObjectTable */
			SET @stmt =
				'IF (OBJECT_ID(''' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectTable) + ''')) IS NOT NULL' + CHAR(10) +
				'DROP TABLE ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectTable) + '; ' + CHAR(10) + CHAR(10) + 
				'CREATE TABLE ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectTable) + ' (' + CHAR(10) + @SourceObjectDefinition +
					CASE @DWDestinationSchemaName
					
						WHEN @DWExtractDWSchemaName  
							THEN  ',' + CHAR(9) + 'INDEX [CC_' + @SourceObjectSchema + '_' + @SourceObjectTable + '] CLUSTERED COLUMNSTORE WITH (DATA_COMPRESSION = COLUMNSTORE)' + CHAR(10)
								+ IIF(@SourceObjectPrimaryKey <> '', ',' + CHAR(9) + 'CONSTRAINT [PK_' + @SourceObjectSchema + '_' + @SourceObjectTable + '] PRIMARY KEY NONCLUSTERED (' + @SourceObjectPrimaryKey + ', [DWSourceConnectionID]) WITH (FILLFACTOR = 90, PAD_INDEX = ON)' + CHAR(10), '')
						
						WHEN @DWExtractHistorySchemaName 
							THEN  ',' + CHAR(9) + 'INDEX [CC_' + @SourceObjectSchema + '_' + @SourceObjectTable + '] CLUSTERED COLUMNSTORE WITH (DATA_COMPRESSION = COLUMNSTORE)' + CHAR(10)  
								+ IIF(@SourceObjectPrimaryKey <> '', ',' + CHAR(9) + 'CONSTRAINT [PK_' + @SourceObjectSchema + '_' + @SourceObjectTable + '] PRIMARY KEY NONCLUSTERED (' + @SourceObjectPrimaryKey + ', [DWValidFromDate], [DWValidToDate], [DWSourceConnectionID]) WITH (FILLFACTOR = 90, PAD_INDEX = ON)' + CHAR(10), '')						
					
						ELSE ''
					END +			 
				');' + CHAR(10) + CHAR(10) +

				/* Add extended properties to Primary Key columns */
				CASE 
					WHEN (@DWDestinationSchemaName NOT IN (@DWExtractDWSchemaName, @DWExtractHistorySchemaName)) THEN @PrimaryKey + CHAR(10) + CHAR(10)
					ELSE ''
				END +
	
				/* Add extended properties with information regarding ETL load parameters */
				CASE 
					WHEN (@DWDestinationSchemaName NOT IN (@DWExtractDWSchemaName, @DWExtractHistorySchemaName))
						THEN (
							SELECT 
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''SourceObjectSchema'', @value = N''' + so.SourceSchema + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''SourceObjectName'', @value = N''' + so.SourceTable + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''SourceObjectSchedule'', @value = N''' + ISNULL(NULLIF(so.SourceObjectSchedule, ''), sc.DataSourceSchedule) + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''LoadModeETL'', @value = N''' + so.LoadModeETL + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''IsReset'', @value = 1; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''IncrementalField'', @value = N''' + so.IncrementalField + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''PreserveSCD2History'', @value = ' + CAST(so.PreserveSCD2History AS nvarchar) + '; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''IncrementalOffSet'', @value = ' + CAST(so.IncrementalOffSet AS nvarchar) + '; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''SourceObjectFilter'', @value = N''' + REPLACE(so.SourceObjectFilter, '''', '''''') + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + sc.SourceConnectionSchema + ''', @level1type = N''TABLE'', @level1name = N''' + so.SourceObjectTable + ''', @name = N''IsEnabled'', @value = ' + CAST(so.IsEnabled AS nvarchar) + '; ' + CHAR(10)
							FROM meta.SourceObject AS so WITH (NOLOCK)
							INNER JOIN meta.SourceConnection AS sc WITH (NOLOCK) ON (so.SourceConnectionID = sc.SourceConnectionID)
							WHERE (so.SourceObjectID = @SourceObjectID)
						)
					ELSE ''
				END;

			/* Prepare message if sql statement is invalid NULL and raise error */
			SET @Message = 'Failed to create entity ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectTable) + ' in ' + @DWDestinationSchemaName + ' layer as it does not contain a valid statement';
			IF (@stmt IS NULL)
			BEGIN
				PRINT CHAR(9) + CHAR(9) + @Message;
				RAISERROR(@Message, 1, 1);
				RETURN -3 
				EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @SourceObjectTable;
			END;

			/* Prepare message if sql statement is valid and update meta.SourceObject */
			IF (@stmt IS NOT NULL)
			BEGIN			
				IF (@emulation = 1) SELECT @SourceObjectSchema AS SourceObjectSchema, @SourceObjectTable AS SourceObjectName, @stmt AS ObjectDefinition;
				IF (@emulation = 0)
				BEGIN
					
					/* Disable table trigger on database */
					IF (dbo.ufnIsTriggerEnabled('TableTracking') = 1)
					BEGIN
						SET @triggerStmt = 'DISABLE TRIGGER [TableTracking] ON DATABASE;';
						EXEC sys.sp_executesql @triggerStmt;
						SET @dbTriggerDisabled = 1;
					END;

					/* Generate Data Warehouse table in specified @DataWarehouseLayer */
					EXEC sys.sp_executesql @stmt;

					SET @Message = 'Successfully created ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectTable) + ' in ' + @DWDestinationSchemaName + ' schema';
					PRINT CHAR(9) + CHAR(9) + @Message;
					EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @SourceObjectTable;

					/* Enable if table trigger is disable on database */
					IF (@dbTriggerDisabled = 1)
					BEGIN
						SET @triggerStmt = 'ENABLE TRIGGER [TableTracking] ON DATABASE;';
						EXEC sys.sp_executesql @triggerStmt;
						SET @dbTriggerDisabled = 0;
					END;

					/* Set IsReset in meta.SourceObject to 0 to prevent Azure Data Factory pipeline from resetting the table */
					IF (@DWDestinationSchemaName IN (@DWExtractDWSchemaName, @DWExtractHistorySchemaName)) 
					BEGIN 
						UPDATE meta.SourceObject SET IsReset = 0 WHERE (SourceObjectID = @SourceObjectID);

						/* Run maintenance of Data Warehouse table apply schema changes */
						EXEC meta.spMaintainObject @DWSourceObjectSchema, @SourceObjectSchema, @SourceObjectTable, @emulation;

					END;
				END;
			END;

			COMMIT TRANSACTION;
		END TRY
		BEGIN CATCH
			IF (@@TRANCOUNT > 0)
				ROLLBACK TRANSACTION;
			
			/* Enable if table trigger is disable on database */
			IF (@dbTriggerDisabled = 1)
			BEGIN
				SET @triggerStmt = 'ENABLE TRIGGER [TableTracking] ON DATABASE;';
				EXEC sys.sp_executesql @triggerStmt;
				SET @dbTriggerDisabled = 0;
			END;

			SET @Message = 'Failed to create ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectTable) + ' in ' + @DWDestinationSchemaName + ' schema due to: ' + ERROR_MESSAGE();
			PRINT CHAR(9) + CHAR(9) + @Message;
			RAISERROR(@Message, 1, 1);
			RETURN -4
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @SourceObjectTable;
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @stmt, @entity = @SourceObjectTable;

		END CATCH
		FETCH NEXT FROM SchemaCur INTO @DWDestinationSchemaName
	END
	CLOSE SchemaCur
	DEALLOCATE SchemaCur

END;