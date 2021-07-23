CREATE PROCEDURE [meta].[spCreateBusinessObject]

	@BusinessObjectSchema NVARCHAR(255)
,	@BusinessObjectName NVARCHAR(255)
,	@emulation TINYINT = 1

AS 
BEGIN
	SET NOCOUNT ON;
	DECLARE @BusinessObjectColumnName NVARCHAR(255);
	DECLARE @BusinessObjectColumnType NVARCHAR(255);
	DECLARE @BusinessObjectColumnLength NVARCHAR(255);
	DECLARE @BusinessObjectColumnIsNullable NVARCHAR(255);
	DECLARE @BusinessObjectColumnIsPrimaryKey TINYINT;
	DECLARE @BusinessObjectColumnIsLookupKey TINYINT;
	DECLARE @BusinessObjectAuditColumn NVARCHAR(255);
	DECLARE @BusinessObjectAuditColumnCount INT;

	DECLARE @PreserveSCD2History TINYINT;
	DECLARE @DWBusinessKeySuffix NVARCHAR(255);
	DECLARE @DWSurrogateKeySuffix NVARCHAR(255);
	DECLARE @DWTransformSchemaName NVARCHAR(255);
	DECLARE @DWTransformDWSchemaName NVARCHAR(255);
	DECLARE @DWTransformStagingSchemaName NVARCHAR(255);
	
	
	DECLARE @DWTransformBridgeSchema NVARCHAR(255);
	DECLARE @DWTransformTempSchema NVARCHAR(255);
	DECLARE @DWTransformDimensionSchema NVARCHAR(255);
	DECLARE @DWTransformFactSchema NVARCHAR(255);


	DECLARE @DWDestinationSchemaName NVARCHAR(255);
	DECLARE @Message NVARCHAR(MAX);
	DECLARE @PackageName NVARCHAR(255);
	DECLARE @triggerStmt NVARCHAR(MAX);
	DECLARE @dbTriggerDisabled TINYINT;

	DECLARE @BusinessObjectPrefix NVARCHAR(255);
	DECLARE @BusinessObjectDefinition NVARCHAR(MAX) = '';
	DECLARE @BusinessObjectColumns NVARCHAR(MAX) = '';
	DECLARE @BusinessObjectPrimaryKey NVARCHAR(MAX) = '';
	DECLARE @BusinessObjectLookupKey NVARCHAR(MAX) = '';
	DECLARE @PrimaryKey NVARCHAR(MAX) = '';
	DECLARE @AuditColumn NVARCHAR(MAX) = '';
	DECLARE @stmt NVARCHAR(MAX);
	
	SET @PackageName					=	OBJECT_NAME(@@PROCID);
	
	/* Check if BusinessObject exists in meta.BusinessObject and reasign variables */
	SELECT 
		@BusinessObjectSchema			=	bo.BusinessObjectSchema
	,	@BusinessObjectName				=	bo.BusinessObjectName
	FROM meta.BusinessObject AS bo WITH (TABLOCK)
	WHERE (bo.BusinessObjectSchema = @BusinessObjectSchema) AND (bo.BusinessObjectName = @BusinessObjectName);

	/* Prepare common Data Warehouse parameters */	
	SELECT
		@DWTransformBridgeSchema		=	MAX(CASE WHEN (ep.[name] = 'DWBridgePrefix')				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWTransformTempSchema			=	MAX(CASE WHEN (ep.[name] = 'DWAppendixPrefix')				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWTransformDimensionSchema		=	MAX(CASE WHEN (ep.[name] = 'DWDimensionPrefix')				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWTransformFactSchema			=	MAX(CASE WHEN (ep.[name] = 'DWFactPrefix')					THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWBusinessKeySuffix			=	MAX(CASE WHEN (ep.[name] = 'DWBusinessKeySuffix')			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWSurrogateKeySuffix			=	MAX(CASE WHEN (ep.[name] = 'DWSurrogateKeySuffix')			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWTransformSchemaName			=	MAX(CASE WHEN (ep.[name] = 'DWTransformSchemaName')			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWTransformStagingSchemaName	=	MAX(CASE WHEN (ep.[name] = 'DWTransformStagingSchemaName')	THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0)
	GROUP BY (ep.major_id)

	/* For each schema in the EXTRACT layer generate create table script */
	DECLARE SchemaCur CURSOR LOCAL FOR
		SELECT s.SchemaName 
		FROM (
			VALUES
				(@BusinessObjectSchema)
			,	(@DWTransformStagingSchemaName)
		) AS s (SchemaName) 
		WHERE @BusinessObjectSchema IN (@DWTransformBridgeSchema, @DWTransformTempSchema, @DWTransformDimensionSchema, @DWTransformFactSchema)
	OPEN SchemaCur
	FETCH NEXT FROM SchemaCur INTO @DWDestinationSchemaName
	WHILE @@FETCH_STATUS = 0
	BEGIN

		/* Check if schema exists in Data Warehouse if not Create Schema */
		IF (SCHEMA_ID(@DWDestinationSchemaName) IS NULL)
		BEGIN
			SET @stmt = 
				'EXEC (''CREATE SCHEMA ' + QUOTENAME(@DWDestinationSchemaName) + ' AUTHORIZATION [dbo];'');' + CHAR(13) + CHAR(10) + CHAR(10) +
				IIF(@BusinessObjectSchema IN (@DWTransformBridgeSchema, @DWTransformDimensionSchema, @DWTransformFactSchema), 
					'EXEC (''CREATE SCHEMA ' + QUOTENAME(@DWDestinationSchemaName + 'View') + ' AUTHORIZATION [dbo];'');', 
					''
				);

			IF (@emulation = 0) EXEC sys.sp_executesql @stmt; 
			PRINT @stmt;
		END;

		/* Clear variables for each generate create table script */
		SET @BusinessObjectAuditColumnCount = 0;
		SET @BusinessObjectDefinition = '';
		SET @BusinessObjectColumns = '';
		SET @BusinessObjectPrimaryKey = '';
		SET @PrimaryKey = '';


		IF (@DWDestinationSchemaName = @DWTransformStagingSchemaName)
		BEGIN
			SET @BusinessObjectDefinition = 
				CASE WHEN (@BusinessObjectSchema = @DWTransformDimensionSchema)
					THEN CHAR(9) + '[' + @BusinessObjectName + @DWSurrogateKeySuffix + '] [BIGINT] IDENTITY(1,1) NOT NULL ' + CHAR(10) + ','
					ELSE '' 
				END +
						CHAR(9) + '[DWCreatedDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '_DWCreatedDate] DEFAULT (GETDATE()) ' + CHAR(10)
		END;

		IF (@DWDestinationSchemaName != @DWTransformStagingSchemaName)
		BEGIN
			
			/* If EDW table is not a dimension add default Audit fields */
			SET @BusinessObjectDefinition = 
				CASE WHEN (@BusinessObjectSchema = @DWTransformDimensionSchema)
					THEN CHAR(9) + '[' + @BusinessObjectName + @DWSurrogateKeySuffix + '] [BIGINT] IDENTITY(1,1) NOT NULL ' + CHAR(10) + ','
					ELSE '' 
				END +
						CHAR(9) + '[DWCreatedDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '_DWCreatedDate] DEFAULT (GETDATE()) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWModifiedDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '_DWModifiedDate] DEFAULT (GETDATE()) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWIsDeleted] [TINYINT] NOT NULL CONSTRAINT [DF_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '_DWIsDeleted] DEFAULT (0) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWIsCurrent] [TINYINT] NOT NULL CONSTRAINT [DF_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '_DWIsCurrent] DEFAULT (1) ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWValidFromDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '_DWValidFromDate] DEFAULT (''1900-01-01 00:00:00.000'') ' + CHAR(10)
				+ ',' + CHAR(9) + '[DWValidToDate] [DATETIME2](3) NOT NULL CONSTRAINT [DF_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '_DWValidToDate] DEFAULT (''9999-12-31 23:59:59.000'') ' + CHAR(10)
		END;

		/* Is the destination schema associated with the a stored procedure in the Data Warehouse staging schema */
		DECLARE cur CURSOR LOCAL READ_ONLY FOR
			SELECT
				[BusinessObjectColumnName]			=	c.name
			,	[BusinessObjectColumnType]			=	UPPER(t.name)
			,	[BusinessObjectColumnLength]		=	CASE 
															WHEN t.name IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN c.max_length = -1 THEN '4000' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
															WHEN t.name IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN c.max_length = -1 THEN '4000' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
															WHEN t.name IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
															WHEN t.name IN ('numeric', 'decimal') THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ', ' + CAST(c.scale AS VARCHAR(5)) + ')'
															ELSE ''
														END
			,	[BusinessObjectColumnIsNullable]	=	CASE WHEN c.name LIKE (OBJECT_NAME(c.object_id) + '%' + @DWBusinessKeySuffix) THEN 'NOT NULL' ELSE 'NULL' END
			,	[BusinessObjectColumnIsPrimaryKey]	=	CASE WHEN c.name LIKE (OBJECT_NAME(c.object_id) + '%' + @DWBusinessKeySuffix) THEN 1 ELSE 0 END
			,	[BusinessObjectColumnIsLookupKey]	=	CASE WHEN (so.LookupKey LIKE ('%' + c.name + '%')) THEN 1 ELSE 0 END
			FROM sys.columns AS c	
			INNER JOIN sys.types AS t ON (c.user_type_id = t.user_type_id)
			INNER JOIN meta.BusinessObject AS so ON (so.BusinessObjectSchema = @BusinessObjectSchema) AND (so.BusinessObjectName = @BusinessObjectName)
			WHERE (c.object_id = OBJECT_ID(@DWTransformStagingSchemaName + '.' + @BusinessObjectName)) AND (c.is_identity = 0) AND (c.default_object_id = 0)
			ORDER BY c.column_id
		OPEN cur 
		FETCH NEXT FROM cur INTO @BusinessObjectColumnName, @BusinessObjectColumnType, @BusinessObjectColumnLength, @BusinessObjectColumnIsNullable, @BusinessObjectColumnIsPrimaryKey, @BusinessObjectColumnIsLookupKey
		WHILE (@@FETCH_STATUS = 0)
		BEGIN
			
			/* Generate list of all source columns */
			IF (@BusinessObjectColumns <> '')
			BEGIN
				SET @BusinessObjectColumns = @BusinessObjectColumns + ', ';
			END

			/* Generate list of primary keys from source */
			IF (@BusinessObjectPrimaryKey <> '') AND (@BusinessObjectColumnIsPrimaryKey = 1)
			BEGIN
				SET @BusinessObjectPrimaryKey = @BusinessObjectPrimaryKey + ', ';
			END;

			/* Generate list of lookup keys from source */
			IF (@BusinessObjectLookupKey <> '') AND (@BusinessObjectColumnIsLookupKey = 1)
			BEGIN
				SET @BusinessObjectLookupKey = @BusinessObjectLookupKey + ', '; 
			END;
			
			/* Create column definition script */
			SET @BusinessObjectDefinition = @BusinessObjectDefinition + ',' + CHAR(9) + QUOTENAME(@BusinessObjectColumnName) + ' ' + QUOTENAME(@BusinessObjectColumnType) + @BusinessObjectColumnLength + ' ' + @BusinessObjectColumnIsNullable + CHAR(10);
			SET @BusinessObjectColumns = @BusinessObjectColumns + QUOTENAME(@BusinessObjectColumnName);

			/* Does the business object have predefined SCD2 audit columns then include in Primary key */
			IF (@BusinessObjectColumnName IN (@BusinessObjectName + 'ValidFromDate', @BusinessObjectName + 'ValidToDate', @BusinessObjectName + 'IsCurrent'))
			BEGIN
				SET @BusinessObjectAuditColumn = @BusinessObjectAuditColumn + ', ' + QUOTENAME(@BusinessObjectColumnName);
				SET @AuditColumn = @AuditColumn + 'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + @BusinessObjectName + ''', @level2type = N''COLUMN'', @level2name = N''' + @BusinessObjectColumnName + ''', @name = N''IsPrimaryKey'', @value = 1;' + CHAR(10)
				SET @BusinessObjectAuditColumnCount = @BusinessObjectAuditColumnCount + 1;
			END;

			/* Create Primary keys used in PK index */
			IF (@BusinessObjectColumnIsPrimaryKey = 1) 
			BEGIN 
				SET @BusinessObjectPrimaryKey = @BusinessObjectPrimaryKey + QUOTENAME(@BusinessObjectColumnName);
				SET @PrimaryKey = @PrimaryKey + 'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + @BusinessObjectName + ''', @level2type = N''COLUMN'', @level2name = N''' + @BusinessObjectColumnName + ''', @name = N''IsPrimaryKey'', @value = 1;' + CHAR(10)
			END;

			/* Create Lookup keys used in LK indexes */
			IF (@BusinessObjectColumnIsLookupKey = 1) SET @BusinessObjectLookupKey = @BusinessObjectLookupKey + QUOTENAME(@BusinessObjectColumnName);
		
			FETCH NEXT FROM cur INTO @BusinessObjectColumnName, @BusinessObjectColumnType, @BusinessObjectColumnLength, @BusinessObjectColumnIsNullable, @BusinessObjectColumnIsPrimaryKey, @BusinessObjectColumnIsLookupKey
		END 
		CLOSE cur
		DEALLOCATE cur

		/* Vertify columnlist beside Data Warehouse fields - does the table have source columns */
		IF (@BusinessObjectColumns = '') OR (@BusinessObjectColumns IS NULL)
		BEGIN
			SET @Message = 'Failed to create entity ' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@BusinessObjectName) + ' in ' + @DWDestinationSchemaName + ' layer as it does not contain a valid statement';
			PRINT CHAR(9) + CHAR(9) + @Message;
			RAISERROR(@Message, 1, 1);
			RETURN -2 
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @BusinessObjectName;
		END;

		/* Skip drop/create table if @DWDestinationSchemaName is ODS/HIST as we don't remove ODS/HIST tables unless done manually */
		IF (OBJECT_ID(@DWDestinationSchemaName + '.' + @BusinessObjectName)) IS NOT NULL AND (@DWDestinationSchemaName IN (@DWTransformBridgeSchema, @DWTransformTempSchema, @DWTransformDimensionSchema, @DWTransformFactSchema)) AND (@emulation = 0)
		BEGIN
			SET @Message = 'Data Warehouse table: ' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@BusinessObjectName) + ' already exists';
			PRINT CHAR(9) + CHAR(9) + @Message;
		END ELSE
		BEGIN TRY
			BEGIN TRANSACTION

			/* Generate create table statement based on @BusinessObjectName */
			SET @stmt =
				'IF (OBJECT_ID(''' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@BusinessObjectName) + ''')) IS NOT NULL' + CHAR(10) +
				'DROP TABLE ' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@BusinessObjectName) + '; ' + CHAR(10) + CHAR(10) + 
				'CREATE TABLE ' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@BusinessObjectName) + ' (' + CHAR(10) + @BusinessObjectDefinition +
				CASE 
					WHEN (@BusinessObjectColumns <> '') AND (@DWDestinationSchemaName IN (@DWTransformBridgeSchema, @DWTransformTempSchema, @DWTransformDimensionSchema, @DWTransformFactSchema))
					THEN
						CASE 
							WHEN @DWDestinationSchemaName IN (@DWTransformDimensionSchema)
								THEN ',' + CHAR(9) + 'INDEX [C_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '] CLUSTERED ([' + @BusinessObjectName + @DWSurrogateKeySuffix +']) WITH (FILLFACTOR = 90, PAD_INDEX = ON, OPTIMIZE_FOR_SEQUENTIAL_KEY = ON)' + CHAR(10)
										 + IIF(@BusinessObjectPrimaryKey <> '', ',' + CHAR(9) + 'CONSTRAINT [PK_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '] PRIMARY KEY NONCLUSTERED (' + @BusinessObjectPrimaryKey + IIF(@BusinessObjectAuditColumnCount = 3, @BusinessObjectAuditColumn, ', [DWValidFromDate], [DWValidToDate]') + ') WITH (FILLFACTOR = 90, PAD_INDEX = ON)' + CHAR(10), '')
							WHEN @DWDestinationSchemaName IN (@DWTransformBridgeSchema, @DWTransformTempSchema, @DWTransformFactSchema)
								THEN ',' + CHAR(9) + 'INDEX [CC_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '] CLUSTERED COLUMNSTORE WITH (DATA_COMPRESSION = COLUMNSTORE)' + CHAR(10)
										 + IIF(@BusinessObjectPrimaryKey <> '', ',' + CHAR(9) + 'CONSTRAINT [PK_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '] PRIMARY KEY NONCLUSTERED (' + @BusinessObjectPrimaryKey + ', [DWCreatedDate], [DWModifiedDate]) WITH (FILLFACTOR = 90, PAD_INDEX = ON)' + CHAR(10), '')
										 + IIF(@BusinessObjectLookupKey  <> '', ',' + CHAR(9) + 'INDEX [LK_' + @DWDestinationSchemaName + '_' + @BusinessObjectName + '] NONCLUSTERED (' + @BusinessObjectLookupKey + ') WITH (FILLFACTOR = 90, PAD_INDEX = ON)' + CHAR(10), '')
							ELSE ''
						END	 
					ELSE ''
				END + ');' + CHAR(10) + CHAR(10) +

				/* Add extended properties to Primary Key columns */
				CASE 
					WHEN (@DWDestinationSchemaName IN (@DWTransformStagingSchemaName)) THEN @PrimaryKey + IIF(@BusinessObjectAuditColumnCount = 3 AND @AuditColumn <> '', @AuditColumn, '') + CHAR(10) + CHAR(10)
					ELSE ''
				END +

				/* Add extended properties with information regarding ETL load parameters */
				CASE 
					WHEN (@DWDestinationSchemaName IN (@DWTransformStagingSchemaName))
						THEN (
							SELECT
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''DataWarehouseLayer'', @value = N''Transform''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''BusinessObjectSchema'', @value = N''' + @BusinessObjectSchema + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''SourceObjectSchema'', @value = N''' + IIF(sed.referenced_id IS NULL, @DWTransformSchemaName, OBJECT_SCHEMA_NAME(o.object_id)) + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''SourceObjectName'', @value = N''' + IIF(sed.referenced_id IS NULL, @BusinessObjectName, o.name) + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''BusinessObjectLookupKey'', @value = N''' + so.LookupKey + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''BusinessObjectSchedule'', @value = N''' + so.Schedule + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''RolePlayingEntity'', @value = N''' + so.RolePlayingEntity + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''LoadModeETL'', @value = N''' + so.LoadPattern + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''IsReset'', @value = 1; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''IncrementalField'', @value = N''' + so.IncrementalField + '''; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''PreserveSCD2History'', @value = ' + CAST(so.PreserveSCD2History AS nvarchar) + '; ' + CHAR(10) +
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''IncrementalOffSet'', @value = ' + CAST(so.IncrementalOffSet AS nvarchar) + '; ' + CHAR(10) +					
								'EXEC sys.sp_AddExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DWDestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + so.BusinessObjectName + ''', @name = N''IsEnabled'', @value = ' + CAST(so.IsEnabled AS nvarchar) + '; ' + CHAR(10)
							FROM meta.BusinessObject AS so
							LEFT JOIN sys.sql_expression_dependencies AS sed ON (sed.referenced_id = OBJECT_ID(@DWDestinationSchemaName + '.' + so.BusinessObjectName))
							LEFT JOIN sys.objects AS o ON sed.referencing_id = o.object_id AND o.type = 'P'
							WHERE (so.BusinessObjectSchema = @BusinessObjectSchema) AND (so.BusinessObjectName = @BusinessObjectName)
						)
					ELSE ''
				END;

			/* Prepare message if sql statement is invalid NULL and raise error */
			SET @Message = 'Failed to create entity ' + QUOTENAME(@BusinessObjectSchema) + '.' + QUOTENAME(@BusinessObjectName) + ' in ' + @DWDestinationSchemaName + ' layer as it does not contain a valid statement';
			IF (@stmt IS NULL)
			BEGIN
				PRINT CHAR(9) + CHAR(9) + @Message;
				RAISERROR(@Message, 1, 1);
				RETURN -3 
				EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @BusinessObjectName;
			END;

			/* Prepare message if sql statement is valid and update meta.BusinessObject */
			IF (@stmt IS NOT NULL)
			BEGIN			
				IF (@emulation = 1) SELECT @DWDestinationSchemaName AS DestinationSchema, @BusinessObjectName AS BusinessObjectName, @stmt AS ObjectDefinition;
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

					SET @Message = 'Successfully created ' + QUOTENAME(@BusinessObjectSchema) + '.' + QUOTENAME(@BusinessObjectName) + ' in ' + @DWDestinationSchemaName + ' schema';
					PRINT CHAR(9) + CHAR(9) + @Message;
					EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @BusinessObjectName;

					/* Enable if table trigger is disable on database */
					IF (@dbTriggerDisabled = 1)
					BEGIN
						SET @triggerStmt = 'ENABLE TRIGGER [TableTracking] ON DATABASE;';
						EXEC sys.sp_executesql @triggerStmt;
						SET @dbTriggerDisabled = 0;
					END;

					/* Set IsReset in dbo.SourceObject to 0 to prevent Azure Data Factory pipeline from resetting the table */
					IF (@DWDestinationSchemaName IN (@DWTransformStagingSchemaName)) 
					BEGIN 
						/* Run maintenance of Data Warehouse table apply schema changes */
						EXEC meta.spMaintainObject @DWTransformStagingSchemaName, @BusinessObjectSchema, @BusinessObjectName, @emulation;
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

			SET @Message = 'Failed to create ' + QUOTENAME(@BusinessObjectSchema) + '.' + QUOTENAME(@BusinessObjectName) + ' in ' + @DWDestinationSchemaName + ' schema due to: ' + ERROR_MESSAGE();
			PRINT CHAR(9) + CHAR(9) + @Message;
			RAISERROR(@Message, 1, 1);
			RETURN -4
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @BusinessObjectName;
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @stmt, @entity = @BusinessObjectName;

		END CATCH
		FETCH NEXT FROM SchemaCur INTO @DWDestinationSchemaName
	END
	CLOSE SchemaCur
	DEALLOCATE SchemaCur

END;