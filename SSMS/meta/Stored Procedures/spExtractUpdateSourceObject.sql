CREATE PROCEDURE [meta].[spExtractUpdateSourceObject]

	@DestinationSchemaName NVARCHAR(255)
,	@DestinationTableName NVARCHAR(255)
,	@PackageName NVARCHAR(MAX)
,	@LoadSequence INT
,	@emulation TINYINT = 1
AS 
BEGIN

	DECLARE @CRLF VARCHAR(2) = CHAR(13) + CHAR(10);
	DECLARE @TaskName NVARCHAR(MAX) = '';
	DECLARE @Message NVARCHAR(MAX) = '';
	DECLARE @stmt NVARCHAR(MAX) = '';
	DECLARE @ValidFromDate NVARCHAR(30);
	DECLARE @ValidToDate NVARCHAR(30);

	DECLARE @DWExtractStagingSchemaName NVARCHAR(255) = '';
	DECLARE @DWExtractDWSchemaName NVARCHAR(255) = '';
	DECLARE @DWExtractHistorySchemaName NVARCHAR(255) = '';

	DECLARE @SourceObjectExistsInDWSchemaName TINYINT = 0;
	DECLARE @SourceObjectExistsInHistorySchemaName TINYINT = 0;	

	DECLARE @IncrementalField NVARCHAR(255) = '';
	DECLARE @SourceObjectColumnName NVARCHAR(255) = '';
	DECLARE @SourceObjectColumnIsPrimaryKey TINYINT = '';

	DECLARE @DestinationColumns NVARCHAR(MAX) = '';
	DECLARE @SourceColumns NVARCHAR(MAX) = '';
	DECLARE @TargetColumns NVARCHAR(MAX) = '';
	DECLARE @PrimaryKeyColumns NVARCHAR(MAX) = '';
	DECLARE @SourceColumnsWithoutIncrementalField NVARCHAR(MAX) = '';
	DECLARE @TargetColumnsWithoutIncrementalField NVARCHAR(MAX) = '';
	DECLARE @SourceTargetPrimaryKeyColumns NVARCHAR(MAX) = '';

	DECLARE @JobLoadModeETL NVARCHAR(50) = '';
	DECLARE @JobIsReset TINYINT = 0;
	DECLARE @StopExecution TINYINT = 0;
	DECLARE @IndexFragmentationLimit INT;
	DECLARE @LoadPattern NVARCHAR(255) = '';

	DECLARE @UpdateCnt BIGINT = 0;
	DECLARE @InsertCnt BIGINT = 0;
	DECLARE @DeleteCnt BIGINT = 0;

	/* Prepare common Data Warehouse parameters */
	SELECT
		@ValidFromDate					=	CAST(CAST(GETUTCDATE() AS DATETIME2(3)) AS NVARCHAR)
	,	@ValidToDate					=	CAST(CAST(DATEADD(MS, -3, @ValidFromDate) AS DATETIME2(3)) AS NVARCHAR)
	,	@DWExtractStagingSchemaName		=	MAX(CASE WHEN [name] = 'DWExtractStagingSchemaName'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWExtractDWSchemaName			=	MAX(CASE WHEN [name] = 'DWExtractDWSchemaName'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWExtractHistorySchemaName		=	MAX(CASE WHEN [name] = 'DWExtractHistorySchemaName'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@IndexFragmentationLimit		=	MAX(CASE WHEN [name] = 'IndexFragmentationLimit'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0) 
	GROUP BY (ep.major_id)

	/* Prepare SourceObject parameters */
	SELECT
		@IncrementalField				=	MAX(CASE WHEN [name] = 'IncrementalField'				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@JobLoadModeETL					=	MAX(CASE WHEN [name] = 'LoadModeETL'					THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)	
	,	@JobIsReset						=	MAX(CASE WHEN [name] = 'IsReset'						THEN CONVERT(TINYINT,		[value]) ELSE 0 END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 1) AND (ep.major_id = OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName))
	GROUP BY (ep.major_id)

	/* Check if @DestinationSchemaName != from @DWExtractStagingSchemaName due to segregation of data sources */
	IF (@DestinationSchemaName != @DWExtractStagingSchemaName)
	BEGIN
		SET @DWExtractDWSchemaName		=	@DestinationSchemaName + '_' + @DWExtractDWSchemaName;
		SET @DWExtractHistorySchemaName =	@DestinationSchemaName + '_' + @DWExtractHistorySchemaName;		
	END;

	/* Check of table exists in Extract DW Schema and Extract History Schema */
	IF(OBJECT_ID(@DWExtractDWSchemaName + '.' + @DestinationTableName) IS NOT NULL)			SET @SourceObjectExistsInDWSchemaName = 1;
	IF(OBJECT_ID(@DWExtractHistorySchemaName + '.' + @DestinationTableName) IS NOT NULL)	SET @SourceObjectExistsInHistorySchemaName = 1;

	/* Detect load pattern based on SourceObject parameters */
	SET @LoadPattern = 
			CASE 
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'CT') THEN 'Change Tracking'
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'CDC') THEN 'Change Data Capture' 
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'ICL') THEN 'Incremental load'
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'FULL') THEN 'Full load'
				WHEN (@JobIsReset = 1) THEN 'Force full load'
			END
		
	/* Generate column list used in main select and primary key columns in join statement */
	DECLARE cur CURSOR LOCAL FOR 
		SELECT
			[SourceObjectColumnName]			=	c.name
		,	[SourceObjectColumnIsPrimaryKey]	=	CASE WHEN (ep.value IS NOT NULL) THEN 1 ELSE 0 END
		FROM sys.columns AS c WITH (NOLOCK)
		LEFT JOIN sys.extended_properties AS ep WITH (NOLOCK) ON (c.object_id = ep.major_id) AND (c.column_id = ep.minor_id) AND (ep.name = 'IsPrimaryKey')
		WHERE (c.is_identity = 0) AND (c.default_object_id = 0) AND (c.name NOT IN ('$SharedName')) AND (c.object_id = OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName))
		ORDER BY c.column_id
	OPEN cur
	FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnIsPrimaryKey
	WHILE @@FETCH_STATUS = 0
	BEGIN

		IF (@DestinationColumns <> '')
		BEGIN
			SET @DestinationColumns = @DestinationColumns + ', ';
			SET @SourceColumns = @SourceColumns + ', ';
			SET @TargetColumns = @TargetColumns + ', ';

			/* Create source and target column variables for detecting changed records */
			IF(@SourceColumnsWithoutIncrementalField <> '') AND (@SourceObjectColumnName <> @IncrementalField) SET @SourceColumnsWithoutIncrementalField = @SourceColumnsWithoutIncrementalField + ', ';
			IF(@TargetColumnsWithoutIncrementalField <> '') AND (@SourceObjectColumnName <> @IncrementalField) SET @TargetColumnsWithoutIncrementalField = @TargetColumnsWithoutIncrementalField + ', ';
		END;

		SET @DestinationColumns = @DestinationColumns + QUOTENAME(@SourceObjectColumnName);
		SET @SourceColumns = @SourceColumns + '[SOURCE].' + QUOTENAME(@SourceObjectColumnName);
		SET @TargetColumns = @TargetColumns + '[TARGET].' + QUOTENAME(@SourceObjectColumnName);

		/* Create source and target column variables for detecting changed records */
		IF(@SourceObjectColumnName <> @IncrementalField) SET @SourceColumnsWithoutIncrementalField = @SourceColumnsWithoutIncrementalField + '[SOURCE].' + QUOTENAME(@SourceObjectColumnName);
		IF(@SourceObjectColumnName <> @IncrementalField) SET @TargetColumnsWithoutIncrementalField = @TargetColumnsWithoutIncrementalField + '[TARGET].' + QUOTENAME(@SourceObjectColumnName);

		/* Create key columns join */
		IF (@SourceTargetPrimaryKeyColumns <> '') AND (@SourceObjectColumnIsPrimaryKey = 1)
		BEGIN
			SET @PrimaryKeyColumns = @PrimaryKeyColumns + ', ';
			SET @SourceTargetPrimaryKeyColumns = @SourceTargetPrimaryKeyColumns + ' AND ';
		END;

		IF(@SourceObjectColumnIsPrimaryKey = 1) 
		BEGIN
			SET @PrimaryKeyColumns = @PrimaryKeyColumns + QUOTENAME(@SourceObjectColumnName);
			SET @SourceTargetPrimaryKeyColumns = @SourceTargetPrimaryKeyColumns + '([SOURCE].' + QUOTENAME(@SourceObjectColumnName) + ' = [TARGET].' + QUOTENAME(@SourceObjectColumnName) + ')';
		END;

		FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnIsPrimaryKey
	END
	CLOSE cur
	DEALLOCATE cur

	/* Prepare load of ODS layer using soft-deletes and Delete/Insert pattern */
	IF (@SourceObjectExistsInDWSchemaName = 1) AND (@SourceTargetPrimaryKeyColumns <> '')
	BEGIN

		/* Create temp table to hold all deleted and updated records */
		SET @stmt = 
			'	/* Create temp table to hold all deleted and updated records */' + @CRLF +
			'	DROP TABLE IF EXISTS #ChangedRecords;' + @CRLF + @CRLF +
			'	/* Create temp table #ChangedRecords to hold all deleted/updated record */' + @CRLF +
			'	SELECT ' + @TargetColumns + ', [TARGET].[DWCreatedDate], [TARGET].[DWTrackingVersion], [TARGET].[DWModifiedDate], [TARGET].[DWIsDeleted]' + @CRLF +
			'	INTO #ChangedRecords' + @CRLF + 
			'	FROM ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
			'	WHERE 1 = 0' + @CRLF + @CRLF
		;
		
		/* Detect which records that has been deleted in source */
		SET @stmt = @stmt + 
			'	/* Detect which records that has been deleted in source */' + @CRLF +
			'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWTrackingVersion], [DWModifiedDate], [DWIsDeleted])' + @CRLF +
			'	SELECT	 ' + @TargetColumns + @CRLF +
			'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
			'	,	[DWTrackingVersion]	= [TARGET].[DWTrackingVersion] ' + @CRLF +
			'	,	[DWModifiedDate]	= ''' + @ValidFromDate + ''' ' + @CRLF +
			'	,	[DWIsDeleted]		= 1 ' + @CRLF +
			'	FROM ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
				CASE
					/* If load is incremental and JobIsReset is false then soft-delete if when exist in source table */
					WHEN (@JobLoadModeETL NOT IN ('FULL','CUSTOM')) AND (@JobIsReset = 0)
						THEN '	INNER JOIN ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF +
							 '	ON (' + @SourceTargetPrimaryKeyColumns + ') AND ([SOURCE].[DWOperation] = ''D'') '

					/* If load is not incremental or JobIsReset is true then soft-delete if not exist in source table */
					WHEN (@JobLoadModeETL IN ('FULL','CUSTOM')) OR (@JobIsReset = 1)
						THEN '	WHERE NOT EXISTS ( ' + @CRLF +
							 '		SELECT 1 FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF + 
							 '		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
							 '	) '
				END + 'AND ([TARGET].[DWIsDeleted] = 0) ' + @CRLF +
			'	SELECT @DeleteCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
		;

		/* Detect which records that has been Updated in source */
		SET @stmt = @stmt +
			'	/* Detect which records that has been Updated in source */' + @CRLF +
			'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWTrackingVersion], [DWModifiedDate], [DWIsDeleted])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + @CRLF +
			'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
			'	,	[DWTrackingVersion]	= [SOURCE].[DWTrackingVersion] ' + @CRLF +
			'	,	[DWModifiedDate]	= ''' + @ValidFromDate + ''' ' + @CRLF +
			'	,	[DWIsDeleted]		= 0 ' + @CRLF +
			'	FROM ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF + 
			'	INNER JOIN ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF +
			'	ON (' + @SourceTargetPrimaryKeyColumns + ') AND ([SOURCE].[DWOperation] IN (''I'', ''U''))' + @CRLF +
			'	WHERE EXISTS (' + @CRLF +
			'		 SELECT ' + @SourceColumnsWithoutIncrementalField + @CRLF +
			'		 EXCEPT ' + @CRLF +  
			'		 SELECT ' + @TargetColumnsWithoutIncrementalField + @CRLF +
			'	)' + @CRLF +
			'	SELECT @UpdateCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
		;

		/* Create Nonclustered Index on #ChangedRecords */
		SET @stmt = @stmt +
			'	/* Create Nonclustered Index on #ChangedRecords */' + @CRLF +
			'	CREATE NONCLUSTERED INDEX [NCI_ChangedRecords] ON #ChangedRecords (' + @PrimaryKeyColumns + ') ;' + @CRLF + @CRLF
		;

		/* Delete records which should deleted or updated */ 
		SET @stmt = @stmt +
			'	/* Delete records which should deleted or updated */' + @CRLF +
			'	DELETE [TARGET] WITH (TABLOCKX)' + @CRLF +
			'	FROM ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
			'	INNER JOIN #ChangedRecords AS [SOURCE] ON (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF + @CRLF
		;

		/* Insert records which have been soft-deleted or updated */ 
		SET @stmt = @stmt +
			'	/* Insert records which have been soft-deleted or updated */' + @CRLF + 
			'	INSERT INTO ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWTrackingVersion])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + ', [SOURCE].[DWCreatedDate], [SOURCE].[DWModifiedDate], [SOURCE].[DWIsDeleted], [SOURCE].[DWTrackingVersion]' + @CRLF +
			'	FROM #ChangedRecords AS [SOURCE]' + @CRLF + @CRLF
		;

		/* Handle Inserts from Source - Insert new and changed records */
		SET @stmt = @stmt +
			'	/* Handle Inserts from Source - Insert new and changed records */ ' + @CRLF +
			'	INSERT INTO ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWTrackingVersion])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + ', ''' + @ValidFromDate + ''', ''' + @ValidFromDate + ''', [SOURCE].[DWTrackingVersion] ' + @CRLF +
			'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF +
			'	WHERE NOT EXISTS (' + @CRLF +
			'		SELECT 1 FROM ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
			'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
			'	) AND ([SOURCE].[DWOperation] IN (''I'', ''U''))' + @CRLF +
			'	SELECT @InsertCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
		;

	END ELSE 
	
	/* Force full load due to missing primary key */
	IF (@SourceObjectExistsInDWSchemaName = 1)
	BEGIN
		SET @stmt = 
			'	INSERT INTO ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWTrackingVersion])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + ', ''' + @ValidFromDate + ''', ''' + @ValidFromDate + ''', [SOURCE].[DWTrackingVersion] ' + @CRLF +
			'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE];' + @CRLF + @CRLF +
			'	SELECT @InsertCnt = @@ROWCOUNT;' + @CRLF
		;
	END;

	BEGIN TRY
		BEGIN TRANSACTION 
		
		/* Execute Merge statement within a begin try catch block */
		
		IF (@SourceObjectExistsInDWSchemaName = 1) AND (@SourceTargetPrimaryKeyColumns <> '')
		BEGIN

			SET @TaskName = @LoadPattern + ' - Load ' + @DWExtractDWSchemaName + ' using ' + @DestinationSchemaName + ''

			/* Delta load pattern with Insert, Update and Delete */
			IF (@emulation = 1) SELECT @stmt AS LoadSCD1Statement;
			IF (@emulation = 0) 
			BEGIN
				EXEC sys.sp_executesql @stmt, N'@UpdateCnt bigint OUTPUT, @InsertCnt bigint OUTPUT, @DeleteCnt bigint OUTPUT', @UpdateCnt OUTPUT, @InsertCnt OUTPUT, @DeleteCnt OUTPUT;

				SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected inserts: ' + CONVERT(nvarchar, @InsertCnt) + ' rows';
				EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'INSERT', @rows = @InsertCnt;

				SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected updates: ' + CONVERT(nvarchar, @UpdateCnt) + ' rows';
				EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'UPDATE', @rows = @updateCnt;

				SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected deletes: ' + CONVERT(nvarchar, @deleteCnt) + ' rows';
				EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'DELETE', @rows = @deleteCnt;

				/* Rebuild/reorganize Columnstore Index */
				SET @stmt = (
					SELECT 'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(ix.IndexName) + ' ON ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (DROP_EXISTING = ON);'
					FROM (
						SELECT
							[IndexName]				=	i.name
						,	[IndexFragmentation]	=	100 * ISNULL(SUM(rg.deleted_rows), 0) / NULLIF(MAX(p.rows), 0)
						FROM sys.column_store_row_groups AS rg WITH (NOLOCK)
						JOIN sys.indexes AS i WITH (NOLOCK) ON  i.object_id = rg.object_id AND i.index_id = rg.index_id
						JOIN sys.partitions AS p WITH (NOLOCK) ON p.object_id = i.object_id
						WHERE i.type IN (5) -- 5 = Clustered columnstore index.
						AND i.object_id = OBJECT_ID(@DWExtractDWSchemaName + '.' + @DestinationTableName)
						GROUP BY i.name, i.type_desc
					) AS ix
					WHERE ix.IndexFragmentation > @IndexFragmentationLimit
				)
				SET @stmt = ISNULL(@stmt, '');
				EXEC sys.sp_executesql @stmt;

				/* Update statistics */
				IF (@stmt IS NOT NULL) AND (@stmt <> '')
				BEGIN
					SET @stmt = 'UPDATE STATISTICS ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';';
					EXEC sys.sp_executesql @stmt;
				END;
			END;

		END ELSE

		IF (@SourceObjectExistsInDWSchemaName = 1)
		BEGIN
			
			SET @TaskName = 'Full load - Load ' + @DWExtractDWSchemaName + ' using ' + @DestinationSchemaName + ''

			/* Full load pattern with truncate and insert */
			IF (@emulation = 1) SELECT @stmt AS LoadSCD1Statement;
			IF (@emulation = 0) 
			BEGIN
				EXEC sys.sp_executesql @stmt, N'@InsertCnt bigint OUTPUT', @InsertCnt OUTPUT;

				SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Load ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected: ' + CONVERT(nvarchar, @InsertCnt) + ' rows';
				EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'RESET', @rows = @InsertCnt;

				/* Rebuild/reorganize Columnstore Index */
				SET @stmt = (
					SELECT 'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(ix.IndexName) + ' ON ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (DROP_EXISTING = ON);'
					FROM (
						SELECT
							[IndexName]				=	i.name
						,	[IndexFragmentation]	=	100 * ISNULL(SUM(rg.deleted_rows), 0) / NULLIF(MAX(p.rows), 0)
						FROM sys.column_store_row_groups AS rg WITH (NOLOCK)
						JOIN sys.indexes AS i WITH (NOLOCK) ON  i.object_id = rg.object_id AND i.index_id = rg.index_id
						JOIN sys.partitions AS p WITH (NOLOCK) ON p.object_id = i.object_id
						WHERE i.type IN (5) -- 5 = Clustered columnstore index.
						AND i.object_id = OBJECT_ID(@DWExtractDWSchemaName + '.' + @DestinationTableName)
						GROUP BY i.name, i.type_desc
					) AS ix
					WHERE ix.IndexFragmentation > @IndexFragmentationLimit
				)
				SET @stmt = ISNULL(@stmt, '');
				EXEC sys.sp_executesql @stmt;

				/* Rebuild (Non)Clustered Index */
				SET @stmt = NULL;
				SELECT @stmt = ISNULL(@stmt, '') + 'ALTER INDEX ' + QUOTENAME(i.name) + ' ON ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' REBUILD WITH (ONLINE = OFF);' + CHAR(10)
				FROM sys.dm_db_index_physical_stats (DB_ID(), OBJECT_ID(@DWExtractDWSchemaName + '.' + @DestinationTableName), NULL, NULL, NULL) AS ips
				JOIN sys.indexes AS i WITH (NOLOCK) ON (ips.object_id = i.object_id) AND (ips.index_id = i.index_id)
				WHERE (i.type IN (1, 2)) AND (ips.avg_fragmentation_in_percent > @IndexFragmentationLimit)
				
				SET @stmt = ISNULL(@stmt, '');
				EXEC sys.sp_executesql @stmt;

				/* Update statistics */
				IF (@stmt IS NOT NULL) AND (@stmt <> '')
				BEGIN
					SET @stmt = 'UPDATE STATISTICS ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';';
					EXEC sys.sp_executesql @stmt;
				END;
			END;
		END;
		
		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

		SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Failed to Load ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' due to: ' + @CRLF + ERROR_MESSAGE();
		EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + @CRLF + @stmt;
		EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		SET @StopExecution = 1;
	END CATCH

	/* Should we update History (SCD2) layer */
	IF (@SourceObjectExistsInHistorySchemaName = 1) AND (@SourceTargetPrimaryKeyColumns <> '')
	BEGIN TRY
		BEGIN TRANSACTION 

		SET @TaskName = @LoadPattern + ' - Load ' + @DWExtractHistorySchemaName + ' using ' + @DestinationSchemaName + ''

		/* Create temp table to hold all deleted and updated records */
		SET @stmt = 
			'	/* Create temp table to hold all deleted and updated records */' + @CRLF +
			'	DROP TABLE IF EXISTS #ChangedRecords;' + @CRLF + @CRLF +
			'	/* Create temp table #ChangedRecords to hold all deleted/updated record */' + @CRLF +
			'	SELECT ' + @TargetColumns + ', [TARGET].[DWCreatedDate], [TARGET].[DWTrackingVersion], [TARGET].[DWModifiedDate], [TARGET].[DWIsDeleted], [TARGET].[DWIsCurrent], [TARGET].[DWOperation], [TARGET].[DWValidFromDate], [TARGET].[DWValidToDate]' + @CRLF +
			'	INTO #ChangedRecords' + @CRLF + 
			'	FROM ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
			'	WHERE 1 = 0' + @CRLF + @CRLF
		;

		/* Handle History Deletes from Source */
		SET @stmt = @stmt +  
			'	/* Handle History Deletes from Source */ ' + @CRLF +
			'	INSERT INTO #ChangedRecords WITH (TABLOCKX) ' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWTrackingVersion], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [DWOperation], [DWValidFromDate], [DWValidToDate])' + @CRLF +
			'	SELECT	 ' + @TargetColumns + @CRLF +
			'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
			'	,	[DWTrackingVersion]	= [TARGET].[DWTrackingVersion] ' + @CRLF +
			'	,	[DWModifiedDate]	= ''' + @ValidFromDate + ''' ' + @CRLF +
			'	,	[DWIsDeleted]		= 1 ' + @CRLF +
			'	,	[DWIsCurrent]		= 1 ' + @CRLF +
			'	,	[DWOperation]		= ''D'' ' + @CRLF +
			'	,	[DWValidFromDate]	= [TARGET].[DWValidFromDate] ' + @CRLF +
			'	,	[DWValidToDate]		= ''' + @ValidToDate + ''' ' + @CRLF +
			'	FROM ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET] ' + @CRLF +
				CASE
					/* If load is incremental and JobIsReset is false then soft-delete if when exist in source table */
					WHEN (@JobLoadModeETL NOT IN ('FULL','CUSTOM')) AND (@JobIsReset = 0)
						THEN '	INNER JOIN ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF +
							 '	ON (' + @SourceTargetPrimaryKeyColumns + ') AND ([SOURCE].[DWOperation] = ''D'') '

					/* If load is not incremental or JobIsReset is true then soft-delete if not exist in source table */
					WHEN (@JobLoadModeETL IN ('FULL','CUSTOM')) OR (@JobIsReset = 1)
						THEN '	WHERE NOT EXISTS ( ' + @CRLF +
							 '		SELECT 1 FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF + 
							 '		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
							 '	) '
				END + 'AND ([TARGET].[DWIsDeleted] = 0) AND ([TARGET].[DWIsCurrent] = 1) ' + @CRLF +
			'	SELECT @DeleteCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
		;

		/* Handle Updates from Source - Close existing records */
		SET @stmt = @stmt +
			'	/* Handle Updates from Source - Close existing records */ ' + @CRLF +
			'	INSERT INTO #ChangedRecords WITH (TABLOCKX) ' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWTrackingVersion], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [DWOperation], [DWValidFromDate], [DWValidToDate])' + @CRLF +
			'	SELECT	 ' + @TargetColumns + @CRLF +
			'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
			'	,	[DWTrackingVersion]	= [TARGET].[DWTrackingVersion] ' + @CRLF +
			'	,	[DWModifiedDate]	= ''' + @ValidFromDate + ''' ' + @CRLF +
			'	,	[DWIsDeleted]		= 0 ' + @CRLF +
			'	,	[DWIsCurrent]		= 0 ' + @CRLF +
			'	,	[DWOperation]		= ''U'' ' + @CRLF +
			'	,	[DWValidFromDate]	= [TARGET].[DWValidFromDate] ' + @CRLF +
			'	,	[DWValidToDate]		= ''' + @ValidToDate + ''' ' + @CRLF +
			'	FROM ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF + 
			'	INNER JOIN ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF + 
			'	ON (' + @SourceTargetPrimaryKeyColumns + ') AND ([SOURCE].[DWOperation] IN (''I'', ''U''))' + @CRLF +
			'	WHERE EXISTS (' + @CRLF +
			'		 SELECT ' + @SourceColumnsWithoutIncrementalField + @CRLF +
			'		 EXCEPT ' + @CRLF +  
			'		 SELECT ' + @TargetColumnsWithoutIncrementalField + @CRLF +
			'	)' + @CRLF +
			'	AND [SOURCE].[DWOperation] IN (''U'', ''I'') AND ([TARGET].[DWIsCurrent] = 1)' + @CRLF +
			'	SELECT @UpdateCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
		;

		/* Handle Updates from Source - Insert new version of records */
		SET @stmt = @stmt +
			'	/* Handle Updates from Source - Close existing records */ ' + @CRLF +
			'	INSERT INTO #ChangedRecords WITH (TABLOCKX) ' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWTrackingVersion], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [DWOperation], [DWValidFromDate], [DWValidToDate])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + @CRLF +
			'	,	[DWCreatedDate]		= ''' + @ValidFromDate + ''' ' + @CRLF +
			'	,	[DWTrackingVersion]	= [SOURCE].[DWTrackingVersion] ' + @CRLF +
			'	,	[DWModifiedDate]	= ''' + @ValidFromDate + ''' ' + @CRLF +
			'	,	[DWIsDeleted]		= 0 ' + @CRLF +
			'	,	[DWIsCurrent]		= 1 ' + @CRLF +
			'	,	[DWOperation]		= ''I'' ' + @CRLF +
			'	,	[DWValidFromDate]	= ''' + @ValidFromDate + ''' ' + @CRLF +
			'	,	[DWValidToDate]		= ''9999-12-31 23:59:59.000'' ' + @CRLF +
			'	FROM #ChangedRecords AS [TARGET]' + @CRLF + 
			'	INNER JOIN ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF + 
			'	ON (' + @SourceTargetPrimaryKeyColumns + ') AND ([SOURCE].[DWOperation] IN (''I'', ''U'')) AND ([TARGET].[DWOperation] = ''U'')' + @CRLF +
			'	SELECT @UpdateCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
		;

		/* Create Nonclustered Index on #ChangedRecords to optimize index scan instead of table scan */
		SET @stmt = @stmt +
			'	/* Create Nonclustered Index on #ChangedRecords to optimize index scan instead of table scan */ ' + @CRLF +
			'	CREATE NONCLUSTERED INDEX [NCI_ChangedRecords] ON #ChangedRecords (' + @PrimaryKeyColumns + ', [DWValidFromDate], [DWValidToDate]) ;' + @CRLF + @CRLF
		;

		/* Handle History Delete all changed records */
		SET @stmt = @stmt +
			'	/* Handle History Delete all changed records */ ' + @CRLF +
			'	DELETE [TARGET] WITH (TABLOCK) ' + @CRLF +
			'	FROM ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF + 
			'	INNER JOIN #ChangedRecords AS [SOURCE] ' + @CRLF + 
			'	ON (' + @SourceTargetPrimaryKeyColumns + ') AND ([SOURCE].[DWValidFromDate] = [TARGET].[DWValidFromDate]) AND ([SOURCE].[DWValidToDate] = [TARGET].[DWValidToDate])' + @CRLF + @CRLF
		;
	
		/* Handle History Insert Changed and updated records into History */
		SET @stmt = @stmt +
			'	/* Handle History Insert Changed and updated records into History */ ' + @CRLF +
			'	INSERT INTO ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [DWOperation], [DWValidFromDate], [DWValidToDate], [DWTrackingVersion])' + @CRLF +
			'	SELECT   ' + @SourceColumns + ', [SOURCE].[DWCreatedDate], [SOURCE].[DWModifiedDate], [SOURCE].[DWIsDeleted], [SOURCE].[DWIsCurrent], [SOURCE].[DWOperation], [SOURCE].[DWValidFromDate], [SOURCE].[DWValidToDate], [SOURCE].[DWTrackingVersion]' + @CRLF +
			'	FROM #ChangedRecords AS [SOURCE]' + @CRLF + @CRLF
		;

		/* Handle History Inserts from Source */
		SET @stmt = @stmt +
			'	/* Handle History Inserts from Source */ ' + @CRLF +
			'	INSERT INTO ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWTrackingVersion])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + ', ''' + @ValidFromDate + ''', ''' + @ValidFromDate + ''', [SOURCE].[DWTrackingVersion] ' + @CRLF +
			'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF +
			'	WHERE NOT EXISTS (SELECT 1 FROM ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET] WHERE (' + @SourceTargetPrimaryKeyColumns + ') )' + @CRLF +
			'	AND ([SOURCE].[DWOperation] IN (''I'', ''U'')) ' + @CRLF +
			'	SELECT @InsertCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
		;

		/* Load pattern with Insert, Update and Delete in History layer (SCD2) */
		IF (@emulation = 1) SELECT @stmt AS LoadSCD2Statement 
		IF (@emulation = 0) 
		BEGIN
			EXEC sys.sp_executesql @stmt, N'@UpdateCnt bigint OUTPUT, @InsertCnt bigint OUTPUT, @DeleteCnt bigint OUTPUT', @UpdateCnt OUTPUT, @InsertCnt OUTPUT, @DeleteCnt OUTPUT;
			
			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected inserts: ' + CONVERT(nvarchar, @InsertCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'INSERT', @rows = @InsertCnt;

			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected updates: ' + CONVERT(nvarchar, @UpdateCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'UPDATE', @rows = @updateCnt;

			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected deletes: ' + CONVERT(nvarchar, @deleteCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'DELETE', @rows = @deleteCnt;

			/* Rebuild/reorganize Columnstore Index */
			SET @stmt = (
				SELECT 'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(ix.IndexName) + ' ON ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (DROP_EXISTING = ON);'
				FROM (
					SELECT
						[IndexName]				=	i.name
					,	[IndexFragmentation]	=	100 * ISNULL(SUM(rg.deleted_rows), 0) / NULLIF(MAX(p.rows), 0)
					FROM sys.column_store_row_groups AS rg WITH (NOLOCK)
					JOIN sys.indexes AS i WITH (NOLOCK) ON  i.object_id = rg.object_id AND i.index_id = rg.index_id
					JOIN sys.partitions AS p WITH (NOLOCK) ON p.object_id = i.object_id
					WHERE i.type IN (5) -- 5 = CLUSTERED COLUMNSTORE INDEX.
					AND i.object_id = OBJECT_ID(@DWExtractHistorySchemaName + '.' + @DestinationTableName)
					GROUP BY i.name, i.type_desc
				) AS ix
				WHERE ix.IndexFragmentation > @IndexFragmentationLimit
			)
			SET @stmt = ISNULL(@stmt, '');
			EXEC sys.sp_executesql @stmt;

			/* Rebuild (Non)Clustered Index */
			SET @stmt = NULL;
			SELECT @stmt = ISNULL(@stmt, '') + 'ALTER INDEX ' + QUOTENAME(i.name) + ' ON ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' REBUILD WITH (ONLINE = OFF);' + CHAR(10)
			FROM sys.dm_db_index_physical_stats (DB_ID(), OBJECT_ID(@DWExtractHistorySchemaName + '.' + @DestinationTableName), NULL, NULL, NULL) AS ips
			JOIN sys.indexes AS i WITH (NOLOCK) ON (ips.object_id = i.object_id) AND (ips.index_id = i.index_id)
			WHERE (i.type IN (1, 2)) AND (ips.avg_fragmentation_in_percent > @IndexFragmentationLimit)
			
			SET @stmt = ISNULL(@stmt, '');
			EXEC sys.sp_executesql @stmt;

			/* Update statistics */
			IF (@stmt IS NOT NULL) AND (@stmt <> '')
			BEGIN
				SET @stmt = 'UPDATE STATISTICS ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';';
				EXEC sys.sp_executesql @stmt;
			END;
		END;

		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

		SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Failed to Load ' + QUOTENAME(@DWExtractHistorySchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' due to: ' + @CRLF + ERROR_MESSAGE();
		EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + @CRLF + @stmt;
		EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		SET @StopExecution = 1;
	END CATCH;

	/* If job is forced full load update to not run reset next load */
	IF (@StopExecution = 0) AND (@emulation = 0) 
	BEGIN						
		
		EXEC sys.sp_UpdateExtendedProperty 
			@level0type = N'SCHEMA', @level0name = @DestinationSchemaName
		,	@level1type = N'TABLE', @level1name = @DestinationTableName
		,	@name = N'IsReset', @value = 0;

	END;

	/* Select variable to be used in Data Factory */

	IF (@StopExecution = 0) AND (@emulation = 0) 
	BEGIN
		SET @Message = 
			'Load sequence ' + CAST(@LoadSequence AS NVARCHAR) + ' - ' + @LoadPattern + ' - ' + 
			'Log end load of: ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName)

		EXEC spLog 'ETL', @PackageName, 'Info', 3, @Message, @DestinationTableName;
	END;

END;