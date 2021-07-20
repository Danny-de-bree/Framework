CREATE PROCEDURE [meta].[spTransformLoadFact]

	@DestinationSchemaName NVARCHAR(255)
,	@DestinationTableName NVARCHAR(255)	
,	@PackageName NVARCHAR(255)			
,	@LoadSequence INT					
,	@Emulation TINYINT = 1
AS 
BEGIN
	
	DECLARE @CRLF VARCHAR(2) = CHAR(13) + CHAR(10);
	DECLARE @TaskName NVARCHAR(MAX);
	DECLARE @Message NVARCHAR(MAX);
	DECLARE @LoadPattern NVARCHAR(255);
	DECLARE @stmt NVARCHAR(MAX);
	DECLARE @DestinationColumns NVARCHAR(MAX) = '';
	DECLARE @TargetColumns NVARCHAR(MAX) = '';
	DECLARE @SourceColumns NVARCHAR(MAX) = '';
	DECLARE @TargetColumnsReverseNumbers NVARCHAR(MAX) = '';
	DECLARE @SourceColumnsReverseNumbers NVARCHAR(MAX) = '';
	DECLARE @TargetColumnsWithoutIncrementalField NVARCHAR(MAX) = '';
	DECLARE @SourceColumnsWithoutIncrementalField NVARCHAR(MAX) = '';
	DECLARE @SourceTargetPrimaryKeyColumns NVARCHAR(MAX) = '';
	DECLARE @PrimaryKeyColumns NVARCHAR(MAX) = '';
	DECLARE @SourceTargetLookupKeyColumns NVARCHAR(MAX) = '';

	DECLARE @CurrentDateTime NVARCHAR(30);
	DECLARE @SCD2ValidToDate NVARCHAR(30);

	DECLARE @DWDimensionPrefix NVARCHAR(255);
	DECLARE @DWFactPrefix NVARCHAR(255);
	DECLARE @DWTransformStagingSchemaName NVARCHAR(255);

	DECLARE @SourceObjectSchema NVARCHAR(255);
	DECLARE @SourceObjectName NVARCHAR(255);
	DECLARE @SourceObjectLookupKey NVARCHAR(255);
	DECLARE @IncrementalField NVARCHAR(255);

	DECLARE @SourceObjectColumnName NVARCHAR(255);
	DECLARE @SourceObjectColumnIsNumeric TINYINT;
	DECLARE @SourceObjectColumnIsPrimaryKey TINYINT;
	DECLARE @SourceObjectColumnIsLookupKey TINYINT;	
	DECLARE @SourceObjectColumnIsDeleted NVARCHAR(255) = '';

	DECLARE @JobLoadModeETL NVARCHAR(50);
	DECLARE @JobIsReset TINYINT;
	DECLARE @PreserveSCD2History TINYINT;
	DECLARE @IndexFragmentationLimit INT;
	DECLARE @StopExecution TINYINT = 0;

	DECLARE @UpdateCnt BIGINT = 0;
	DECLARE @InsertCnt BIGINT = 0;
	DECLARE @DeleteCnt BIGINT = 0;
	DECLARE @AltDeleteCnt BIGINT = 0;

	/* Prepare CurrentDateTime and SCD2ValidToDate parameters */
	SET @CurrentDateTime				= (SELECT CAST(CAST(GETUTCDATE() AS DATETIME2(3)) AS NVARCHAR));
	SET @SCD2ValidToDate				= (SELECT CAST(CAST(DATEADD(MS, -3, @CurrentDateTime) AS DATETIME2(3)) AS NVARCHAR));

	/* Prepare common Data Warehouse parameters */
	SELECT
		@DWTransformStagingSchemaName	= MAX(CASE WHEN [name] = 'DWTransformStagingSchemaName'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@IndexFragmentationLimit		= MAX(CASE WHEN [name] = 'IndexFragmentationLimit'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWDimensionPrefix				= MAX(CASE WHEN [name] = 'DWDimensionPrefix'				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWFactPrefix					= MAX(CASE WHEN [name] = 'DWFactPrefix'						THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0) 
	GROUP BY ep.major_id

	/* Prepare SourceObject parameters */
	SELECT
		@SourceObjectSchema				= MAX(CASE WHEN [name] = 'SourceObjectSchema'				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@SourceObjectName				= MAX(CASE WHEN [name] = 'SourceObjectName'					THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@SourceObjectLookupKey			= MAX(CASE WHEN [name] = 'SourceObjectLookupKey'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@IncrementalField				= MAX(CASE WHEN [name] = 'IncrementalField'					THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@JobLoadModeETL					= MAX(CASE WHEN [name] = 'LoadModeETL'						THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@JobIsReset						= MAX(CASE WHEN [name] = 'IsReset'							THEN CONVERT(TINYINT,		[value]) ELSE 0 END)
	,	@PreserveSCD2History			= MAX(CASE WHEN [name] = 'PreserveSCD2History'				THEN CONVERT(TINYINT,		[value]) ELSE 0 END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 1) AND (ep.major_id = OBJECT_ID(@DWTransformStagingSchemaName + '.' + @DestinationTableName))
	GROUP BY ep.major_id

	/* Are we using the correct meta stored procedure ? */
	IF (@DestinationSchemaName = @DWDimensionPrefix)
	BEGIN

		SET @Message = 'Error: unable to use ' + OBJECT_NAME(@@PROCID) + ' to load ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' due to invalid entity type.' ;
		PRINT @Message;
		IF(@Emulation = 0) THROW 51000, @Message, 1;
		RETURN -1;
	END;

	/* Detect load pattern based on SourceObject parameters */
	SET @LoadPattern = 
			CASE 
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'CT') THEN 'Change Tracking'
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'CDC') THEN 'Change Data Capture' 
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'ICL') THEN 'Incremental load'
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'FULL') THEN 'Full load'
				WHEN (@JobIsReset = 1) THEN 'Force full load'
			END
	
	/* Set taskname for the stored procedure */
	SET @TaskName = @LoadPattern + ' - Load Fact';

	/* If @JobIsReset and entity is a DW fact table (not CALC fact) clean-up deleted or invalid records */
	IF (@DestinationSchemaName = @DWFactPrefix)
	BEGIN TRY

		/* Prepare delete statement to clean the table for invalid records */
		SET @stmt =
			'	/* Prepare delete statement to clean the table for invalid records */ ' + @CRLF + 
			'	DELETE [TARGET] WITH (TABLOCKX)' + @CRLF +
			'	FROM '+ QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
			'	WHERE ([TARGET].[DWIsDeleted] = 1) OR ([TARGET].[DWIsCurrent] = 0)' + @CRLF +
			'	SELECT @DeleteCnt = @@ROWCOUNT ;'
		;

		/* Clean up deleted and invalid records */
		IF (@emulation = 1) SELECT @DestinationSchemaName AS DestinationSchemaName, @DestinationTableName AS DestinationTableName, @stmt AS SqlStatement;
		IF (@emulation = 0) AND (@JobIsReset = 1)
		BEGIN
			BEGIN TRANSACTION 

			EXEC sys.sp_executesql @stmt, N'@DeleteCnt bigint OUTPUT', @DeleteCnt OUTPUT;

			COMMIT TRANSACTION;

			SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Remove invalid records ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName);
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'DELETE', @rows = @deleteCnt;

		END;

	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Failed to remove invalid records ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' due to: ' + @CRLF + ERROR_MESSAGE();
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + @CRLF + @stmt;
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

	END CATCH;

	/* Prepare @SourceTargetLookupKeyColumns parameter to be used in load logic*/
	IF(@SourceObjectLookupKey <> '') SET @SourceTargetLookupKeyColumns = '([SOURCE].[LookupKeyPlaceHolder] = [TARGET].[PrimaryKeyPlaceHolder]) AND ([SOURCE].[PrimaryKeyPlaceHolder] != [TARGET].[PrimaryKeyPlaceHolder])';

	/* Generate column list used in main select and primary key columns in join statement */
	DECLARE cur CURSOR LOCAL FOR 
		SELECT
			[SourceObjectColumnName]			=	c.name
		,	[SourceObjectColumnIsNumeric]		=	CASE WHEN t.name IN ('decimal', 'numeric', 'smallmoney', 'money', 'float', 'real') THEN 1 ELSE 0 END
		,	[SourceObjectColumnIsPrimaryKey]	=	CASE WHEN (ic.column_id IS NOT NULL) THEN 1 ELSE 0 END
		,	[SourceObjectColumnIsLookupKey]		=	CASE WHEN (@SourceObjectLookupKey LIKE ('%' + c.name + '%')) THEN 1 ELSE 0 END
		FROM sys.columns AS c WITH (NOLOCK)
		INNER JOIN sys.types AS t WITH (NOLOCK) ON (c.user_type_id = t.user_type_id)
		LEFT JOIN sys.columns AS cx WITH (NOLOCK) ON (c.name = cx.name) AND (cx.object_id = OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName))
		LEFT JOIN sys.indexes AS i WITH (NOLOCK) ON (cx.object_id = i.object_id) AND (i.is_primary_key = 1)
		LEFT JOIN sys.index_columns AS ic WITH (NOLOCK) ON (i.index_id = ic.index_id) AND (i.object_id = ic.object_id) AND (cx.column_id = ic.column_id)
		WHERE (c.is_identity = 0) AND (c.object_id = OBJECT_ID(@DWTransformStagingSchemaName + '.' + @DestinationTableName)) AND (c.default_object_id = 0)
		ORDER BY c.column_id
	OPEN cur
	FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnIsNumeric, @SourceObjectColumnIsPrimaryKey, @SourceObjectColumnIsLookupKey
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

			/* Create source and target column variables to hold reverse postings */
			SET @TargetColumnsReverseNumbers = @TargetColumnsReverseNumbers + ', ';
			SET @SourceColumnsReverseNumbers = @SourceColumnsReverseNumbers	+ ', ';
		END;

		SET @DestinationColumns = @DestinationColumns + QUOTENAME(@SourceObjectColumnName);
		SET @SourceColumns = @SourceColumns + '[SOURCE].' + QUOTENAME(@SourceObjectColumnName);
		SET @TargetColumns = @TargetColumns + '[TARGET].' + QUOTENAME(@SourceObjectColumnName);

		/* Create source and target column variables for detecting changed records */
		IF(@SourceObjectColumnName <> @IncrementalField) SET @SourceColumnsWithoutIncrementalField = @SourceColumnsWithoutIncrementalField + '[SOURCE].' + QUOTENAME(@SourceObjectColumnName);
		IF(@SourceObjectColumnName <> @IncrementalField) SET @TargetColumnsWithoutIncrementalField = @TargetColumnsWithoutIncrementalField + '[TARGET].' + QUOTENAME(@SourceObjectColumnName);

		/* Create source and target column variables to hold reverse postings */
		SET @SourceColumnsReverseNumbers = @SourceColumnsReverseNumbers + IIF(@SourceObjectColumnIsNumeric = 1, '-', '') + '[SOURCE].' + QUOTENAME(@SourceObjectColumnName);
		SET @TargetColumnsReverseNumbers = @TargetColumnsReverseNumbers + IIF(@SourceObjectColumnIsNumeric = 1, '-', '') + '[TARGET].' + QUOTENAME(@SourceObjectColumnName);

		/* Create key columns join */
		IF(@SourceTargetPrimaryKeyColumns <> '') AND (@SourceObjectColumnIsPrimaryKey = 1)
		BEGIN
			SET @PrimaryKeyColumns = @PrimaryKeyColumns + ', ';
			SET @SourceTargetPrimaryKeyColumns = @SourceTargetPrimaryKeyColumns + ' AND ';
		END

		IF(@SourceObjectColumnIsPrimaryKey = 1) 
		BEGIN
			SET @PrimaryKeyColumns = @PrimaryKeyColumns + QUOTENAME(@SourceObjectColumnName);
			SET @SourceTargetPrimaryKeyColumns = @SourceTargetPrimaryKeyColumns + '([SOURCE].' + QUOTENAME(@SourceObjectColumnName) + ' = [TARGET].' + QUOTENAME(@SourceObjectColumnName) + ')';
		END

		IF(@SourceObjectColumnIsLookupKey = 1) OR (@SourceObjectColumnIsPrimaryKey = 1) 
		BEGIN
			IF(@SourceObjectColumnIsLookupKey = 1) SET @SourceTargetLookupKeyColumns = REPLACE(@SourceTargetLookupKeyColumns, 'LookupKeyPlaceHolder', @SourceObjectColumnName)
			IF(@SourceObjectColumnIsPrimaryKey = 1) SET @SourceTargetLookupKeyColumns = REPLACE(@SourceTargetLookupKeyColumns, 'PrimaryKeyPlaceHolder', @SourceObjectColumnName) 
		END; 

		/* Create variable to hold information if the source contains a IsDeleted marked */
		IF(@SourceObjectColumnName LIKE ('%IsDeleted%')) SET @SourceObjectColumnIsDeleted = '[SOURCE].' + QUOTENAME(@SourceObjectColumnName);

		FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnIsNumeric, @SourceObjectColumnIsPrimaryKey, @SourceObjectColumnIsLookupKey
	END
	CLOSE cur;
	DEALLOCATE cur;

	/* Do we have any changed records ? */
	IF (@Emulation = 0)
	BEGIN
		SET @stmt = 'SELECT @InsertCnt = COUNT(1) FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';'
		EXEC sys.sp_executesql @stmt, N'@InsertCnt bigint OUTPUT', @InsertCnt OUTPUT;

		IF (@InsertCnt = 0) SET @StopExecution = 1; 
	END

	/* Prepare load of EDW layer using soft-deletes and Delete/Insert pattern */
	IF (@SourceTargetPrimaryKeyColumns <> '') AND (@StopExecution = 0)
	BEGIN
		
		/* Should we preserve fact history ? if no then use standard load pattern */
		IF (@PreserveSCD2History = 0)
		BEGIN

			/* Create temp table to hold all Changed records */
			SET @stmt =
				'	/* Create temp table to hold all changed records */' + @CRLF +
				'	DROP TABLE IF EXISTS #ChangedRecords;' + @CRLF + @CRLF +
				'	/* Create temp table #ChangedRecords to hold all deleted/updated record */' + @CRLF +
				'	SELECT ' + @TargetColumns + ', [TARGET].[DWCreatedDate], [TARGET].[DWModifiedDate], [TARGET].[DWIsDeleted], [TARGET].[DWIsCurrent], [ETLOperation] = CAST(NULL AS NVARCHAR(10))' + @CRLF +
				'	INTO #ChangedRecords' + @CRLF + 
				'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
				'	WHERE 1 = 0' + @CRLF + @CRLF
			;

			/* Handle Deletes from Source - Mark deleted records flag */
			SET @stmt = @stmt +
				'	/* Detect which records that has been deleted in source */' + @CRLF +
				'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
				'	SELECT	 ' + REPLACE(@TargetColumns, REPLACE(@SourceObjectColumnIsDeleted, '[SOURCE]', '[TARGET]'), 1) + @CRLF +
				'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
				'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWIsDeleted]		= 1 ' + @CRLF +
				'	,	[DWIsCurrent]		= 0 ' + @CRLF +
				'	,	[ETLOperation]		= ''D'' ' + @CRLF +
				'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
					CASE
						/* If load is incremental and JobIsReset is false then soft-delete if when exist in source table */
						WHEN (@JobLoadModeETL NOT IN ('FULL','CUSTOM')) AND (@JobIsReset = 0) AND (@SourceObjectColumnIsDeleted <> '')
							THEN '	WHERE EXISTS ( ' + @CRLF +
								 '		SELECT 1 FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF +
								 '		WHERE (' + @SourceTargetPrimaryKeyColumns + ') AND (' + @SourceObjectColumnIsDeleted + ' = 1)' + @CRLF +
								 '	)' + @CRLF

						/* If load is not incremental or JobIsReset is true then soft-delete if not exist in source table */
						WHEN (@JobLoadModeETL IN ('FULL','CUSTOM')) OR (@JobIsReset = 1)
							THEN '	WHERE NOT EXISTS ( ' + @CRLF +
								 '		SELECT 1 FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF + 
								 '		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + IIF(@SourceObjectColumnIsDeleted <> '', ' AND (' + @SourceObjectColumnIsDeleted + ' = 0)', '') + @CRLF +
								 '	)' + @CRLF
					END +
				'	AND ([TARGET].[DWIsCurrent] = 1)' + @CRLF +
				'	SELECT @DeleteCnt = @@ROWCOUNT ;' + @CRLF + @CRLF +
					CASE 
						/* We should only check transform view if the load is incremental - as all data already is loaded into the Staging table */
						WHEN (@JobLoadModeETL NOT IN ('FULL','CUSTOM')) AND (@JobIsReset = 0) AND (@SourceTargetLookupKeyColumns <> '')
							THEN '	/* Handle Deletes from Source - hard delete records which does not exits in transformation logic (changing primary keys) */' + @CRLF +
								 '	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
								 '			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
								 '	SELECT	 ' + REPLACE(@TargetColumns, REPLACE(@SourceObjectColumnIsDeleted, '[SOURCE]', '[TARGET]'), 1) + @CRLF +
								 '	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
								 '	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
								 '	,	[DWIsDeleted]		= 1 ' + @CRLF +
								 '	,	[DWIsCurrent]		= 0 ' + @CRLF +
								 '	,	[ETLOperation]		= ''Dx'' ' + @CRLF +
								 '	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF + 
								 '	WHERE EXISTS (' + @CRLF +
								 '		SELECT 1 FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF + 
								 '		WHERE (' + @SourceTargetLookupKeyColumns + ')' + @CRLF +
								 '	) AND ([TARGET].[DWIsCurrent] = 1)' + @CRLF +
								 '	SELECT @AltDeleteCnt = @@ROWCOUNT;' + @CRLF + @CRLF
						ELSE '	/* No lookupKey found */' + @CRLF +
							 '	SELECT @AltDeleteCnt = 0' + @CRLF + @CRLF
					END ;
			
			/* Are we able to locate a deleted flag '%IsDeleted' in the transformation logic ? as @SourceObjectColumnIsDeleted will return null  */
			IF (@stmt IS NULL)
			BEGIN
				SET @stmt = @stmt +
					'	/* Handle Deletes from Source - Mark deleted records flag */ ' + @CRLF +
					'	SELECT @DeleteCnt = 0 ;' + @CRLF +
					'	SELECT @AltDeleteCnt = 0 ;' + @CRLF + @CRLF
					;
			END;

			/* Detect which records that has been Updated in source */
			SET @stmt = @stmt +
				'	/* Create temp table to hold all updated records */' + @CRLF +
				'	DROP TABLE IF EXISTS #UpdatedRecords;' + @CRLF + @CRLF +
				'	/* Detect which records that has been Updated in source */' + @CRLF +
				'	SELECT ' + @SourceColumns + @CRLF +
				'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
				'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWIsDeleted]		= 0 ' + @CRLF +
				'	,	[DWIsCurrent]		= 1 ' + @CRLF +
				'	,	[ETLOperation]		= ''U'' ' + @CRLF +
				'	INTO #UpdatedRecords ' + @CRLF +
				'	FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF +
				'	INNER JOIN ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
				'	ON (' + @SourceTargetPrimaryKeyColumns + ')' + IIF(@SourceObjectColumnIsDeleted <> '', ' AND (' + @SourceObjectColumnIsDeleted + ' = 0)', '') + @CRLF +
				'	WHERE EXISTS (' + @CRLF +
				'		SELECT ' + @SourceColumnsWithoutIncrementalField + @CRLF +
				'		EXCEPT ' + @CRLF +  
				'		SELECT ' + @TargetColumnsWithoutIncrementalField + @CRLF +
				'	)' + @CRLF +
				--'	AND ([TARGET].[DWIsCurrent] = 1)' + @CRLF +
				'	SELECT @UpdateCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
			;

			/* Detect if deleted records is actually a updated record */
			SET @stmt = @stmt +
				'	/* Detect if deleted records is actually a updated record */' + @CRLF +
				'	DELETE [TARGET] WITH (TABLOCKX)' + @CRLF +
				'	FROM #ChangedRecords AS [TARGET]' + @CRLF + 
				'	WHERE EXISTS ( ' + @CRLF +
				'		SELECT 1 FROM #UpdatedRecords AS [SOURCE]' + @CRLF + 
				'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
				'	)' + @CRLF + @CRLF
			;

			/* Insert updated records */
			SET @stmt = @stmt + 
				'	/* Insert updated records into temp table #ChangeRecords */' + @CRLF +
				'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
				'	SELECT	 ' + @SourceColumns + ', [SOURCE].[DWCreatedDate], [SOURCE].[DWModifiedDate], [SOURCE].[DWIsDeleted], [SOURCE].[DWIsCurrent], [SOURCE].[ETLOperation]' + @CRLF +
				'	FROM #UpdatedRecords AS [SOURCE]' + @CRLF +
				'	WHERE NOT EXISTS (' + @CRLF +
				'		SELECT 1 FROM #ChangedRecords AS [TARGET]' + @CRLF +
				'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
				'	)' + @CRLF + @CRLF

			/* Create Nonclustered Index on #ChangedRecords to optimize index scan instead of table scan */
			SET @stmt = @stmt +
				'	/* Create Nonclustered Index on #ChangedRecords to optimize index scan instead of table scan */ ' + @CRLF +
				'	CREATE NONCLUSTERED INDEX [NCI_ChangedRecords] ON #ChangedRecords (' + @PrimaryKeyColumns + ') ;' + @CRLF + @CRLF
			;

			/* Delete records which should deleted or updated */ 
			SET @stmt = @stmt +
				'	/* Delete records which should updated */' + @CRLF +
				'	DELETE [TARGET] WITH (TABLOCKX)' + @CRLF +
				'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
				'	WHERE EXISTS (' + @CRLF +
				'		SELECT 1 FROM #ChangedRecords AS [SOURCE]' + @CRLF +
				'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
				'	)' + @CRLF + @CRLF
			;

			/* Insert records which have been updated */ 
			SET @stmt = @stmt +
				'	/* Insert records which have been updated */' + @CRLF + 
				'	INSERT INTO ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent])' + @CRLF +
				'	SELECT	 ' + @SourceColumns + ', [SOURCE].[DWCreatedDate], [SOURCE].[DWModifiedDate], [SOURCE].[DWIsDeleted], [SOURCE].[DWIsCurrent]' + @CRLF +
				'	FROM #ChangedRecords AS [SOURCE]' + @CRLF +
				'	WHERE NOT EXISTS (' + @CRLF +
				'		SELECT 1 FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
				'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
				'	)' + @CRLF + @CRLF
			;

			/* Handle Inserts from Source - Insert new and changed records */
			SET @stmt = @stmt +
				'	/* Handle Inserts from Source - Insert new records */ ' + @CRLF +
				'	INSERT INTO ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate])' + @CRLF +
				'	SELECT	 ' + @SourceColumns + ', ''' + @CurrentDateTime + ''', ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF +
				'	WHERE NOT EXISTS (' + @CRLF +
				'		SELECT 1 FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
				'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
				'	)' + 
					CASE /* Do we have IsDeleted flag in the transformation logic */
						WHEN @SourceObjectColumnIsDeleted <> '' THEN ' AND (' + @SourceObjectColumnIsDeleted + ' = 0)'
						ELSE ''
					END + @CRLF +
				'	SELECT @InsertCnt = @@ROWCOUNT ;' + @CRLF + @CRLF;
		END;

		/* Should we preserve fact history ? if Yes then use an Accumulated History Fact */
		IF (@PreserveSCD2History = 1)
		BEGIN

			/* Create temp table to hold all Changed records */
			SET @stmt =
				'	/* Create temp table to hold all changed records */' + @CRLF +
				'	DROP TABLE IF EXISTS #ChangedRecords;' + @CRLF + @CRLF +
				'	/* Create temp table #ChangedRecords to hold all deleted/updated record */' + @CRLF +
				'	SELECT ' + @TargetColumns + ', [TARGET].[DWCreatedDate], [TARGET].[DWModifiedDate], [TARGET].[DWValidFromDate], [TARGET].[DWIsDeleted], [TARGET].[DWIsCurrent], [ETLOperation] = CAST(NULL AS NVARCHAR(10))' + @CRLF +
				'	INTO #ChangedRecords' + @CRLF + 
				'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
				'	WHERE 1 = 0' + @CRLF + @CRLF
			;

			/* Handle Deletes from Source - Mark deleted records flag */
			SET @stmt = @stmt +
				'	/* Detect which records that has been deleted in source */' + @CRLF +
				'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWValidFromDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
				'	SELECT	 ' + @TargetColumnsReverseNumbers + @CRLF +
				'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
				'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWValidFromDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWIsDeleted]		= 1 ' + @CRLF +
				'	,	[DWIsCurrent]		= 0 ' + @CRLF +
				'	,	[ETLOperation]		= ''D'' ' + @CRLF +
				'	FROM (' + @CRLF +
				'		INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
				'				(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWValidFromDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
				'		OUTPUT INSERTED.*' + @CRLF + 
				'		SELECT ' + @TargetColumns + @CRLF +
				'		,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
				'		,	[DWModifiedDate]	= [TARGET].[DWModifiedDate] ' + @CRLF +
				'		,	[DWValidFromDate]	= [TARGET].[DWValidFromDate] ' + @CRLF +
				'		,	[DWIsDeleted]		= 1 ' + @CRLF +
				'		,	[DWIsCurrent]		= 0 ' + @CRLF +
				'		,	[ETLOperation]		= ''D'' ' + @CRLF +
				'		FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
					CASE
						/* If load is incremental and JobIsReset is false then soft-delete if when exist in source table */
						WHEN (@JobLoadModeETL NOT IN ('FULL','CUSTOM')) AND (@JobIsReset = 0) AND (@SourceObjectColumnIsDeleted <> '')
							THEN '		WHERE EXISTS ( ' + @CRLF +
								 '			SELECT 1 FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF +
								 '			WHERE (' + @SourceTargetPrimaryKeyColumns + ') AND (' + @SourceObjectColumnIsDeleted + ' = 1)' + @CRLF +
								 '		)' + @CRLF

						/* If load is not incremental or JobIsReset is true then soft-delete if not exist in source table */
						WHEN (@JobLoadModeETL IN ('FULL','CUSTOM')) OR (@JobIsReset = 1)
							THEN '		WHERE NOT EXISTS ( ' + @CRLF +
								 '			SELECT 1 FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF + 
								 '			WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + IIF(@SourceObjectColumnIsDeleted <> '', ' AND (' + @SourceObjectColumnIsDeleted + ' = 0)', '') + @CRLF +
								 '		)' + @CRLF
					END +
				'		AND ([TARGET].[DWIsCurrent] = 1)' + @CRLF + 
				'	) AS [TARGET] ' + @CRLF + 
				'	SELECT @DeleteCnt = @@ROWCOUNT ;' + @CRLF + @CRLF +

				CASE 
					/* We should only check transform view if the load is incremental - as all data already is loaded into the Staging table */
					WHEN (@JobLoadModeETL NOT IN ('FULL','CUSTOM')) AND (@JobIsReset = 0) AND (@SourceTargetLookupKeyColumns <> '')
						THEN
							'	/* Handle Deletes from Source - hard delete records which does not exits in transformation logic (changing primary keys) */' + @CRLF +
							'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
							'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWValidFromDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
							'	SELECT	 ' + @TargetColumnsReverseNumbers + @CRLF +
							'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
							'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
							'	,	[DWValidFromDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
							'	,	[DWIsDeleted]		= 1 ' + @CRLF +
							'	,	[DWIsCurrent]		= 0 ' + @CRLF +
							'	,	[ETLOperation]		= ''Dx'' ' + @CRLF +
							'	FROM (' + @CRLF +
							'		INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
							'				(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWValidFromDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
							'		OUTPUT INSERTED.*' + @CRLF + 
							'		SELECT ' + @TargetColumns + @CRLF +
							'		,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
							'		,	[DWModifiedDate]	= [TARGET].[DWModifiedDate] ' + @CRLF +
							'		,	[DWValidFromDate]	= [TARGET].[DWValidFromDate] ' + @CRLF +
							'		,	[DWIsDeleted]		= 1 ' + @CRLF +
							'		,	[DWIsCurrent]		= 0 ' + @CRLF +
							'		,	[ETLOperation]		= ''Dx'' ' + @CRLF +
							'		FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
							'		WHERE EXISTS (' + @CRLF +
							'			SELECT 1 FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF + 
							'			WHERE (' + @SourceTargetLookupKeyColumns + ')' + @CRLF +
							'		) AND ([TARGET].[DWIsCurrent] = 1)' + @CRLF +
							'	) AS [TARGET] ' + @CRLF + 
							'	SELECT @AltDeleteCnt = @@ROWCOUNT;' + @CRLF + @CRLF
					ELSE '	/* No lookupKey found */' + @CRLF +
						 '	SELECT @AltDeleteCnt = 0' + @CRLF + @CRLF
				END ;

			/* Detect which records that has been Updated in source */
			SET @stmt = @stmt +
				'	/* Create temp table to hold all updated records */' + @CRLF +
				'	DROP TABLE IF EXISTS #UpdatedRecords;' + @CRLF + @CRLF +
				'	/* Detect which records that has been Updated in source */' + @CRLF +
				'	SELECT ' + @TargetColumns + @CRLF +
				'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
				'	,	[DWModifiedDate]	= [TARGET].[DWModifiedDate] ' + @CRLF +
				'	,	[DWValidFromDate]	= [TARGET].[DWValidFromDate] ' + @CRLF +
				'	,	[DWIsDeleted]		= 0 ' + @CRLF +
				'	,	[DWIsCurrent]		= 0 ' + @CRLF +
				'	,	[ETLOperation]		= ''U'' ' + @CRLF +
				'	INTO #UpdatedRecords ' + @CRLF +
				'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET] ' + @CRLF +
				'	INNER JOIN ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF +
				'	ON (' + @SourceTargetPrimaryKeyColumns + ')' + IIF(@SourceObjectColumnIsDeleted <> '', ' AND (' + @SourceObjectColumnIsDeleted + ' = 0)', '') + @CRLF +
				'	WHERE EXISTS (' + @CRLF +
				'		SELECT ' + @SourceColumnsWithoutIncrementalField + @CRLF +
				'		EXCEPT ' + @CRLF +  
				'		SELECT ' + @TargetColumnsWithoutIncrementalField + @CRLF +
				'	) AND ([TARGET].[DWIsCurrent] = 1)' + @CRLF +
				'	SELECT @UpdateCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
			;

			/* Insert reverse posting of updated fact-record */
			SET @stmt = @stmt +
				'	/* Insert reverse posting of updated fact-record */' + @CRLF +
				'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWValidFromDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
				'	SELECT ' + @SourceColumnsReverseNumbers + @CRLF +
				'	,	[DWCreatedDate]		= [SOURCE].[DWCreatedDate] ' + @CRLF +
				'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWValidFromDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWIsDeleted]		= 0 ' + @CRLF +
				'	,	[DWIsCurrent]		= 0 ' + @CRLF +
				'	,	[ETLOperation]		= ''U'' ' + @CRLF +
				'	FROM #UpdatedRecords AS [SOURCE]' + @CRLF + @CRLF
			;

			/* Insert New posting of updated fact-record from Staging */
			SET @stmt = @stmt +
				'	/* Insert New posting of updated fact-record from Staging */' + @CRLF +
				'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWValidFromDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
				'	SELECT ' + @SourceColumns + @CRLF +
				'	,	[DWCreatedDate]		= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWValidFromDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWIsDeleted]		= 0 ' + @CRLF +
				'	,	[DWIsCurrent]		= 1 ' + @CRLF +
				'	,	[ETLOperation]		= ''I'' ' + @CRLF +
				'	FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF +
				'	WHERE EXISTS (' + @CRLF + 
				'		SELECT 1 FROM #ChangedRecords AS [TARGET]' + @CRLF + 
				'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
				'	)'+ @CRLF + @CRLF
			;

			/* Insert original fact record with update DWIsCurrent flag */
			SET @stmt = @stmt +
				'	/* Insert original fact record with update DWIsCurrent flag */' + @CRLF +
				'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWValidFromDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
				'	SELECT ' + @SourceColumns + @CRLF +
				'	,	[DWCreatedDate]		= [SOURCE].[DWCreatedDate] ' + @CRLF +
				'	,	[DWModifiedDate]	= [SOURCE].[DWModifiedDate] ' + @CRLF +
				'	,	[DWValidFromDate]	= [SOURCE].[DWValidFromDate] ' + @CRLF +
				'	,	[DWIsDeleted]		= [SOURCE].[DWIsDeleted] ' + @CRLF +
				'	,	[DWIsCurrent]		= [SOURCE].[DWIsCurrent]' + @CRLF +
				'	,	[ETLOperation]		= [SOURCE].[ETLOperation] ' + @CRLF +
				'	FROM #UpdatedRecords AS [SOURCE]' + @CRLF + @CRLF
			;

			/* Handle new records from Staging which does not exists in the fact table */
			SET @stmt = @stmt +
				'	/* Handle new records from Staging which does not exists in the fact table */' + @CRLF +
				'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWValidFromDate], [DWIsDeleted], [DWIsCurrent], [ETLOperation])' + @CRLF +
				'	SELECT ' + @SourceColumns + @CRLF +
				'	,	[DWCreatedDate]		= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWValidFromDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWIsDeleted]		= 0 ' + @CRLF +
				'	,	[DWIsCurrent]		= 1 ' + @CRLF +
				'	,	[ETLOperation]		= ''I'' ' + @CRLF +
				'	FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF +
				'	WHERE NOT EXISTS (' + @CRLF + 
				'		SELECT 1 FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF + 
				'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
				'	)' + @CRLF +
				'	SELECT @InsertCnt = @@ROWCOUNT ;' + @CRLF + @CRLF

			/* Create Nonclustered Index on #ChangedRecords to optimize index scan instead of table scan */
			SET @stmt = @stmt +
				'	/* Create Nonclustered Index on #ChangedRecords to optimize index scan instead of table scan */ ' + @CRLF +
				'	CREATE NONCLUSTERED INDEX [NCI_ChangedRecords] ON #ChangedRecords (' + @PrimaryKeyColumns + ', [DWCreatedDate], [DWModifiedDate]) ;' + @CRLF + @CRLF
			;

			/* Remove fact records which has been updated though change detection */
			SET @stmt = @stmt +
				'	/* Remove fact records which has been updated though change detection */ ' + @CRLF +
				'	DELETE [TARGET] WITH (TABLOCK) ' + @CRLF +
				'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET] ' + @CRLF +
				'	WHERE EXISTS ( ' + @CRLF +
				'		SELECT 1 FROM #ChangedRecords AS [SOURCE] ' + @CRLF + 
				'		WHERE (' + @SourceTargetPrimaryKeyColumns + ' AND ([SOURCE].[DWCreatedDate] = [TARGET].[DWCreatedDate]) AND ([SOURCE].[DWModifiedDate] = [TARGET].[DWModifiedDate])) ' + @CRLF +
				'	); ' + @CRLF + @CRLF +
					
				'	/* Insert new and Changed records into the fact table */ ' + @CRLF +
				'	INSERT INTO ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCK) ' + @CRLF +
				'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate], [DWValidFromDate], [DWIsDeleted], [DWIsCurrent])' + @CRLF +
				'	SELECT ' + @SourceColumns + @CRLF +
				'	,	[DWCreatedDate]		= [SOURCE].[DWCreatedDate] ' + @CRLF +
				'	,	[DWModifiedDate]	= [SOURCE].[DWModifiedDate] ' + @CRLF +
				'	,	[DWValidFromDate]	= [SOURCE].[DWValidFromDate] ' + @CRLF +
				'	,	[DWIsDeleted]		= [SOURCE].[DWIsDeleted] ' + @CRLF +
				'	,	[DWIsCurrent]		= [SOURCE].[DWIsCurrent]' + @CRLF +
				'	FROM #ChangedRecords AS [SOURCE]' + @CRLF + @CRLF
				;
		END;


	END;

	IF (@SourceTargetPrimaryKeyColumns = '') AND (@StopExecution = 0)
	BEGIN
		
		/* If no primary key exists we are forced to truncate full-load the entity */
		SET @stmt = 
			'	/* Truncate EDW table */ ' + @CRLF +
			'	TRUNCATE TABLE ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';' + @CRLF + @CRLF +
			'	INSERT INTO ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + ', ''' + @CurrentDateTime + ''', ''' + @CurrentDateTime + ''' ' + @CRLF +
			'	FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF +
				CASE /* Do we have IsDeleted flag in the transformation logic */
					WHEN @SourceObjectColumnIsDeleted <> '' THEN ' WHERE (' + @SourceObjectColumnIsDeleted + ' = 0)'
					ELSE ''
				END + ';' + @CRLF +
			'	SELECT @InsertCnt = @@ROWCOUNT;' + @CRLF +
			'	SELECT @UpdateCnt = 0 ;' + @CRLF +
			'	SELECT @DeleteCnt = 0 ;' + @CRLF +
			'	SELECT @AltDeleteCnt = 0;' + @CRLF + @CRLF;
	END;

	/* Prepare execute sql statement */
	IF (@StopExecution = 0)
	BEGIN TRY

		/* Delta load pattern with Insert, Update and Delete */
		IF (@emulation = 1) SELECT @DestinationSchemaName AS DestinationSchemaName, @DestinationTableName AS DestinationTableName, @stmt AS SqlStatement;
		IF (@emulation = 0)
		BEGIN
			BEGIN TRANSACTION 

			EXEC sys.sp_executesql @stmt, N'@UpdateCnt bigint OUTPUT, @InsertCnt bigint OUTPUT, @DeleteCnt bigint OUTPUT, @AltDeleteCnt bigint OUTPUT', @UpdateCnt OUTPUT, @InsertCnt OUTPUT, @DeleteCnt OUTPUT, @AltDeleteCnt OUTPUT;

			/* If job is reset update metadata that next load should be incremental */
			IF (@JobIsReset = 1) EXEC sys.sp_UpdateExtendedProperty @level0type = N'SCHEMA', @level0name = @DWTransformStagingSchemaName, @level1type = N'TABLE', @level1name = @DestinationTableName, @name = N'IsReset', @value = 0;	
	
			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected inserts: ' + CONVERT(nvarchar, @InsertCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'INSERT', @rows = @InsertCnt;

			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected updates: ' + CONVERT(nvarchar, @UpdateCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'UPDATE', @rows = @updateCnt;

			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected deletes: ' + CONVERT(nvarchar, @deleteCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'DELETE', @rows = @deleteCnt;

			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected Alternative deletes: ' + CONVERT(nvarchar, @AltDeleteCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'ALDELETE', @rows = @AltDeleteCnt;

			COMMIT TRANSACTION;
		END;

	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

		SET @StopExecution = 1;

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Failed to Merge into ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' due to: ' + @CRLF + ERROR_MESSAGE();
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + @CRLF + @stmt;
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		/* Break Azure Data Factory! */
		SELECT 1/0
	END CATCH;

	IF (@StopExecution = 0)
	BEGIN TRY

		/* Rebuild/reorganize Columnstore Index */
		SET @stmt = (
			SELECT 'CREATE CLUSTERED COLUMNSTORE INDEX ' + QUOTENAME(ix.IndexName) + ' ON ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (DROP_EXISTING = ON);'
			FROM (
				SELECT
					[IndexName]				=	i.name
				,	[IndexFragmentation]	=	100 * ISNULL(SUM(rg.deleted_rows), 0) / NULLIF(MAX(p.rows), 0)
				FROM sys.column_store_row_groups AS rg WITH (NOLOCK)
				JOIN sys.indexes AS i WITH (NOLOCK) ON  i.object_id = rg.object_id AND i.index_id = rg.index_id
				JOIN sys.partitions AS p WITH (NOLOCK) ON p.object_id = i.object_id
				WHERE i.type IN (5) /* 5 = CLUSTERED COLUMNSTORE INDEX. */
				AND i.object_id = OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName)
				GROUP BY i.name, i.type_desc
			) AS ix
			WHERE ix.IndexFragmentation > @IndexFragmentationLimit
		);

		SET @stmt = ISNULL(@stmt, '');
		IF (@emulation = 0) EXEC sys.sp_executesql @stmt;

		/* Update statistics */
		SET @stmt = 'UPDATE STATISTICS ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';';
		IF (@emulation = 0) EXEC sys.sp_executesql @stmt;

	END TRY
	BEGIN CATCH
		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Failed to rebuild index/Update statistics on ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' due to: ' + @CRLF + ERROR_MESSAGE();
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;
	END CATCH;

	IF (@Emulation = 0) AND (OBJECT_ID('dbo.BusinessObjectExecutionPlan') IS NOT NULL)
	BEGIN
		/* Update dbo.BusinessObjectExecutionPlan set ExecutionStatus = 'Finished' */
		UPDATE [TARGET] WITH (TABLOCKX) SET 
			[TARGET].[ExecutionStatusCode] = 0
		FROM dbo.BusinessObjectExecutionPlan AS [TARGET]
		WHERE ([TARGET].[PrecedenceObjectSchema] = @SourceObjectSchema) AND ([TARGET].[PrecedenceObjectName] = @SourceObjectName)
	END;

END;