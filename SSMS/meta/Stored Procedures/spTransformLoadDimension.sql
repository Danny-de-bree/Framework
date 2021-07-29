CREATE PROCEDURE [meta].[spTransformLoadDimension]

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

	DECLARE @DestinationIdentityColumn NVARCHAR(MAX) = '';
	DECLARE @CastIdentityColumn NVARCHAR(MAX) = '';
	DECLARE @SourceIdentityColumn NVARCHAR(MAX) = '';
	DECLARE @TargetIdentityColumn NVARCHAR(MAX) = '';
	DECLARE @DestinationTargetIdentityColumn NVARCHAR(MAX) = '';
	DECLARE @LatestSurrogateKeyValue NVARCHAR(255); 

	DECLARE @TargetColumnsWithoutIncrementalField NVARCHAR(MAX) = '';
	DECLARE @SourceColumnsWithoutIncrementalField NVARCHAR(MAX) = '';
	DECLARE @SourceTargetPrimaryKeyColumns NVARCHAR(MAX) = '';
	DECLARE @PrimaryKeyColumns NVARCHAR(MAX) = '';

	DECLARE @CurrentDateTime NVARCHAR(30);
	DECLARE @SCD2ValidToDate NVARCHAR(30);

	DECLARE @DWDimensionPrefix NVARCHAR(255);
	DECLARE @DWTransformStagingSchemaName NVARCHAR(255);

	DECLARE @SourceObjectSchema NVARCHAR(255);
	DECLARE @SourceObjectName NVARCHAR(255);
	DECLARE @IncrementalField NVARCHAR(255);

	DECLARE @SourceObjectColumnName NVARCHAR(255);
	DECLARE @SourceObjectColumnIsPrimaryKey TINYINT;
	DECLARE @SourceObjectColumnIsIdentity TINYINT;
	DECLARE @SourceObjectColumnIsDeleted NVARCHAR(255) = '';

	DECLARE @JobLoadModeETL NVARCHAR(50);
	DECLARE @JobIsReset TINYINT;
	DECLARE @PreserveSCD2History TINYINT;
	DECLARE @IndexFragmentationLimit INT;
	DECLARE @StopExecution TINYINT = 0;

	DECLARE @UpdateCnt BIGINT = 0;
	DECLARE @InsertCnt BIGINT = 0;
	DECLARE @DeleteCnt BIGINT = 0;

	/* Prepare validfrom and validto parameters */
	SET @CurrentDateTime				= CAST(CAST(GETUTCDATE() AS DATETIME2(3)) AS NVARCHAR(30));
	SET @SCD2ValidToDate				= CAST(CAST(DATEADD(MS, -3, @CurrentDateTime) AS DATETIME2(3)) AS NVARCHAR(30));

	/* Prepare common Data Warehouse parameters */
	SELECT
		@DWTransformStagingSchemaName	= MAX(CASE WHEN [name] = 'DWTransformStagingSchemaName'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@IndexFragmentationLimit		= MAX(CASE WHEN [name] = 'IndexFragmentationLimit'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWDimensionPrefix				= MAX(CASE WHEN [name] = 'DWDimensionPrefix'				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0) 
	GROUP BY ep.major_id

	/* Prepare SourceObject parameters */
	SELECT
		@SourceObjectSchema				= MAX(CASE WHEN [name] = 'SourceObjectSchema'				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@SourceObjectName				= MAX(CASE WHEN [name] = 'SourceObjectName'					THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@IncrementalField				= MAX(CASE WHEN [name] = 'IncrementalField'					THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@JobLoadModeETL					= MAX(CASE WHEN [name] = 'LoadModeETL'						THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@JobIsReset						= MAX(CASE WHEN [name] = 'IsReset'							THEN CONVERT(TINYINT,		[value]) ELSE 0 END)
	,	@PreserveSCD2History			= MAX(CASE WHEN [name] = 'PreserveSCD2History'				THEN CONVERT(TINYINT,		[value]) ELSE 0 END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 1) AND (ep.major_id = OBJECT_ID(@DWTransformStagingSchemaName + '.' + @DestinationTableName))
	GROUP BY ep.major_id

	/* Are we using the correct meta stored procedure ? */
	IF (@DestinationSchemaName != @DWDimensionPrefix)
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

	/* Retrive the latest Surrogate key value for the dimension */
	SET @LatestSurrogateKeyValue = ISNULL(IDENT_CURRENT('' + @DestinationSchemaName + '.' + @DestinationTableName + ''), 0);

	/* Generate column list used in main select and primary key columns in join statement */
	DECLARE cur CURSOR LOCAL FOR 
		SELECT
			[SourceObjectColumnName]			=	c.name
		,	[SourceObjectColumnIsIdentity]		=	c.is_identity
		,	[SourceObjectColumnIsPrimaryKey]	=	CASE WHEN (ic.column_id IS NOT NULL) THEN 1 ELSE 0 END
		FROM sys.columns AS c WITH (NOLOCK)
		LEFT JOIN sys.columns AS cx WITH (NOLOCK) ON (c.name = cx.name) AND (cx.object_id = OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName))
		LEFT JOIN sys.indexes AS i WITH (NOLOCK) ON (cx.object_id = i.object_id) AND (i.is_primary_key = 1)
		LEFT JOIN sys.index_columns AS ic WITH (NOLOCK) ON (i.index_id = ic.index_id) AND (i.object_id = ic.object_id) AND (cx.column_id = ic.column_id)
		WHERE (c.object_id = OBJECT_ID(@DWTransformStagingSchemaName + '.' + @DestinationTableName)) AND (c.name NOT IN ('DWCreatedDate'))
		ORDER BY c.column_id
	OPEN cur
	FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnIsIdentity, @SourceObjectColumnIsPrimaryKey
	WHILE @@FETCH_STATUS = 0
	BEGIN

		IF (@SourceObjectColumnIsIdentity = 1)
		BEGIN

			SET @DestinationIdentityColumn = @DestinationIdentityColumn + ', ' + QUOTENAME(@SourceObjectColumnName);
			SET @SourceIdentityColumn = @SourceIdentityColumn + ', [SOURCE].' + QUOTENAME(@SourceObjectColumnName);
			SET @TargetIdentityColumn = @TargetIdentityColumn + ', [TARGET].' + QUOTENAME(@SourceObjectColumnName);
			SET @DestinationTargetIdentityColumn = @DestinationTargetIdentityColumn + CHAR(9) + ',' + CHAR(9) + QUOTENAME(@SourceObjectColumnName) + ' = ' + '[TARGET].' + QUOTENAME(@SourceObjectColumnName);
			SET @CastIdentityColumn = @CastIdentityColumn + ', ' + QUOTENAME(@SourceObjectColumnName) + ' = CAST(NULL AS BIGINT)';
		END;

		IF (@SourceObjectColumnIsIdentity = 0)
		BEGIN
			IF (@DestinationColumns != '')
			BEGIN
				SET @DestinationColumns = @DestinationColumns + ', ';
				SET @SourceColumns = @SourceColumns + ', ';
				SET @TargetColumns = @TargetColumns + ', ';

				/* Create source and target column variables for detecting changed records */
				IF (@SourceColumnsWithoutIncrementalField != '') AND (@SourceObjectColumnName != @IncrementalField) SET @SourceColumnsWithoutIncrementalField = @SourceColumnsWithoutIncrementalField + ', ';
				IF (@TargetColumnsWithoutIncrementalField != '') AND (@SourceObjectColumnName != @IncrementalField) SET @TargetColumnsWithoutIncrementalField = @TargetColumnsWithoutIncrementalField + ', ';
			END;

			SET @DestinationColumns = @DestinationColumns + QUOTENAME(@SourceObjectColumnName);
			SET @SourceColumns = @SourceColumns + '[SOURCE].' + QUOTENAME(@SourceObjectColumnName);
			SET @TargetColumns = @TargetColumns + '[TARGET].' + QUOTENAME(@SourceObjectColumnName);

			/* Create source and target column variables for detecting changed records */
			IF (@SourceObjectColumnName != @IncrementalField) SET @SourceColumnsWithoutIncrementalField = @SourceColumnsWithoutIncrementalField + '[SOURCE].' + QUOTENAME(@SourceObjectColumnName);
			IF (@SourceObjectColumnName != @IncrementalField) SET @TargetColumnsWithoutIncrementalField = @TargetColumnsWithoutIncrementalField + '[TARGET].' + QUOTENAME(@SourceObjectColumnName);

			/* Create key columns join */
			IF (@SourceTargetPrimaryKeyColumns != '') AND (@SourceObjectColumnIsPrimaryKey = 1)
			BEGIN
				SET @PrimaryKeyColumns = @PrimaryKeyColumns + ', ';
				SET @SourceTargetPrimaryKeyColumns = @SourceTargetPrimaryKeyColumns + ' AND ';
			END;

			IF (@SourceObjectColumnIsPrimaryKey = 1) 
			BEGIN
				SET @PrimaryKeyColumns = @PrimaryKeyColumns + QUOTENAME(@SourceObjectColumnName);
				SET @SourceTargetPrimaryKeyColumns = @SourceTargetPrimaryKeyColumns + '([SOURCE].' + QUOTENAME(@SourceObjectColumnName) + ' = [TARGET].' + QUOTENAME(@SourceObjectColumnName) + ')';
			END;

			/* Create variable to hold information if the source contains a IsDeleted flag */
			IF (@SourceObjectColumnName LIKE ('%IsDeleted%')) SET @SourceObjectColumnIsDeleted = '[SOURCE].' + QUOTENAME(@SourceObjectColumnName);
		END;

		FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnIsIdentity, @SourceObjectColumnIsPrimaryKey
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

		/* Create temp table to hold all deleted and updated records */
		SET @stmt = 
			'	/* Create temp table to hold all deleted and updated records */' + @CRLF +
			'	DROP TABLE IF EXISTS #ChangedRecords;' + @CRLF + @CRLF +
			'	/* Create temp table #ChangedRecords to hold all deleted/updated record */' + @CRLF +
			'	SELECT ' + @TargetColumns + @CastIdentityColumn + ', [TARGET].[DWCreatedDate], [TARGET].[DWModifiedDate], [TARGET].[DWIsDeleted], [TARGET].[DWIsCurrent], [TARGET].[DWValidFromDate], [TARGET].[DWValidToDate], [ETLOperation] = CAST(NULL AS NVARCHAR(3))' + @CRLF +
			'	INTO #ChangedRecords' + @CRLF + 
			'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
			'	WHERE 1 = 0' + @CRLF + @CRLF
		;
		
		/* Detect which records that has been deleted in source */
		SET @stmt = @stmt + 
			'	/* Detect which records that has been deleted in source */' + @CRLF +
			'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + @DestinationIdentityColumn + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [DWValidFromDate], [DWValidToDate], [ETLOperation])' + @CRLF +
			'	SELECT	 ' + @TargetColumns + @CRLF +
				@DestinationTargetIdentityColumn + @CRLF +
			'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
			'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
			'	,	[DWIsDeleted]		= 1 ' + @CRLF +
			'	,	[DWIsCurrent]		= 1 ' + @CRLF +
			'	,	[DWValidFromDate]	= [TARGET].[DWValidFromDate] ' + @CRLF +
			'	,	[DWValidToDate]		= ''' + @SCD2ValidToDate + ''' ' + @CRLF +
			'	,	[ETLOperation]		= ''D'' ' + @CRLF +
			'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
				CASE
					/* If load is incremental and JobIsReset is false then soft-delete if when exist in source table */
					WHEN (@JobLoadModeETL NOT IN ('FULL','CUSTOM')) AND (@JobIsReset = 0)
						THEN '	INNER JOIN ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF +
							 '	ON (' + @SourceTargetPrimaryKeyColumns + ')' + IIF(@SourceObjectColumnIsDeleted <> '', ' AND (' + @SourceObjectColumnIsDeleted + ' = 1)', '') + @CRLF

					/* If load is not incremental or JobIsReset is true then soft-delete if not exist in source table */
					WHEN (@JobLoadModeETL IN ('FULL','CUSTOM')) OR (@JobIsReset = 1)
						THEN '	WHERE NOT EXISTS ( ' + @CRLF +
							 '		SELECT 1 FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE]' + @CRLF + 
							 '		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + IIF(@SourceObjectColumnIsDeleted <> '', ' AND (' + @SourceObjectColumnIsDeleted + ' = 0)', '') + @CRLF +
							 '	)' + @CRLF
				END +
			'	AND ([TARGET].[DWIsDeleted] = 0) AND ([TARGET].[DWIsCurrent] = 1)' + @CRLF +
			'	SELECT @DeleteCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
		;

		/* Detect which records that has been Updated in source */
		SET @stmt = @stmt +
			'	/* Create temp table to hold all updated records */' + @CRLF +
			'	DROP TABLE IF EXISTS #UpdatedRecords;' + @CRLF + @CRLF +
			CASE @PreserveSCD2History
				WHEN 0 THEN '	/* Detect which records that has been Updated in source */'
				WHEN 1 THEN '	/* Handle SCD2 History close existing records */'
			END + @CRLF +				
			'	SELECT	 ' + IIF(@PreserveSCD2History = 1, @TargetColumns, @SourceColumns) + @CRLF +
				@DestinationTargetIdentityColumn + @CRLF +
			'	,	[DWCreatedDate]		= [TARGET].[DWCreatedDate] ' + @CRLF +
			'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
			'	,	[DWIsDeleted]		= 0 ' + @CRLF +
			'	,	[DWIsCurrent]		= ' + IIF(@PreserveSCD2History = 1, '0', '1') + @CRLF +
			'	,	[DWValidFromDate]	= [TARGET].[DWValidFromDate] ' + @CRLF +
			'	,	[DWValidToDate]		= ' + IIF(@PreserveSCD2History = 1, '''' + @SCD2ValidToDate + '''', '''9999-12-31 23:59:59.000''') + @CRLF +
			'	,	[ETLOperation]		= ''U'' ' + @CRLF +
			'	INTO #UpdatedRecords ' + @CRLF +
			'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF + 
			'	INNER JOIN ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF +
			'	ON (' + @SourceTargetPrimaryKeyColumns + ')' + 
				CASE /* Do we have IsDeleted flag in the transformation logic */
					WHEN @SourceObjectColumnIsDeleted <> '' THEN ' AND (' + @SourceObjectColumnIsDeleted + ' = 0)'
					ELSE ''
				END + @CRLF +
			'	WHERE EXISTS (' + @CRLF +
			'		 SELECT ' + @SourceColumnsWithoutIncrementalField + @CRLF +
			'		 EXCEPT ' + @CRLF +  
			'		 SELECT ' + @TargetColumnsWithoutIncrementalField + @CRLF +
			'	) AND ([TARGET].[DWIsCurrent] = 1)' + @CRLF +
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
			'			(' + @DestinationColumns + @DestinationIdentityColumn + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [DWValidFromDate], [DWValidToDate], [ETLOperation])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + @SourceIdentityColumn + ', [SOURCE].[DWCreatedDate], [SOURCE].[DWModifiedDate], [SOURCE].[DWIsDeleted], [SOURCE].[DWIsCurrent], [SOURCE].[DWValidFromDate], [SOURCE].[DWValidToDate], [SOURCE].[ETLOperation]' + @CRLF +
			'	FROM #UpdatedRecords AS [SOURCE]' + @CRLF +
			'	WHERE NOT EXISTS (' + @CRLF +
			'		SELECT 1 FROM #ChangedRecords AS [TARGET]' + @CRLF +
			'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + @CRLF +
			'	)' + @CRLF + @CRLF

		/* Handle SCD2 History insert new record version if @PreserveSCD2History is true */
		IF (@PreserveSCD2History = 1)
		BEGIN
			
			/* Handle SCD2 History insert new record version */
			SET @stmt = @stmt +
				'	/* Handle SCD2 History insert new record version */' + @CRLF +
				'	INSERT INTO #ChangedRecords WITH (TABLOCKX)' + @CRLF +
				'			(' + @DestinationColumns + @DestinationIdentityColumn + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [DWValidFromDate], [DWValidToDate], [ETLOperation])' + @CRLF +
				'	SELECT	 ' + @SourceColumns + @CRLF +
						REPLACE(@DestinationIdentityColumn, ', ', CHAR(9) + ',' + CHAR(9)) + ' = ' + @LatestSurrogateKeyValue + ' + ROW_NUMBER() OVER(ORDER BY (SELECT NULL))' + @CRLF +
				'	,	[DWCreatedDate]		= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWModifiedDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWIsDeleted]		= 0 ' + @CRLF +
				'	,	[DWIsCurrent]		= 1 ' + @CRLF +
				'	,	[DWValidFromDate]	= ''' + @CurrentDateTime + ''' ' + @CRLF +
				'	,	[DWValidToDate]		= ''9999-12-31 23:59:59.000'' ' + @CRLF +
				'	,	[ETLOperation]		= ''I'' ' + @CRLF +
				'	FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF + 
				'	INNER JOIN #UpdatedRecords AS [TARGET] ' + @CRLF + 
				'	ON (' + @SourceTargetPrimaryKeyColumns + ') AND ([TARGET].[DWIsCurrent] = 0)' + 
					CASE /* Do we have IsDeleted flag in the transformation logic */
						WHEN @SourceObjectColumnIsDeleted <> '' THEN ' AND (' + @SourceObjectColumnIsDeleted + ' = 0)'
						ELSE ''
					END + @CRLF +				
				'	SELECT @UpdateCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
			;

		END;

		/* Create Nonclustered Index on #ChangedRecords to optimize index scan instead of table scan */
		SET @stmt = @stmt +
			'	/* Create Nonclustered Index on #ChangedRecords to optimize index scan instead of table scan */ ' + @CRLF +
			'	CREATE NONCLUSTERED INDEX [NCI_ChangedRecords] ON #ChangedRecords (' + @PrimaryKeyColumns + ', [DWValidFromDate]) ;' + @CRLF + @CRLF
		;

		/* Handle Changed records - Delete existing records which has been updated or soft-deleted */ 
		SET @stmt = @stmt +
			'	/* Handle Changed records - Delete existing records which has been updated or soft-deleted */' + @CRLF +
			'	DELETE [TARGET] WITH (TABLOCKX)' + @CRLF +
			'	FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
			'	INNER JOIN #ChangedRecords AS [SOURCE] ON (' + @SourceTargetPrimaryKeyColumns + ' AND ([SOURCE].[DWValidFromDate] = [TARGET].[DWValidFromDate]))' + @CRLF + @CRLF
		;

		/* Handle Inserts from Source - Insert changed records */
		SET @stmt = @stmt +
			'	/* Handle Inserts from Source - Insert changed records */' + @CRLF +
			'	SET IDENTITY_INSERT ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' ON ;' + @CRLF +
			'	INSERT INTO ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + @DestinationIdentityColumn + ', [DWCreatedDate], [DWModifiedDate], [DWIsDeleted], [DWIsCurrent], [DWValidFromDate], [DWValidToDate])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + @SourceIdentityColumn + ', [SOURCE].[DWCreatedDate], [SOURCE].[DWModifiedDate], [SOURCE].[DWIsDeleted], [SOURCE].[DWIsCurrent], [SOURCE].[DWValidFromDate], [SOURCE].[DWValidToDate]' + @CRLF +
			'	FROM #ChangedRecords AS [SOURCE]' + @CRLF +
			'	SET IDENTITY_INSERT ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' OFF ;' + @CRLF + @CRLF
		;

		/* Handle Inserts from Source - Insert new and changed records */
		SET @stmt = @stmt +
			'	/* Handle Inserts from Source - Insert new records */ ' + @CRLF +
			'	INSERT INTO ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + ', ''' + @CurrentDateTime + ''', ''' + @CurrentDateTime + ''' ' + @CRLF +
			'	FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF +
			'	WHERE NOT EXISTS (' + CHAR (10) +
			'		SELECT 1 FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [TARGET]' + @CRLF +
			'		WHERE (' + @SourceTargetPrimaryKeyColumns + ')' + CHAR (10) +
			'	)' + 
				CASE /* Do we have IsDeleted flag in the transformation logic */
					WHEN @SourceObjectColumnIsDeleted <> '' THEN ' AND (' + @SourceObjectColumnIsDeleted + ' = 0)'
					ELSE ''
				END + @CRLF +
			'	SELECT @InsertCnt = @@ROWCOUNT ;' + @CRLF + @CRLF
		;

	END
	
	IF (@SourceTargetPrimaryKeyColumns = '') AND (@StopExecution = 0)
	BEGIN
		
		/* If no primary key exists we are forced to truncate full-load the entity */
		SET @stmt = 
			'	/* Truncate EDW table */ ' + @CRLF +
			'	TRUNCATE TABLE ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';' + @CRLF +
			'	INSERT INTO ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX)' + @CRLF +
			'			(' + @DestinationColumns + ', [DWCreatedDate], [DWModifiedDate])' + @CRLF +
			'	SELECT	 ' + @SourceColumns + ', ''' + @CurrentDateTime + ''', ''' + @CurrentDateTime + ''' ' + @CRLF +
			'	FROM ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' AS [SOURCE] ' + @CRLF + 
				CASE /* Do we have IsDeleted flag in the transformation logic */
					WHEN @SourceObjectColumnIsDeleted <> '' THEN 'WHERE (' + @SourceObjectColumnIsDeleted + ' = 0)'
					ELSE ''
				END + @CRLF +
			'	SELECT @InsertCnt = @@ROWCOUNT;' + @CRLF +
			'	SELECT @UpdateCnt = 0 ;' + @CRLF +
			'	SELECT @DeleteCnt = 0 ;' + @CRLF + @CRLF;
	END;

	/* Prepare execute sql statement */
	IF (@StopExecution = 0)
	BEGIN TRY

		SET @TaskName = @LoadPattern + ' - Load Dimension'

		/* Delta load pattern with Insert, Update and Delete */
		IF (@emulation = 1) SELECT @DestinationSchemaName AS DestinationSchemaName, @DestinationTableName AS DestinationTableName, @stmt AS SqlStatement;
		IF (@emulation = 0) 
		BEGIN
			BEGIN TRANSACTION 

			EXEC sys.sp_executesql @stmt, N'@UpdateCnt bigint OUTPUT, @InsertCnt bigint OUTPUT, @DeleteCnt bigint OUTPUT', @UpdateCnt OUTPUT, @InsertCnt OUTPUT, @DeleteCnt OUTPUT;

			COMMIT TRANSACTION;

			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected inserts: ' + CONVERT(nvarchar, @InsertCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'INSERT', @rows = @InsertCnt;

			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected updates: ' + CONVERT(nvarchar, @UpdateCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'UPDATE', @rows = @updateCnt;

			SET @message = 'Load sequence ' + CONVERT(nvarchar, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Merge into ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' affected deletes: ' + CONVERT(nvarchar, @deleteCnt) + ' rows';
			EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName, @operation = 'DELETE', @rows = @deleteCnt;

			/* Update statistics */
			SET @stmt = 'UPDATE STATISTICS ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';';
			EXEC sys.sp_executesql @stmt;

			/* If job is reset update metadata that next load should be incremental */
			IF (@JobIsReset = 1) EXEC sys.sp_UpdateExtendedProperty @level0type = N'SCHEMA', @level0name = @DWTransformStagingSchemaName, @level1type = N'TABLE', @level1name = @DestinationTableName, @name = N'IsReset', @value = 0;

		END;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Failed to Merge into ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' due to: ' + @CRLF + ERROR_MESSAGE();
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + @CRLF + @stmt;
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		/* Break Azure Data Factory! */
		SELECT 1/0
	END CATCH;

	IF (@Emulation = 0) AND (OBJECT_ID('meta.BusinessObjectExecutionPlan') IS NOT NULL)
	BEGIN
		/* Update meta.BusinessObjectExecutionPlan set ExecutionStatus = 'Finished' */
		UPDATE [TARGET] WITH (TABLOCKX) SET 
			[TARGET].[ExecutionStatusCode] = 0
		FROM meta.BusinessObjectExecutionPlan AS [TARGET]
		WHERE ([TARGET].[PrecedenceObjectSchema] = @SourceObjectSchema) AND ([TARGET].[PrecedenceObjectName] = @SourceObjectName)
	END;
END;