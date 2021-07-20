CREATE PROCEDURE [meta].[spTransformLoadStaging]

	@DestinationSchemaName NVARCHAR(255)
,	@DestinationTableName NVARCHAR(255)
,	@PackageName NVARCHAR(255)
,	@LoadSequence INT
,	@Emulation TINYINT = 1
AS 
BEGIN
	
	DECLARE @TaskName NVARCHAR(MAX);
	DECLARE @Message NVARCHAR(MAX);
	DECLARE @LoadPattern NVARCHAR(255);
	DECLARE @stmt NVARCHAR(MAX);
	DECLARE @Sql1 NVARCHAR(MAX) = '';
	DECLARE @Sql2 NVARCHAR(MAX) = '';

	DECLARE @SourceObjectSchema NVARCHAR(255);
	DECLARE @SourceObjectName NVARCHAR(255);
	DECLARE @SourceObjectPrefix NVARCHAR(255);
	DECLARE @SourceObjectFilter NVARCHAR(255);
	DECLARE @DWTransformStagingSchemaName NVARCHAR(255);

	DECLARE @SourceObjectColumnName NVARCHAR(255);
	DECLARE @SourceObjectColumnType NVARCHAR(255);
	DECLARE @SourceObjectColumnIsPrimaryKey TINYINT;

	DECLARE @JobLoadModeETL NVARCHAR(50);
	DECLARE @JobIsReset TINYINT;
	DECLARE @JobIsPartitionLoad TINYINT = 0;
	DECLARE @LastLoadedValue NVARCHAR(255);
	DECLARE @IncrementalField NVARCHAR(255);
	DECLARE @IncrementalFieldType NVARCHAR(255);
	DECLARE @IncrementalOffSet INT;

	/* Prepare common Data Warehouse parameters */
	SET @DWTransformStagingSchemaName	= (SELECT CONVERT(NVARCHAR(255), [value]) FROM sys.fn_listextendedproperty (NULL, NULL, NULL, NULL, NULL, NULL, NULL) WHERE [name] = 'DWTransformStagingSchemaName');

	/* Check if object exists */
	IF (OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName) IS NULL)
	BEGIN
		/* To avoid breaking the load package when object does not exist - simply print error */
		PRINT 'Unable to load entity as it does not exists';		
		RETURN -1;
	END;

	/* Prepare SourceObject parameters */
	SELECT
		@SourceObjectSchema			= MAX(CASE WHEN [name] = 'SourceObjectSchema'	THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@SourceObjectName			= MAX(CASE WHEN [name] = 'SourceObjectName'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@JobLoadModeETL				= MAX(CASE WHEN [name] = 'LoadModeETL'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)	
	,	@JobIsReset					= MAX(CASE WHEN [name] = 'IsReset'				THEN CONVERT(TINYINT,		[value]) ELSE 0 END)
	,	@IncrementalField			= MAX(CASE WHEN [name] = 'IncrementalField'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@IncrementalOffSet			= MAX(CASE WHEN [name] = 'IncrementalOffSet'	THEN CONVERT(INT,			[value]) ELSE 0 END)
	,	@SourceObjectFilter			= MAX(CASE WHEN [name] = 'SourceObjectFilter'	THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@SourceObjectPrefix			= MAX(CASE WHEN [name] = 'SourceObjectPrefix'	THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 1) AND (ep.major_id = OBJECT_ID(@DWTransformStagingSchemaName + '.' + @DestinationTableName))
	GROUP BY ep.major_id

	/* DROP/CREATE Dummy statistics on [DestinationSchemaName].[DestinationTableName] with SAMPLE */
	SET @stmt = 
		'	IF EXISTS (SELECT 1 FROM sys.stats AS st WITH (NOLOCK) WHERE (st.object_id = OBJECT_ID(''' + @DWTransformStagingSchemaName + '.' + @DestinationTableName + ''')) AND (st.name = ''ST_' + @DWTransformStagingSchemaName + '_' + @DestinationTableName + '''))' + CHAR(10) +
		'	BEGIN' + CHAR(10) +
		'		DROP STATISTICS ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + '.' + QUOTENAME('ST_' + @DWTransformStagingSchemaName + '_' + @DestinationTableName) + ';' + CHAR(10) +
		'	END;' + CHAR(10) + 
		'	CREATE STATISTICS ' + QUOTENAME('ST_' + @DWTransformStagingSchemaName + '_' + @DestinationTableName) + ' ON ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + '(' + COL_NAME(OBJECT_ID(@DWTransformStagingSchemaName + '.' + @DestinationTableName), 1) + ') WITH SAMPLE 1 PERCENT;' + CHAR(10) + 
		'	DROP STATISTICS ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + '.' + QUOTENAME('ST_' + @DWTransformStagingSchemaName + '_' + @DestinationTableName) + ';'
		;
	EXEC sys.sp_executesql @stmt; 

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
		,	[SourceObjectColumnType]			=	UPPER(t.name)
		FROM sys.columns AS c WITH (NOLOCK)
		JOIN sys.types AS t WITH (NOLOCK) ON c.user_type_id = t.user_type_id
		WHERE (c.is_identity = 0) AND (c.name NOT IN ('DWCreatedDate')) AND (c.object_id = OBJECT_ID(@DWTransformStagingSchemaName + '.' + @DestinationTableName))
		ORDER BY c.column_id
	OPEN cur
	FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnType
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		IF (@Sql1 <> '')
		BEGIN
			SET @Sql1 = @Sql1 + ', ';
			SET @Sql2 = @Sql2 + ', ';
		END;

		SET @Sql1 =	@Sql1 + CHAR(9) + QUOTENAME(@SourceObjectColumnName) + CHAR(10);
		SET @Sql2 =	@Sql2 + CHAR(9) + QUOTENAME(@SourceObjectColumnName) + ' = ' + QUOTENAME(@SourceObjectColumnName) + CHAR(10);
		
		IF (@IncrementalField = @SourceObjectColumnName)
		BEGIN 
			SET @IncrementalFieldType = @SourceObjectColumnType
		END;

		FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnType
	END
	CLOSE cur;
	DEALLOCATE cur;

	/* Get Last loaded value registered in DWH */
	BEGIN
		SET @stmt = 
		'SELECT @LastLoadedValue = ' + 			
			CASE 
				WHEN (@JobLoadModeETL IN ('ICL'))
					THEN
						CASE
							WHEN @IncrementalFieldType LIKE ('DATE%') THEN 'ISNULL(MAX(' + QUOTENAME(@IncrementalField) + '), ''1900-01-01 00:00:00.000'')'
							ELSE 'MAX(' + QUOTENAME(@IncrementalField) + ')'
						END
				ELSE '1'
			END + CHAR(10) +
		'FROM ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (NOLOCK)' ;

		/* Get last loaded value */
		EXEC sys.sp_executesql @stmt, N'@LastLoadedValue NVARCHAR(255) OUTPUT', @LastLoadedValue OUTPUT;
		SET @LastLoadedValue = ISNULL(@LastLoadedValue, '1');
	END;

	/* Is transform source a Stored Procedure */
	IF (SELECT [type] FROM sys.objects AS o WITH (NOLOCK) WHERE o.object_id = OBJECT_ID(@SourceObjectSchema + '.' + @SourceObjectName)) = 'P'
	BEGIN
		SET @stmt = 'EXEC ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' ' + CONVERT(NVARCHAR, @JobIsReset) + ';' ;
	END;

	/* Prepare select statement for loading the Staging table - check if job is incremental or full load */
	IF (SELECT [type] FROM sys.objects AS o WITH (NOLOCK) WHERE o.object_id = OBJECT_ID(@SourceObjectSchema + '.' + @SourceObjectName)) = 'V'
	BEGIN
		SET @stmt =
			'TRUNCATE TABLE ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';' + CHAR(10) + CHAR(13) +
			'INSERT INTO ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (TABLOCKX) (' + CHAR(10) +
				+ @Sql1 +
			')' + CHAR(10) +
			'SELECT ' + CHAR(10)
				+ @Sql2 + 
			'FROM ' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + CHAR(10) +
			'WHERE 1 = 1 ' + 
			CASE /* Is job incremental or full load */
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'ICL') 
				THEN
					CASE /* Is source object filter condition */
						WHEN (@SourceObjectFilter <> '') AND (@SourceObjectFilter IS NOT NULL) THEN 'AND ' + @SourceObjectFilter + CHAR(10)
						ELSE '' 
					END + 
				'AND (' + QUOTENAME(@IncrementalField) + 
				' > ' + 
					CASE /* Handle Incremental off - Last loaded value depending on whether is a date or not */
						WHEN (@IncrementalFieldType LIKE ('DATE%')) THEN '''' + CONVERT(NVARCHAR, DATEADD(DAY, - @IncrementalOffSet, @LastLoadedValue), 121) + ''''
						ELSE CAST(@LastLoadedValue - @IncrementalOffSet AS NVARCHAR)
					END +
				')'
				ELSE ''
			END
		;
	END;

	/* Prepare execute sql statement */
	IF (@Emulation = 1)
	BEGIN
		
		/* If emulation is true then only select object parameters and sql statement */
		SELECT @DWTransformStagingSchemaName AS DestinationSchemaName, @DestinationTableName AS DestinationTableName, @stmt AS SqlStatement;

	END ELSE 
	BEGIN TRY 

		SET @TaskName = @LoadPattern + ' - Load Staging using Transform' + '';

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ' - ' + 'Start Loading ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName);
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName;

		BEGIN TRANSACTION

		/* Load Staging table */
		EXEC sys.sp_executesql @stmt;

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ' - ' + 'End Loading ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName);
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName;

		COMMIT TRANSACTION;
	END TRY
	BEGIN CATCH
		IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + 'Failed to Load ' + QUOTENAME(@DWTransformStagingSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' due to: ' + CHAR(10) + ERROR_MESSAGE();
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		SET @message = 'Load sequence ' + CONVERT(NVARCHAR, @LoadSequence) + ' - ' + @TaskName + ': ' + CHAR(10) + @stmt;
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;

		/* Break Azure Data Factory! */
		SELECT 1/0
	END CATCH;

END;