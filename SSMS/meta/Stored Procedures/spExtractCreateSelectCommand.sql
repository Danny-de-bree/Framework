CREATE PROCEDURE [meta].[spExtractCreateSelectCommand]

	@DestinationSchemaName NVARCHAR(255)
,	@DestinationTableName NVARCHAR(255)
,	@PackageName NVARCHAR(255)
,	@LoadSequence INT
AS 
BEGIN
	
	DECLARE @Message NVARCHAR(MAX);
	DECLARE @ADFWriteBatchSize INT;
	DECLARE @DefaultMaxDop INT;
	DECLARE @EnableAutoPartition TINYINT;

	DECLARE @DWExtractDWSchemaName NVARCHAR(255);
	DECLARE @DWExtractHistorySchemaName NVARCHAR(255);

	DECLARE @Sql1 NVARCHAR(MAX) = '';
	DECLARE @Sql2 NVARCHAR(MAX) = '';
	DECLARE @Sql3 NVARCHAR(MAX) = '';
	DECLARE @Sql4 NVARCHAR(MAX) = '';

	DECLARE @stmt NVARCHAR(MAX);
	DECLARE @PreCopyCommand NVARCHAR(MAX);
	DECLARE @CheckLoadedValue NVARCHAR(MAX);
	DECLARE @TabularTranslatorMapping NVARCHAR(MAX) = '';

	DECLARE @DataSourceName NVARCHAR(255);
	DECLARE @DataSourceServerName NVARCHAR(255);
	DECLARE @DataSourceDatabaseName NVARCHAR(255);
	DECLARE @SourceObjectSchema NVARCHAR(255);
	DECLARE @SourceObjectName NVARCHAR(255);

	DECLARE @SourceObjectColumnName NVARCHAR(255);
	DECLARE @SourceObjectColumnType NVARCHAR(255);
	DECLARE @SourceObjectSinkColumnType NVARCHAR(255);
	DECLARE @SourceObjectColumnLength NVARCHAR(255);
	DECLARE @SourceObjectColumnIsPrimaryKey TINYINT;
	DECLARE @SourceObjectColumnPrimaryKeyColumCount INT = 0;
	DECLARE @SourceObjectColumnPrimaryKeyColumType NVARCHAR(255) = '';
	DECLARE @SourceObjectPartitionColumn NVARCHAR(255) = '';
	DECLARE @SourceObjectPartitionOption NVARCHAR(255) = 'None';
	DECLARE @SourceObjectPartitionUpper BIGINT = 1;
	DECLARE @SourceObjectPartitionLower	BIGINT = 0;

	DECLARE @JobLoadModeETL NVARCHAR(50);
	DECLARE @JobIsReset TINYINT;
	DECLARE @JobIsPartitionLoad TINYINT = 0;
	DECLARE @LastLoadedValue NVARCHAR(255);
	DECLARE @IncrementalField NVARCHAR(255);
	DECLARE @IncrementalFieldType NVARCHAR(255);
	DECLARE @IncrementalOffSet INT;
	DECLARE @SourceObjectFilter NVARCHAR(255);
	DECLARE @RowCount BIGINT = 0;

	/* Prepare common Data Warehouse parameters */
	SELECT
		@DefaultMaxDop					= 1
	,	@ADFWriteBatchSize				= MAX(CASE WHEN [name] = 'ADFWriteBatchSize'			THEN CONVERT(INT		  , [value]) ELSE 0 END)
	,	@EnableAutoPartition			= MAX(CASE WHEN [name] = 'EnableAutoPartition'			THEN CONVERT(NVARCHAR(255), [value]) ELSE 0 END)
	,	@DWExtractDWSchemaName			= MAX(CASE WHEN [name] = 'DWExtractDWSchemaName'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DWExtractHistorySchemaName		= MAX(CASE WHEN [name] = 'DWExtractHistorySchemaName'	THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0) 
	GROUP BY (ep.major_id)

	/* Prepare SourceObject parameters */
	SELECT
		@DataSourceName					= MAX(CASE WHEN [name] = 'DataSourceName'				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DataSourceServerName			= MAX(CASE WHEN [name] = 'DataSourceServerName'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@DataSourceDatabaseName			= MAX(CASE WHEN [name] = 'DataSourceDatabaseName'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@SourceObjectSchema				= MAX(CASE WHEN [name] = 'SourceObjectSchema'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@SourceObjectName				= MAX(CASE WHEN [name] = 'SourceObjectName'				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@JobLoadModeETL					= MAX(CASE WHEN [name] = 'LoadModeETL'					THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)	
	,	@JobIsReset						= MAX(CASE WHEN [name] = 'IsReset'						THEN CONVERT(TINYINT,		[value]) ELSE 0 END)
	,	@IncrementalField				= MAX(CASE WHEN [name] = 'IncrementalField'				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	,	@IncrementalOffSet				= MAX(CASE WHEN [name] = 'IncrementalOffSet'			THEN CONVERT(INT,			[value]) ELSE 0 END)
	,	@SourceObjectFilter				= MAX(CASE WHEN [name] = 'SourceObjectFilter'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 1) AND (ep.major_id = OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName))
	GROUP BY (ep.major_id)

	/* Fetch metadata from schema if object extended properties are blank "" */
	IF (@DataSourceName = '')
	BEGIN
		SELECT
			@DataSourceName					= MAX(CASE WHEN [name] = 'DataSourceName'				THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
		,	@DataSourceServerName			= MAX(CASE WHEN [name] = 'DataSourceServerName'			THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
		,	@DataSourceDatabaseName			= MAX(CASE WHEN [name] = 'DataSourceDatabaseName'		THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
		FROM sys.extended_properties AS ep WITH (NOLOCK)
		WHERE (ep.class = 3) AND (ep.major_id = SCHEMA_ID(@DestinationSchemaName))
		GROUP BY (ep.major_id)

		/* Update DW and History schema names */
		SET @DWExtractDWSchemaName			= @DestinationSchemaName + '_' + @DWExtractDWSchemaName;
		SET @DWExtractHistorySchemaName		= @DestinationSchemaName + '_' + @DWExtractHistorySchemaName;

	END;

	/* DROP/CREATE Dummy statistics on [DestinationSchemaName].[DestinationTableName] with SAMPLE */
	SET @stmt = 
		'	IF EXISTS (SELECT 1 FROM sys.stats AS st WITH (NOLOCK) WHERE (st.object_id = OBJECT_ID(''' + @DestinationSchemaName + '.' + @DestinationTableName + ''')) AND (st.name = ''ST_' + @DestinationSchemaName + '_' + @DestinationTableName + '''))' + CHAR(10) +
		'	BEGIN' + CHAR(10) +
		'		DROP STATISTICS ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + '.' + QUOTENAME('ST_' + @DestinationSchemaName + '_' + @DestinationTableName) + ';' + CHAR(10) +
		'	END;' + CHAR(10) + 
		'	CREATE STATISTICS ' + QUOTENAME('ST_' + @DestinationSchemaName + '_' + @DestinationTableName) + ' ON ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + '(' + COL_NAME(OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName), 1) + ') WITH SAMPLE 1 PERCENT;' + CHAR(10) + 
		'	DROP STATISTICS ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + '.' + QUOTENAME('ST_' + @DestinationSchemaName + '_' + @DestinationTableName) + ';'
		;
	EXEC sys.sp_executesql @stmt; 

	/* Create #temp table to hold SQL datatypes converted to .NET Framework */
	DROP TABLE IF EXISTS #SqlDataTypes;

	SELECT x.SqlDataType, x.NET
	INTO #SqlDataTypes
	FROM (
	VALUES 
		('bigint'			, 'Int64')
	,	('binary'			, 'byte[]')
	,	('bit'				, 'bool')
	,	('char'				, 'String')
	,	('date'				, 'DateTime')
	,	('datetime'			, 'DateTime')
	,	('datetime2'		, 'DateTime')
	,	('datetimeoffset'	, 'DateTimeOffset')
	,	('decimal'			, 'decimal')
	,	('filestream'		, 'byte[]')
	,	('float'			, 'double')
	,	('image'			, 'byte[]')
	,	('int'				, 'int32')
	,	('money'			, 'decimal')
	,	('nchar'			, 'string')
	,	('ntext'			, 'string')
	,	('numeric'			, 'decimal')
	,	('nvarchar'			, 'string')
	,	('real'				, 'Single')
	,	('rowversion'		, 'byte[]')
	,	('smalldatetime'	, 'DateTime')
	,	('smallint'			, 'short')
	,	('smallmoney'		, 'decimal')
	,	('sql_variant'		, 'object')
	,	('text'				, 'string')
	,	('time'				, 'TimeSpan')
	,	('timestamp'		, 'byte[]')
	,	('tinyint'			, 'byte')
	,	('uniqueidentifier'	, 'Guid')
	,	('varbinary'		, 'byte[]')
	,	('varchar'			, 'string')
	,	('xml'				, 'string')
	) AS x (SqlDataType, NET)

	/* Generate column list used in main select and primary key columns in join statement */
	DECLARE cur CURSOR LOCAL FOR 
		SELECT
			[SourceObjectColumnName]			=	c.name
		,	[SourceObjectColumnType]			=	UPPER(t.name)
		,	[SourceObjectColumnLength]			=	CASE 
														WHEN t.name IN ('varchar', 'char', 'varbinary', 'binary', 'text') THEN '(' + CASE WHEN c.max_length = -1 THEN '4000' ELSE CAST(c.max_length AS VARCHAR(5)) END + ')'
														WHEN t.name IN ('nvarchar', 'nchar', 'ntext') THEN '(' + CASE WHEN c.max_length = -1 THEN '4000' ELSE CAST(c.max_length / 2 AS VARCHAR(5)) END + ')'
														WHEN t.name IN ('datetime2', 'time2', 'datetimeoffset') THEN '(' + CAST(c.scale AS VARCHAR(5)) + ')'
														WHEN t.name IN ('numeric', 'decimal') THEN '(' + CAST(c.[precision] AS VARCHAR(5)) + ', ' + CAST(c.scale AS VARCHAR(5)) + ')'
														ELSE ''
													END
		,	[SourceObjectSinkColumnType]		=	sdt.NET
		,	[SourceObjectColumnIsPrimaryKey]	=	CASE WHEN (ep.value IS NOT NULL) THEN 1 ELSE 0 END
		FROM sys.columns AS c WITH (NOLOCK)
		INNER JOIN sys.types AS t WITH (NOLOCK) ON (c.user_type_id = t.user_type_id)
		LEFT JOIN sys.extended_properties AS ep WITH (NOLOCK) ON (c.object_id = ep.major_id) AND (c.column_id = ep.minor_id) AND (ep.name = 'IsPrimaryKey')
		LEFT JOIN #SqlDataTypes AS sdt ON (sdt.SqlDataType = t.name)
		WHERE (c.is_identity = 0) AND (c.default_object_id = 0) AND (c.name NOT IN ('$SharedName')) AND (c.object_id = OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName))
		ORDER BY c.column_id
	OPEN cur
	FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnType, @SourceObjectColumnLength, @SourceObjectSinkColumnType, @SourceObjectColumnIsPrimaryKey
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		IF (@Sql1 <> '')
		BEGIN
			SET @Sql1 = @Sql1 + ',';
			SET @Sql2 = @Sql2 + ',';
			SET @Sql4 = @Sql4 + ',';
		END;

		IF (@Sql3 <> '') AND (@SourceObjectColumnIsPrimaryKey = 1)
		BEGIN
			SET @Sql3 = @Sql3 + ' AND ';
		END

		SET @Sql1 =	@Sql1 + CHAR(9) + QUOTENAME(@SourceObjectColumnName) + ' = CAST(' + IIF(@SourceObjectColumnIsPrimaryKey = 1, 'CT.', 'SRC.') + QUOTENAME(@SourceObjectColumnName) + ' AS ' + @SourceObjectColumnType + @SourceObjectColumnLength + ')' + CHAR(10);
		SET @Sql2 =	@Sql2 + CHAR(9) + QUOTENAME(@SourceObjectColumnName) + ' = CAST(' + 'SRC.' + QUOTENAME(@SourceObjectColumnName) + ' AS ' + @SourceObjectColumnType + @SourceObjectColumnLength + ')' + CHAR(10);
		SET @Sql4 = @Sql4 + '{' + CHAR(10) + CHAR(9) + '"source": { "name": "' + @SourceObjectColumnName + '", "type": "' + @SourceObjectSinkColumnType + '"},' + CHAR(10) + CHAR(9) + '"sink": { "name": "' + @SourceObjectColumnName + '"}' + CHAR(10) + '}' + CHAR(10);

		IF (@IncrementalField = @SourceObjectColumnName)
		BEGIN 
			SET @IncrementalFieldType = @SourceObjectColumnType
		END;

		IF (@SourceObjectColumnIsPrimaryKey = 1) 
		BEGIN 
			SET @Sql3 = @Sql3 + '(SRC.' + QUOTENAME(@SourceObjectColumnName) + ' = CT.' + QUOTENAME(@SourceObjectColumnName) + ')'
			SET @SourceObjectColumnPrimaryKeyColumCount = @SourceObjectColumnPrimaryKeyColumCount + 1;
			SET @SourceObjectColumnPrimaryKeyColumType = @SourceObjectColumnType;
			SET @SourceObjectPartitionColumn = @SourceObjectColumnName;
		END;

		FETCH NEXT FROM cur INTO @SourceObjectColumnName, @SourceObjectColumnType, @SourceObjectColumnLength, @SourceObjectSinkColumnType, @SourceObjectColumnIsPrimaryKey
	END
	CLOSE cur
	DEALLOCATE cur

	/* Generate Azure Pre-Copy script */
	BEGIN
		SET @PreCopyCommand = 
			'TRUNCATE TABLE ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';' ;
	END;

	/* Generate Json Column mapping */
	BEGIN
		SET @TabularTranslatorMapping = 
			'{"type": "TabularTranslator", "mappings": [' + @Sql4 + ']}';
	END;

	/* Get Last loaded value registered in DWH */
	BEGIN
		SET @stmt = 
		'SELECT @LastLoadedValue = ' + 			
			CASE 
				WHEN (@JobLoadModeETL IN ('CT', 'CDC'))
					THEN 'MAX([DWTrackingVersion])'
				WHEN (@JobLoadModeETL IN ('ICL'))
					THEN
						CASE
							WHEN @IncrementalFieldType LIKE ('DATE%') THEN 'ISNULL(MAX(' + QUOTENAME(@IncrementalField) + '), ''1900-01-01 00:00:00.000'')'
							ELSE 'MAX(' + QUOTENAME(@IncrementalField) + ')'
						END
				ELSE '1'
			END + CHAR(10) +
		'FROM ' + QUOTENAME(@DWExtractDWSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' WITH (NOLOCK)' ;

		EXEC sys.sp_executesql @stmt, N'@LastLoadedValue NVARCHAR(255) OUTPUT', @LastLoadedValue OUTPUT;
		SET @LastLoadedValue = ISNULL(@LastLoadedValue, '1');

	END;

	/* Prepare select statement for latest tracting version using the Change Tracking pattern */
	IF (@JobLoadModeETL = 'CT')
	BEGIN
		SET @CheckLoadedValue =
			'SELECT [DWTrackingVersion] = COUNT(1)' + CHAR(10) +
			'FROM CHANGETABLE(CHANGES ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ', ' + @LastLoadedValue + ') AS CT';		
	END;

	/* Prepare select statement for latest tracting version using the Change Data Capture pattern */
	IF (@JobLoadModeETL = 'CDC')
	BEGIN
		SET @CheckLoadedValue =
			'SELECT [DWTrackingVersion] = CONVERT(BIGINT, FORMAT(sys.fn_cdc_map_lsn_to_time(sys.fn_cdc_get_min_lsn(N''' + @SourceObjectSchema + '_' + @SourceObjectName + ''')), ''yyyyMMddHHmmssfff''));';	
	END;

	/* Prepare select statement for latest tracting version using the Incremenatal load pattern */
	IF (@JobLoadModeETL = 'ICL')
	BEGIN
		SET @CheckLoadedValue =
			'SELECT [DWTrackingVersion] = COUNT(1) ' + CHAR(10) +
			'FROM ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + CHAR(10) +
			'WHERE ' + CASE WHEN (@SourceObjectFilter <> '') AND (@SourceObjectFilter IS NOT NULL) THEN @SourceObjectFilter ELSE '1 = 1 ' END + 
			' AND (' + QUOTENAME(@IncrementalField) + ' > ' + 
				CASE /* Handle Incremental off - Last loaded value depending on whether is a date or not */
					WHEN (@IncrementalFieldType LIKE ('DATE%')) THEN '''' + CONVERT(NVARCHAR, DATEADD(DAY, - @IncrementalOffSet, @LastLoadedValue), 121) + ''''
					ELSE CAST(@LastLoadedValue - @IncrementalOffSet AS NVARCHAR)
				END +
			')' ;	
	END;

	SET @CheckLoadedValue = ISNULL(@CheckLoadedValue, 'SELECT [DWTrackingVersion] = 1;') ;

	/* Only if EnableAutoPartition is enabled should Azure Data Factory use a dynamic partition load */
	IF ((@EnableAutoPartition = 1) AND (@JobIsReset = 1)) AND (@SourceObjectColumnPrimaryKeyColumCount = 1) AND (@SourceObjectColumnPrimaryKeyColumType IN ('BIGINT','INT'))
	BEGIN
		SET @SourceObjectPartitionOption = 'DynamicRange';
		SET @JobIsPartitionLoad = 1;

		SET @DefaultMaxDop = (SELECT CONVERT(INT, [value]) FROM sys.fn_listextendedproperty (NULL, NULL, NULL, NULL, NULL, NULL, NULL) WHERE [name] = 'DefaultMaxDop');

		SET @stmt = 
			'SELECT ' + CHAR(10) +
			'	@SourceObjectPartitionUpper = MAX(' + QUOTENAME(@SourceObjectPartitionColumn) + ')' + CHAR(10) +
			',	@SourceObjectPartitionLower = MIN(' + QUOTENAME(@SourceObjectPartitionColumn) + ')' + CHAR(10) +
			'FROM ' + CASE WHEN (@DWExtractHistorySchemaName != '') THEN QUOTENAME(@DWExtractHistorySchemaName) ELSE QUOTENAME(@DWExtractDWSchemaName) END + '.' + QUOTENAME(@DestinationTableName);

		EXEC sp_executesql @stmt, 
			N'@SourceObjectPartitionUpper BIGINT OUTPUT, @SourceObjectPartitionLower BIGINT OUTPUT', 
			@SourceObjectPartitionUpper OUTPUT, @SourceObjectPartitionLower OUTPUT ;
	END;					

	/* Prepare select statement for incremental load patterns */

	IF (@JobIsReset = 0)
	BEGIN

		/* Prepare select statement for incremental load using the Change Tracking pattern */
		IF (@JobLoadModeETL = 'CT')
		BEGIN
			SET @stmt =
				'/* Define variables @MinimumValidVersion and @CurrentVersion */' + CHAR(10) +
				'DECLARE @MinimumValidVersion BIGINT;' + CHAR(10) +
				'DECLARE @CurrentVersion BIGINT;' + CHAR(10) + CHAR(10) +
				'SELECT ' + CHAR(10) +
				'	@CurrentVersion			=	MAX(CT.[SYS_CHANGE_VERSION])' + CHAR(10) +
				',	@MinimumValidVersion	=	MIN(CT.[SYS_CHANGE_VERSION])' + CHAR(10) +
				'FROM CHANGETABLE(CHANGES ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ', 0) AS CT' + CHAR(10) + CHAR(10) +
				'/* Check if LastLoadedValue is valid within the retention period if not force full load */' + CHAR(10) +
				'IF (' + @LastLoadedValue + ' >= @MinimumValidVersion)' + CHAR(10) +
				'BEGIN' + CHAR(10) + CHAR(10) +
				'/* Run Incremental load using Change Tracking as the LastLoadedValue is valid */ ' + CHAR(10) +
				'SELECT ' + CHAR(10)
					+ @sql1 +
				',	[DWOperation] = CAST(CT.[SYS_CHANGE_OPERATION] AS NVARCHAR(10))' + CHAR(10) +
				',	[DWTrackingVersion] = CAST(CT.[SYS_CHANGE_VERSION] AS BIGINT)'  + CHAR(10) +
				'FROM CHANGETABLE(CHANGES ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ', ' + @LastLoadedValue + ') AS CT' + CHAR(10) +
				'LEFT JOIN ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' AS SRC ON (' + @Sql3 + ')' + CHAR(10) +
				'WHERE ' + CASE @JobIsPartitionLoad WHEN 1 THEN '?AdfDynamicRangePartitionCondition ' ELSE '1 = 1 ' END + CHAR(10) +
					CASE /* Is source object filter condition */
						WHEN (@SourceObjectFilter <> '') AND (@SourceObjectFilter IS NOT NULL) THEN 'AND ' + @SourceObjectFilter + CHAR(10)
						ELSE '' 
					END + CHAR(10) +
				'END ELSE' + CHAR(10) +
				'BEGIN' + CHAR(10) + CHAR(10) +
				'/* Force full-load as the LastLoadedValue is invalid */ ' + CHAR(10) + CHAR(10) +
				'SELECT ' + CHAR(10)
					+ @Sql2 +
				',	[DWOperation] = CAST(''I'' AS NVARCHAR(10))' + CHAR(10) +
				',	[DWTrackingVersion] = CAST(@CurrentVersion AS BIGINT)'  + CHAR(10) +
				'FROM ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' AS SRC' + CHAR(10) +
				'WHERE ' + CASE @JobIsPartitionLoad WHEN 1 THEN '?AdfDynamicRangePartitionCondition ' ELSE '1 = 1 ' END + CHAR(10) +
					CASE /* Is source object filter condition */
						WHEN (@SourceObjectFilter <> '') AND (@SourceObjectFilter IS NOT NULL) THEN 'AND ' + @SourceObjectFilter + CHAR(10)
						ELSE '' 
					END + CHAR(10) +
				'END;'
				;
		END;

		/* Prepare select statement for incremental load using the Change Data Capture pattern */
		IF (@JobLoadModeETL = 'CDC')
		BEGIN
			SET @stmt =
				'USE ' + QUOTENAME(@DataSourceDatabaseName) + CHAR(10) + CHAR(10) +
				'DECLARE @LastCDCStartDate DATETIME;' + CHAR(10) +
				'DECLARE @LastStartLsn BINARY(10);' + CHAR(10) +
				'DECLARE @LastEndLsn BINARY(10);' + CHAR(10) + CHAR(10) +
				'SELECT @LastCDCStartDate = ''' + @LastLoadedValue + ''';' + CHAR(10) +
				'SELECT @LastStartLsn = sys.fn_cdc_map_time_to_lsn (''smallest greater than'', @LastCDCStartDate);' + CHAR(10) +
				'SELECT @LastEndLsn = sys.fn_cdc_get_max_lsn();' + CHAR(10) + CHAR(10) +
				'SELECT ' + CHAR(10)
					+ @Sql1 +
				',	[DWOperation] = CAST(CASE CT.[__$operation] WHEN 1 THEN ''D'' WHEN 2 THEN ''I'' WHEN 3 THEN ''U'' WHEN 4 THEN ''U'' ELSE ''I'' END AS NVARCHAR(10))' + CHAR(10) +
				',	[DWTrackingVersion] = (CAST(sys.fn_cdc_map_lsn_to_time (CT.[__$start_lsn]) AS DATETIME))'  + CHAR(10) +
				'FROM cdc.fn_cdc_get_net_changes_' + @SourceObjectSchema + '_' + @SourceObjectName + '(@LastStartLsn, @LastEndLsn, ''all'') AS CT' + CHAR(10) +
				'LEFT JOIN ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' AS SRC ON (' + @Sql3 + ')' + CHAR(10) +
				'WHERE CT.[__$operation] IN (1,2,4) AND ' + CASE @JobIsPartitionLoad WHEN 1 THEN '?AdfDynamicRangePartitionCondition ' ELSE '1 = 1 ' END 
				;
		END;

		/* Prepare select statement for incremental load using the Incremenatal load pattern */
		IF (@JobLoadModeETL = 'ICL')
		BEGIN
			SET @stmt =
				'SELECT ' + CHAR(10)
					+ @Sql2 +
				',	[DWOperation] = CAST(''I'' AS NVARCHAR(10)) ' + CHAR(10) +
				'FROM ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' AS SRC' + CHAR(10) +
				'WHERE ' + CASE @JobIsPartitionLoad WHEN 1 THEN '?AdfDynamicRangePartitionCondition ' ELSE '1 = 1 ' END + CHAR(10) +
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
				')' ;
		END;
	END;

	/* Prepare select statement for full load pattern */
	IF (@JobLoadModeETL = 'FULL')
	BEGIN
		SET @stmt =
			'SELECT ' + CHAR(10)
				+ @Sql2 +
			',	[DWOperation] = CAST(''I'' AS NVARCHAR(10)) ' + CHAR(10) +
			'FROM ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' AS SRC' + CHAR(10) +
			'WHERE ' + CASE @JobIsPartitionLoad WHEN 1 THEN '?AdfDynamicRangePartitionCondition' ELSE '1 = 1' END + CHAR(10) +
				CASE /* Is source object filter condition */
					WHEN (@SourceObjectFilter <> '') AND (@SourceObjectFilter IS NOT NULL) THEN 'AND ' + @SourceObjectFilter
					ELSE '' 
				END
			;
	END;

	/* If Job is reset and load is incremental pattern */
	IF (@JobIsReset = 1) 
	BEGIN

		/* Prepare select statement for incremental load using the Change Tracking pattern */
		IF (@JobLoadModeETL = 'CT')
		BEGIN
			SET @stmt =
				'/* Define variables @MinimumValidVersion and @CurrentVersion */' + CHAR(10) +
				'DECLARE @CurrentVersion BIGINT;' + CHAR(10) + CHAR(10) +
				'SELECT @CurrentVersion	= MAX(CT.[SYS_CHANGE_VERSION])' + CHAR(10) +
				'FROM CHANGETABLE(CHANGES ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ', 0) AS CT' + CHAR(10) + CHAR(10) +
				'SELECT ' + CHAR(10)
					+ @Sql2 +
				',	[DWOperation] = CAST(''I'' AS NVARCHAR(10))' + CHAR(10) +
				',	[DWTrackingVersion] = CAST(@CurrentVersion AS BIGINT)'  + CHAR(10) +
				'FROM ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' AS SRC' + CHAR(10) +
				'WHERE ' + CASE @JobIsPartitionLoad WHEN 1 THEN '?AdfDynamicRangePartitionCondition ' ELSE '1 = 1 ' END + CHAR(10) +
					CASE /* Is source object filter condition */
						WHEN (@SourceObjectFilter <> '') AND (@SourceObjectFilter IS NOT NULL) THEN 'AND ' + @SourceObjectFilter + CHAR(10)
						ELSE '' 
					END
				;
		END;

		/* Prepare select statement for incremental load using the Incremenatal load pattern */
		IF (@JobLoadModeETL = 'ICL')
		BEGIN
			SET @stmt =
				'SELECT ' + CHAR(10)
					+ @Sql2 +
				',	[DWOperation] = CAST(''I'' AS NVARCHAR(10)) ' + CHAR(10) +
				'FROM ' + QUOTENAME(@DataSourceDatabaseName) + '.' + QUOTENAME(@SourceObjectSchema) + '.' + QUOTENAME(@SourceObjectName) + ' AS SRC' + CHAR(10) +
				'WHERE ' + CASE @JobIsPartitionLoad WHEN 1 THEN '?AdfDynamicRangePartitionCondition ' ELSE '1 = 1 ' END + CHAR(10) +
					CASE /* Is source object filter condition */
						WHEN (@SourceObjectFilter <> '') AND (@SourceObjectFilter IS NOT NULL) THEN 'AND ' + @SourceObjectFilter + CHAR(10)
						ELSE '' 
					END
				;
		END;
	END;

	/* Log Start of extract */
	SET @Message = 
		'Load sequence ' + CAST(@LoadSequence AS NVARCHAR) + ' - ' + 
			CASE 
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'CT') THEN 'Change Tracking' 
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'CDC') THEN 'Change Data Capture' 
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'ICL') THEN 'Incremental load'
				WHEN (@JobIsReset = 0) AND (@JobLoadModeETL = 'FULL') THEN 'Full load'
				WHEN (@JobIsReset = 1) THEN 'Force full load'
			END + ' - ' +
		'Log start load of: ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName)

	EXEC spLog 'ETL', @PackageName, 'Info', 3, @Message, @DestinationTableName;

	/* Select variable to be used in Data Factory */
	SELECT

		/* Data Warehouse staging schema and table - Could be another SQL Database */
		[DestinationSchemaName]			=	@DestinationSchemaName
	,	[DestinationTableName]			=	@DestinationTableName
	,	[DataSourceName]				=	@DataSourceName
	,	[DataSourceServerName]			=	@DataSourceServerName	
	,	[DataSourceDatabaseName]		=	@DataSourceDatabaseName	
	,	[SourceObjectSchema]			=	@SourceObjectSchema
	,	[SourceObjectName]				=	@SourceObjectName

		/* T-SQL scripts to be executed in Azure Data Factory Copy activity */
	,	[SqlCommand]					=	@stmt
	,	[PreCopyCommand]				=	@PreCopyCommand
	,	[ADFWriteBatchSize]				=	@ADFWriteBatchSize
	,	[TabularTranslatorMapping]		=	@TabularTranslatorMapping

		/* Parameters to be used in Azure Data Factory if partition load is enabled */
	,	[SourceObjectPartitionOption]	=	@SourceObjectPartitionOption
	,	[SourceObjectPartitionColumn]	=	CASE WHEN @JobIsPartitionLoad = 1 THEN @SourceObjectPartitionColumn ELSE '' END
	,	[SourceObjectPartitionLower]	=	ISNULL(@SourceObjectPartitionLower, 0)
	,	[SourceObjectPartitionUpper]	=	ISNULL(@SourceObjectPartitionUpper, @ADFWriteBatchSize * @DefaultMaxDop)
	,	[DefaultMaxDop]					=	@DefaultMaxDop

		/* Data Warehouse parameters to determine whether a table should be full loaded or incremental loaded */
	,	[JobIsReset]					=	@JobIsReset
	,	[JobLoadModeETL]				=	@JobLoadModeETL

		/* Parameters to be used in Azure Data Factory if incremental load is enabled */
	,	[LastLoadedValue]				=	@LastLoadedValue 
	,	[CheckLoadedValue]				=	@CheckLoadedValue

END;