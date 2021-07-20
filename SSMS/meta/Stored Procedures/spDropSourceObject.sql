CREATE PROCEDURE [meta].[spDropSourceObject]
		@DestinationSchemaName NVARCHAR(255) = ''
	,	@DestinationTableName NVARCHAR(255) = ''
	,	@IsHardDelete TINYINT = 0
	,	@DeleteHistory TINYINT = 0
	,	@Emulation TINYINT = 1
AS
BEGIN
	DECLARE @stmt NVARCHAR(MAX);
	DECLARE @DWDestinationSchemaName NVARCHAR(255);

	DECLARE @DWDimensionPrefix NVARCHAR(255);
	DECLARE @DWFactPrefix NVARCHAR(255);
	DECLARE @DWExtractDWSchemaName NVARCHAR(255);
	DECLARE @DWExtractStagingSchemaName NVARCHAR(255);
	DECLARE @DWExtractHistorySchemaName NVARCHAR(255);
	DECLARE @DWTransformSchemaName NVARCHAR(255);
	DECLARE @DWTransformDWSchemaName NVARCHAR(255);
	DECLARE @DWTransformDMSchemaName NVARCHAR(255);
	DECLARE @DWTransformStagingSchemaName NVARCHAR(255);

	DECLARE @Message NVARCHAR(MAX);
	DECLARE @PackageName NVARCHAR(255);
	DECLARE @triggerStmt NVARCHAR(max);
	DECLARE @dbTriggerDisabled TINYINT;

	SET @PackageName = OBJECT_NAME(@@PROCID);

	/* Prepare common data warehouse parameters */
	SELECT
		@DWDimensionPrefix				=	MAX(CASE WHEN (ep.name = 'DWDimensionPrefix')				THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	@DWFactPrefix					=	MAX(CASE WHEN (ep.name = 'DWFactPrefix')					THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	@DWExtractStagingSchemaName		=	MAX(CASE WHEN (ep.name = 'DWExtractStagingSchemaName')		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	@DWExtractDWSchemaName			=	MAX(CASE WHEN (ep.name = 'DWExtractDWSchemaName')			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	@DWExtractHistorySchemaName		=	MAX(CASE WHEN (ep.name = 'DWExtractHistorySchemaName')		THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	@DWTransformSchemaName			=	MAX(CASE WHEN (ep.name = 'DWTransformSchemaName')			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	@DWTransformStagingSchemaName	=	MAX(CASE WHEN (ep.name = 'DWTransformStagingSchemaName')	THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	@DWTransformDWSchemaName		=	MAX(CASE WHEN (ep.name = 'DWTransformDWSchemaName')			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	,	@DWTransformDMSchemaName		=	MAX(CASE WHEN (ep.name = 'DWTransformDMSchemaName')			THEN CAST(ep.value AS NVARCHAR(255)) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0)
	GROUP BY (ep.major_id)

	/* As we are potentially deleting data ensure the parameters are correctly configured - can not delete History if not hard deleted */
	IF (@DeleteHistory > @IsHardDelete)
	BEGIN
		SET @Message = 'Unable to delete history for ' + QUOTENAME(@DestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' when soft delete is enabled';
		PRINT @Message;
		RAISERROR(@Message, 1, 1);
		RETURN -1; 
		EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;
	END;

	/* For each schema in the EXTRACT layer generate create table script */
	DECLARE SchemaCur CURSOR LOCAL FOR
		SELECT s.name FROM sys.schemas AS s WHERE s.name IN (@DWExtractStagingSchemaName,@DWExtractDWSchemaName,@DWExtractHistorySchemaName) AND @DestinationSchemaName IN (@DWExtractStagingSchemaName) UNION 
		SELECT s.name FROM sys.schemas AS s WHERE s.name IN (@DWTransformStagingSchemaName,@DWTransformDWSchemaName,@DWTransformDMSchemaName) AND @DestinationSchemaName IN (@DWTransformSchemaName)
	OPEN SchemaCur
	FETCH NEXT FROM SchemaCur INTO @DWDestinationSchemaName
	WHILE @@FETCH_STATUS = 0
	BEGIN
		
		/* Check avaliablitiy of destination table */
		IF (OBJECT_ID(@DWDestinationSchemaName + '.' + @DestinationTableName)) IS NULL AND (@DWDestinationSchemaName != @DWTransformDMSchemaName)
		BEGIN
			SET @Message = 'Unable to find ' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ' as the object does not exist';
			PRINT @Message;
			RETURN -2;
		END;

		/* Create statment to be executed */
		SET @stmt = 
			CASE 
				WHEN (@DWDestinationSchemaName IN (@DWExtractStagingSchemaName,@DWExtractDWSchemaName,@DWExtractHistorySchemaName)) AND (@IsHardDelete = 1) AND (@DeleteHistory = 1)
					THEN 'DROP TABLE IF EXISTS ' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';' + CHAR(10) +
						 IIF(@DWDestinationSchemaName = @DestinationSchemaName, 'DELETE [dbo].[SourceObject] WHERE [DestinationSchema] = ''' + @DestinationSchemaName + ''' AND [DestinationTable] = ''' + @DestinationTableName + ''';', '')
				
				WHEN (@DWDestinationSchemaName IN (@DWExtractStagingSchemaName,@DWExtractDWSchemaName)) AND (@IsHardDelete = 1) AND (@DeleteHistory = 0)
					THEN 'DROP TABLE IF EXISTS ' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';' + CHAR(10) +
						 IIF(@DWDestinationSchemaName = @DestinationSchemaName, 'DELETE [dbo].[SourceObject] WHERE [DestinationSchema] = ''' + @DestinationSchemaName + ''' AND [DestinationTable] = ''' + @DestinationTableName + ''';', '')
				
				WHEN (@DWDestinationSchemaName IN (@DWExtractStagingSchemaName,@DWExtractDWSchemaName,@DWExtractHistorySchemaName)) AND (@IsHardDelete = 0)
					THEN IIF(@DWDestinationSchemaName = @DestinationSchemaName, 'UPDATE [dbo].[SourceObject] SET [IsEnabled] = 0 WHERE [DestinationSchema] = ''' + @DestinationSchemaName + ''' AND [DestinationTable] = ''' + @DestinationTableName + ''';', '') + CHAR(10) +
						 IIF(@DWDestinationSchemaName = @DestinationSchemaName, 'EXEC sys.sp_UpdateExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + @DestinationTableName + ''', @name = N''IsEnabled'', @value = 0;', '') + CHAR(10)
				
				WHEN (@DWDestinationSchemaName IN (@DWTransformStagingSchemaName,@DWTransformDWSchemaName,@DWTransformDMSchemaName)) AND (@IsHardDelete = 1) AND (@DeleteHistory = 0)
					THEN IIF(@DWDestinationSchemaName = @DWTransformStagingSchemaName, 'DROP VIEW IF EXISTS ' + QUOTENAME(@DWTransformSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';' + CHAR(10), '') +
						 IIF(@DWDestinationSchemaName = @DWTransformDMSchemaName, 'DROP VIEW IF EXISTS ' + QUOTENAME(@DWTransformDMSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';' + CHAR(10), '') + 
						 IIF(@DWDestinationSchemaName IN (@DWTransformStagingSchemaName, @DWTransformDWSchemaName),'DROP TABLE IF EXISTS ' + QUOTENAME(@DWDestinationSchemaName) + '.' + QUOTENAME(@DestinationTableName) + ';', '') + CHAR(10) +
						 IIF(@DWDestinationSchemaName = @DWTransformStagingSchemaName, 'DELETE [dbo].[SourceObject] WHERE [DestinationSchema] = ''' + @DWDestinationSchemaName + ''' AND [DestinationTable] = ''' + @DestinationTableName + ''';', '')

				WHEN (@DWDestinationSchemaName IN (@DWTransformStagingSchemaName,@DWTransformDWSchemaName,@DWTransformDMSchemaName)) AND (@IsHardDelete = 0)
					THEN IIF(@DWDestinationSchemaName = @DWTransformStagingSchemaName, 'UPDATE [dbo].[SourceObject] SET [IsEnabled] = 0 WHERE [DestinationSchema] = ''' + @DestinationSchemaName + ''' AND [DestinationTable] = ''' + @DestinationTableName + ''';', '') + CHAR(10) +
						 IIF(@DWDestinationSchemaName = @DWTransformStagingSchemaName, 'EXEC sys.sp_UpdateExtendedProperty @level0type = N''SCHEMA'', @level0name = N''' + @DestinationSchemaName + ''', @level1type = N''TABLE'', @level1name = N''' + @DestinationTableName + ''', @name = N''IsEnabled'', @value = 0;', '') + CHAR(10)

				ELSE ''
			END

		IF (@Emulation = 1)
		BEGIN 
			SELECT @DWDestinationSchemaName AS DestinationSchemaName, @DestinationTableName AS DestinationTableName , @stmt AS SqlStatement;
		END ELSE
		BEGIN
			BEGIN TRY
				BEGIN TRANSACTION

				/* Disable table trigger on database */
				IF (dbo.ufnIsTriggerEnabled('TableTracking') = 1)
				BEGIN
					SET @triggerStmt = 'DISABLE TRIGGER [TableTracking] ON DATABASE;' + CHAR(10) + 'DISABLE TRIGGER [ViewTracking] ON DATABASE;';
					EXEC sys.sp_executesql @triggerStmt;
					SET @dbTriggerDisabled = 1;
				END;

				EXEC sys.sp_executesql @stmt; 

				COMMIT TRANSACTION;

				SET @Message = 'Successfully removed [' + @DestinationSchemaName + '].[' + @DestinationTableName + '] in ' + @DWDestinationSchemaName + ' schema';
				PRINT @Message;
				EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Info', @severity = 3, @message = @message, @entity = @DestinationTableName;

				/* Enable if table trigger is disable on database */
				IF (@dbTriggerDisabled = 1)
				BEGIN
					SET @triggerStmt = 'ENABLE TRIGGER [TableTracking] ON DATABASE;' + CHAR(10) + 'ENABLE TRIGGER [ViewTracking] ON DATABASE;';
					EXEC sys.sp_executesql @triggerStmt;
					SET @dbTriggerDisabled = 0;
				END;

			END TRY
			BEGIN CATCH
				IF (@@TRANCOUNT > 0)
					ROLLBACK TRANSACTION;
				
				SET @Message = 'Failed to drop [' + @DestinationSchemaName + '].[' + @DestinationTableName + '] in ' + @DWDestinationSchemaName + ' schema due to: ' + ERROR_MESSAGE();
				PRINT @Message;
				RAISERROR(@Message, 1, 1);
				RETURN -3
				EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message, @entity = @DestinationTableName;
				EXEC dbo.spLog @subsystem = 'DW', @source = @PackageName, @type = 'Error', @severity = 1, @message = @stmt, @entity = @DestinationTableName;
			END CATCH;
		END;

		FETCH NEXT FROM SchemaCur INTO @DWDestinationSchemaName
	END
	CLOSE SchemaCur
	DEALLOCATE SchemaCur
END;