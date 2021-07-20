CREATE PROCEDURE [meta].[spTransformLoadEntity]

	@DestinationSchemaName NVARCHAR(255)
,	@DestinationTableName NVARCHAR(255)
,	@PackageName NVARCHAR(255)
,	@LoadSequence INT = 0
,	@emulation TINYINT = 1
AS
BEGIN

	DECLARE @DWDimensionPrefix NVARCHAR(255);
	DECLARE @SourceObjectPrefix NVARCHAR(255);
	DECLARE @StopExecution TINYINT = 0;

	/* Prepare common Data Warehouse parameters */
	SELECT @DWDimensionPrefix = MAX(CASE WHEN [name] = 'DWDimensionPrefix' THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK)
	WHERE (ep.class = 0) 
	GROUP BY ep.major_id

	/* Check if object exists */
	IF (OBJECT_ID(@DestinationSchemaName + '.' + @DestinationTableName) IS NULL)
	BEGIN
		/* To avoid breaking the load package when object does not exist - simply print error */
		PRINT 'Unable to load entity as it does not exists';		
		RETURN -1;
	END;

	/* Is it a fact or dimension load pattern? */
	IF (@DestinationSchemaName = @DWDimensionPrefix)
	BEGIN
		
		/* If entity is a dimension use dimension load pattern */
		EXEC meta.spTransformLoadDimension @DestinationSchemaName = @DestinationSchemaName, @DestinationTableName = @DestinationTableName, @PackageName = @PackageName, @LoadSequence = @LoadSequence, @Emulation = @emulation;
	END ELSE 
	BEGIN

		/* Else use fact load pattern */
		EXEC meta.spTransformLoadFact @DestinationSchemaName = @DestinationSchemaName, @DestinationTableName = @DestinationTableName, @PackageName = @PackageName, @LoadSequence = @LoadSequence, @Emulation = @emulation;
	END;

END;