CREATE PROCEDURE [meta].[spUpdateIsChangeTracking]
	
	@DestinationSchemaName NVARCHAR(255)
,	@DestinationTableName NVARCHAR(255)
,	@IsChangeTracking TINYINT = 0
AS
BEGIN
	SET NOCOUNT ON;

	/* Update SourceObject set @IsChangeTracking */
	UPDATE so SET 
		SO.LoadModeETL			=	CASE WHEN (@IsChangeTracking = 1) AND (so.LoadModeETL = 'FULL') THEN 'CT' ELSE so.LoadModeETL END
	,	SO.IncrementalField		=	CASE WHEN (@IsChangeTracking = 1) AND (so.LoadModeETL = 'FULL') THEN '' ELSE so.IncrementalField END
	FROM [dbo].[SourceObject] AS so
	JOIN [dbo].[SourceConnection] AS sc ON (so.SourceConnectionID = sc.SourceConnectionID)
	WHERE (sc.SourceConnectionSchema = @DestinationSchemaName) AND (so.SourceObjectTable = @DestinationTableName)

END