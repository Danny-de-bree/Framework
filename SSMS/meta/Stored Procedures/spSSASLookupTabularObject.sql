CREATE PROCEDURE [meta].[spSSASLookupTabularObject]

	@PackageName nvarchar(255)

AS
BEGIN

	DECLARE @message NVARCHAR(max) ;
	DECLARE @errorSeverity INT ;
	DECLARE @errorState INT ;

	BEGIN TRY
		SELECT 
			[AASOlapRegion]
		,	[AASOlapServer]
		,	[TabularObjectName]
		,	[IsEnabled]
		,	[IsIcremental]
		FROM meta.TabularObject 
		WHERE (IsEnabled = 1)
		ORDER BY [TabularObjectName]
	END TRY
	BEGIN CATCH
		SELECT @errorSeverity = ERROR_SEVERITY();
		SELECT @errorState = ERROR_STATE();
		SET @message = 'Find Tabular Object: Failed to find Cube entity due to ' + ERROR_MESSAGE();
		EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Error', @severity = 1, @message = @message ;
		RAISERROR (@message, @errorSeverity, @errorState);
	END CATCH

END