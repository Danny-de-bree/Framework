
CREATE PROCEDURE [dbo].[spLog]
		-- Add the parameters for the stored procedure here
	    @subsystem	NVARCHAR(8)
	,	@source		NVARCHAR(255)
	,	@type		NVARCHAR(8)
	,	@severity	INT
	,	@message	NVARCHAR(MAX)
	,	@entity		NVARCHAR(128)	= NULL
	,	@operation	NVARCHAR(8)		= NULL
	,	@rows		INT				= NULL
AS
BEGIN
	-- SET NOCOUNT ON added to prevent extra result sets from
	-- interfering with SELECT statements.
	SET NOCOUNT ON;

	DECLARE @DWLogLevel	NVARCHAR(255);

	SET @subsystem	= LTRIM(RTRIM(@subsystem));
	SET @source		= LTRIM(RTRIM(@source));
	SET @type		= LTRIM(RTRIM(@type));
	SET @entity		= LTRIM(RTRIM(@entity));
	SET @operation	= LTRIM(RTRIM(@operation));

	SELECT @DWLogLevel = MAX(CASE WHEN ep.Name = @subsystem + 'LogLevel' THEN CONVERT(NVARCHAR(255), [value]) ELSE '' END)
	FROM sys.extended_properties AS ep WITH (NOLOCK);

	IF (@DWLogLevel LIKE '%' + @type + '%') OR (@type = 'Error')
	BEGIN
		-- Insert log message into Log table
		BEGIN TRY
			INSERT INTO dbo.[Log] 
				([Subsystem], [Source], [Type], [Severity], [Message], [Time], [Entity], [Operation], [Rows])
			VALUES
				(@subsystem, @source, @type, @severity, @message, GETUTCDATE(), @entity, @operation, @rows)
		END TRY
		BEGIN CATCH
			PRINT '   Unable to write log to database ' + @subsystem + ' ' + @type + ' ' + CAST(@severity AS nvarchar) + ' ' + @message + ' '  +  ' due to ' + ERROR_MESSAGE()
			RETURN -1;
		END CATCH
	END;

	RETURN 0;
END