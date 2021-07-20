CREATE PROCEDURE [meta].[spGetExtendedProperties]
AS
BEGIN
	DECLARE @CRLF NVARCHAR(2) = CHAR(13) + CHAR(10);
	DECLARE @PivotColumns NVARCHAR(MAX) = '';
	DECLARE @ColumnList NVARCHAR(MAX) = '';
	DECLARE @stmt NVARCHAR(MAX);

	SELECT 
		@PivotColumns = @PivotColumns + IIF(@PivotColumns != '', ', ', '') + CAST(QUOTENAME(ep.name) AS NVARCHAR(255))
	,	@ColumnList = @ColumnList + IIF(@ColumnList != '', ', ', '') + CHAR(9) + CAST(QUOTENAME(ep.name) AS NVARCHAR(255)) + ' = ' + CAST('piv.' + QUOTENAME(ep.name) AS NVARCHAR(255)) + CHAR(10)
	FROM sys.extended_properties AS ep
	WHERE ep.class = 0


	SET @stmt = 
		'With CteExtendedProperties AS (' + @CRLF +
		'	SELECT ' + @CRLF +
		'		[EpName] = CAST(ep.name AS NVARCHAR(255)) ' + @CRLF +
		'	,	[EpValue] = CAST(ep.value AS NVARCHAR(255)) ' + @CRLF +
		'	FROM sys.extended_properties AS ep ' + @CRLF +
		'	WHERE (ep.class = 0)' + @CRLF +	
		')'+ @CRLF + @CRLF +
	
		'SELECT ' + @CRLF +
			@ColumnList +
		',	[DWDatabaseName] = CAST(DB_NAME() AS NVARCHAR(255))' + @CRLF +
		',	[DWServerName] = CAST(@@servername AS NVARCHAR(255))' + @CRLF +
		'FROM CteExtendedProperties AS ep' + @CRLF +
		'PIVOT (' + @CRLF +
		'	MAX(ep.EpValue)' + @CRLF +
		'	FOR ep.EpName IN (' + @PivotColumns + ')' + @CRLF +
		') AS piv' + @CRLF
	;

	EXEC sys.sp_executesql @stmt;
END;