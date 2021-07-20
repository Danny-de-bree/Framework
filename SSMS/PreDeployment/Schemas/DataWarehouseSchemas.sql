SET NOCOUNT ON;
DECLARE @SchemaVariable TABLE (SchemaId INT, SchemaName NVARCHAR(255))
DECLARE @stmt NVARCHAR(MAX) = '';
DECLARE @RowCount INT = 0;
DECLARE @Counter INT = 1;

INSERT INTO @SchemaVariable (SchemaId, SchemaName)
SELECT 
	[SchemaId]		=	ROW_NUMBER() OVER(ORDER BY (SELECT NULL))
,	[SchemaName]	=	CAST(ep.[value] AS NVARCHAR(255))
FROM sys.extended_properties AS ep WITH (TABLOCK)
WHERE (ep.class = 0) AND (ep.name LIKE ('%Schema%'))

SELECT @RowCount = COUNT(1) FROM @SchemaVariable;

WHILE (@Counter <= @RowCount)
BEGIN
	
	SET @stmt = '';

	SELECT 
		@stmt =
		'IF SCHEMA_ID(''' + SchemaName + ''') IS NULL' + CHAR(10) +
		'BEGIN' + CHAR(10) +
		'	EXEC(''CREATE SCHEMA ' + QUOTENAME(SchemaName) + ' AUTHORIZATION [dbo];'')' + CHAR(10) +
		'END;' + CHAR(10)
	FROM @SchemaVariable
	WHERE (SchemaId = @Counter)

	EXEC sys.sp_executesql @stmt;

	SET @Counter = @Counter + 1

END