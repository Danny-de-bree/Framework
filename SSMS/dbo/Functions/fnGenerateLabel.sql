CREATE FUNCTION [dbo].[fnGenerateLabel] (
		@field1 NVARCHAR(128)
	,	@field2 NVARCHAR(128)
	,	@field3 NVARCHAR(128) = NULL
)
RETURNS TABLE
WITH SCHEMABINDING
AS
	RETURN
	SELECT LabelVal = (ISNULL(@field1 + ' ', '') + ISNULL(@field2, '') + ISNULL(' ' + @field3, ''))