
-- ===========================================================================
-- Author:		Ivan Erik Kragh
-- Create date: 2019-05-29
-- Description:	Convert CamelCase to Proper Case
-- ===========================================================================

CREATE FUNCTION [dbo].[fnGetProperCase](@Input AS NVARCHAR(255))
RETURNS @table TABLE (TxtVal NVARCHAR(255))
WITH SCHEMABINDING
AS 
BEGIN

	WHILE PATINDEX('%[^ ][ABCDEFGHIJKLMNOPQRSTUVWXYZ]%' COLLATE SQL_Latin1_General_CP1_CS_AS, @Input COLLATE SQL_Latin1_General_CP1_CS_AS) > 0
	BEGIN
	    SET @Input = STUFF(@Input, PATINDEX('%[^ ][ABCDEFGHIJKLMNOPQRSTUVWXYZ]%' COLLATE SQL_Latin1_General_CP1_CS_AS, @Input COLLATE SQL_Latin1_General_CP1_CS_AS) + 1, 0, ' ')
	END;
	
	INSERT @table 
	VALUES (@Input)

	RETURN;
END