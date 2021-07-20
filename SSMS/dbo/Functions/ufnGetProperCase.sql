
-- ===========================================================================
-- Author:		Ivan Erik Kragh
-- Create date: 2019-05-29
-- Description:	Convert CamelCase to Proper Case
-- ===========================================================================

CREATE FUNCTION [dbo].[ufnGetProperCase](@Input as NVARCHAR(MAX))
RETURNS NVARCHAR(MAX)
AS
BEGIN
	DECLARE @Regex AS NVARCHAR(MAX) ;
	SET @Regex = '%[^ ][ABCDEFGHIJKLMNOPQRSTUVWXYZ]%' ;

	WHILE PATINDEX(@Regex COLLATE SQL_Latin1_General_CP1_CS_AS, @Input COLLATE SQL_Latin1_General_CP1_CS_AS) > 0
	BEGIN
	    SET @Input = STUFF(@Input, PATINDEX(@Regex COLLATE SQL_Latin1_General_CP1_CS_AS, @Input COLLATE SQL_Latin1_General_CP1_CS_AS) + 1, 0, ' ')
	END
	
	RETURN @Input
END