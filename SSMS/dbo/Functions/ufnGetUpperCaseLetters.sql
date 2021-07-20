-- ===========================================================================
-- Author:		Ivan Erik Kragh
-- Create date: 2019-05-29
-- Description:	Get only UpperCase letters from word (used to create alias)
-- ===========================================================================

CREATE FUNCTION [dbo].[ufnGetUpperCaseLetters](@string as NVARCHAR(255))
RETURNS NVARCHAR(MAX)
AS
BEGIN
	WHILE PATINDEX('%[^A-Z0-9]%',@string COLLATE Latin1_General_BIN) <> 0
	BEGIN
	    SET @string = 
			STUFF(
			    @string,
			    PATINDEX('%[^A-Z0-9]%',@string COLLATE Latin1_General_BIN),1,''
			)
	END

	RETURN @string;
END