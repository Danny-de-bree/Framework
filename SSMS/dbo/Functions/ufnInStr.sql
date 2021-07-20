
CREATE FUNCTION [dbo].[ufnInStr](@str [nvarchar](max), @substr [nvarchar](max), @start [int], @occurrence [int])
RETURNS int
BEGIN
	DECLARE @found int = @occurrence
	DECLARE @pos int = @start

	WHILE (1 = 1)
	BEGIN
		-- Find the next occurrence
		SET @pos = CHARINDEX(@substr, @str, @pos)

		-- Nothing found
		IF (@pos IS NULL) OR (@pos = 0)
		BEGIN
			BREAK
		END

		-- The required occurrence found
		IF (@found = 1)
		BEGIN
			BREAK
		END

		-- Prepare to find another one occurrence
		SET @found = @found - 1
		SET @pos = @pos + 1
	END

	RETURN @pos
END