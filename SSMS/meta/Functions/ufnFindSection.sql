CREATE FUNCTION [meta].[ufnFindSection](@definition nvarchar(max), @searchStr nvarchar(32), @needWhiteAround int) 
RETURNS int
BEGIN
	DECLARE @searchPos int = -1;
	DECLARE @curPos int = 0;
	DECLARE @pLevel int = 0;
	DECLARE @fPos int;
	DECLARE @fPos2 int;
	DECLARE @startPPos int;
	DECLARE @endPPos int;
	DECLARE @preIdx int;
	DECLARE @sufIdx int;
	DECLARE @preChr nvarchar(1);  -- Must be able to hold an empty string
	DECLARE @sufChr nvarchar(1);  -- Must be able to hold an empty string

	DECLARE @maxIdx int;
	DECLARE @startIdx int;

	IF (@needWhiteAround = 1)
	BEGIN
		SET @startIdx = 2;
		SET @maxIdx = 5;
	END
	ELSE BEGIN
		SET @startIdx = 1;
		SET @maxIdx = 1;
	END;

	WHILE (@searchPos = -1)
	BEGIN
		IF (@pLevel = 0)
		BEGIN
			SET @fPos = 0;

			SET @preIdx = @startIdx;
			WHILE (@preIdx <= @maxIdx)
			BEGIN
				SET @sufIdx = @startIdx;
				WHILE (@sufIdx <= @maxIdx)
				BEGIN
					SET @preChr = CASE @preIdx
						WHEN 1 THEN ''
						WHEN 2 THEN ' '
						WHEN 3 THEN CHAR(9)
						WHEN 4 THEN CHAR(13)
						ELSE CHAR(10)
					END

					SET @sufChr = CASE @sufIdx
						WHEN 1 THEN ''
						WHEN 2 THEN ' '
						WHEN 3 THEN CHAR(9)
						WHEN 4 THEN CHAR(13)
						ELSE CHAR(10)
					END

					SET @fPos2 = CHARINDEX(@preChr + @searchStr + @sufChr, @definition, @curPos + 1)
					IF (@fPos2 > 0) AND ((@fPos2 < @fPos) OR (@fPos = 0)) SET @fPos = @fPos2

					SET @sufIdx = @sufIdx +1;
				END;

				SET @preIdx = @preIdx +1;
			END;
		END;

		IF (@fPos = 0) BREAK;

		SET @startPPos = CHARINDEX('(', @definition, @curPos + 1)

		SET @endPPos = CHARINDEX(')', @definition, @curPos + 1)

		IF (@pLevel = 0) AND ((@startPPos = 0) OR (@startPPos > @fPos))
		BEGIN
			SET @searchPos = @fPos;
		END
		ELSE BEGIN
			IF (@startPPos > 0) AND (@startPPos < @endPPos)
			BEGIN
				SET @pLevel += 1;
				SET @curPos = @startPPos;
			END

			IF (@endPPos > 0) AND ((@endPPos < @startPPos) OR (@startPPos = 0))
			BEGIN
				SET @pLevel -= 1;
				SET @curPos = @endPPos;
			END
		END
	END;

	RETURN @searchPos;
END