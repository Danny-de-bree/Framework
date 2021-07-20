
-- ===========================================================================
-- Author:		Flemming Haurum
-- Create date: 2014-10-16
-- Description:	Compares two version strings and determines if are valid,
--              lower, equal or higher
-- Return value:-2: Not valid
--				-1: The old version is a higher version than the new version
--				 0: The two versions are the same
--				 1: The new version is a higher version than the old version
-- ===========================================================================
CREATE FUNCTION [dbo].[ufnCompareVersions](@oldVersion [nvarchar](32), @newVersion [nvarchar](32)) 
RETURNS [int]
BEGIN
	SET @oldVersion = LTRIM(RTRIM(@oldVersion))
	SET @newVersion = LTRIM(RTRIM(@newVersion))

	-- Empty version strings is not valid
	IF (LEN(@oldVersion) = 0) OR (LEN(@newVersion) = 0)
	BEGIN
		RETURN -2
	END

	-- Only 0-9 and . is valid in the version string
	IF (PATINDEX('%[^0-9.]%', @oldVersion) <> 0) OR (PATINDEX('%[^0-9.]%', @newVersion) <> 0)
	BEGIN
		RETURN -2
	END

	-- Check if version strings starts with .
	IF (CHARINDEX('.', @oldVersion) = 1) OR (CHARINDEX('.', @newVersion) = 1)
	BEGIN
		RETURN -2
	END

	-- Check if version strings ends with .
	IF (CHARINDEX('.', REVERSE(@oldVersion)) = 1) OR (CHARINDEX('.', REVERSE(@newVersion)) = 1)
	BEGIN
		RETURN -2
	END

	-- Check if version strings contains ..
	IF (CHARINDEX('..', @oldVersion) <> 0) OR (CHARINDEX('..', @newVersion) <> 0)
	BEGIN
		RETURN -2
	END

	DECLARE @endPos [int]
	DECLARE @oldValue [int]
	DECLARE @newValue [int]

	WHILE (LEN(@oldVersion) > 0) AND (LEN(@newVersion) > 0)
	BEGIN
		SET @endPos = ISNULL(NULLIF(CHARINDEX('.', @oldVersion), 0), LEN(@oldVersion) + 1)
		SET @oldValue = CONVERT(int, SUBSTRING(@oldVersion, 1, @endPos - 1))
		SET @oldVersion = SUBSTRING(@oldVersion, @endPos + 1, LEN(@oldVersion))

		SET @endPos = ISNULL(NULLIF(CHARINDEX('.', @newVersion), 0), LEN(@newVersion) + 1)
		SET @newValue = CONVERT(int, SUBSTRING(@newVersion, 1, @endPos - 1))
		SET @newVersion = SUBSTRING(@newVersion, @endPos + 1, LEN(@newVersion))

		IF (@oldValue < @newValue)
		BEGIN
			RETURN 1
		END
		IF (@oldValue > @newValue)
		BEGIN
			RETURN -1
		END
	END

	-- Check if @newVersion is longer than @oldVersion - e.g. @oldVersion='1.1' and @newVersion='1.1.1'
	IF (LEN(@oldVersion) = 0) AND (LEN(@newVersion) > 0)
	BEGIN
		RETURN 1
	END
	-- Check if @oldVersion is longer than @newVersion - e.g. @oldVersion='1.1.1' and @newVersion='1.1'
	IF (LEN(@oldVersion) > 0) AND (LEN(@newVersion) = 0)
	BEGIN
		RETURN -1
	END

	RETURN 0
END