CREATE FUNCTION [dbo].[fnSingularize] (
	@FieldName NVARCHAR(255)
)
RETURNS @table TABLE (FieldName NVARCHAR(255))
WITH SCHEMABINDING
AS
BEGIN
	DECLARE @Output varchar(max)

	IF @FieldName NOT LIKE '%s'
	-- already singular
	BEGIN
		SET @Output = @FieldName
	END

	ELSE IF @FieldName LIKE '%ss'
	-- already singular ie. mass, chess
	BEGIN
		SET @Output = @FieldName
	END

	ELSE IF @FieldName LIKE '%ies' 
	-- ie. cherries, ladies
	BEGIN
		SET @Output = SUBSTRING(@FieldName, 1, LEN(@FieldName)-3) + 'y'
	END

	ELSE IF @FieldName LIKE '%oes' 
	-- ie. heroes, potatoes
	BEGIN
		SET @Output = SUBSTRING(@FieldName, 1, LEN(@FieldName) -2)
	END

	ELSE IF @FieldName LIKE '%es' and SUBSTRING(@FieldName, LEN(@FieldName)-2, 1) in ('a', 'e', 'i', 'o', 'u')
	-- ie. massages, phases
	BEGIN
		SET @Output = SUBSTRING(@FieldName, 1, LEN(@FieldName) -1)
	END

	ELSE IF @FieldName LIKE '%es' and SUBSTRING(@FieldName, LEN(@FieldName) -2, 1) in ('h')
	-- ie. witches, dishes
	BEGIN
		SET @Output = SUBSTRING(@FieldName, 1, LEN(@FieldName) - 2)
	END

	ELSE IF @FieldName LIKE '%es' and SUBSTRING(@FieldName, LEN(@FieldName) -2, 1) in ('b','c','d','f','g','j','k','l','m','n','p','q','r','s','t','v','w','x','y','z')
	-- ie. kisses, judges
	BEGIN
		SET @Output = SUBSTRING(@FieldName, 1, LEN(@FieldName) - 1)
	END

	ELSE IF @FieldName LIKE '%s'
	-- ie. laps, clocks, boys
	BEGIN
		SET @Output = SUBSTRING(@FieldName, 1, LEN(@FieldName) -1)
	END

	INSERT @table 
	VALUES (@Output)

	RETURN;
END