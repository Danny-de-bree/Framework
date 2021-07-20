CREATE FUNCTION [meta].[ufnCheckSchema] (@SchemaName NVARCHAR(255))
RETURNS BIT
AS
BEGIN
	IF EXISTS (
		SELECT 1
		FROM sys.extended_properties AS ep WITH (NOLOCK)
		WHERE (ep.class = 0) AND ep.name LIKE ('DW%Prefix') AND ep.value = @SchemaName
	)
	BEGIN
		RETURN 1;
	END

	RETURN 0;
END;