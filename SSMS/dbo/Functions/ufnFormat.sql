
-- Begin user-defined function

CREATE FUNCTION [dbo].[ufnFormat](@input datetime)
RETURNS nvarchar(128)
BEGIN
	RETURN convert(datetime, @input, 110);
END

-- End of user-defined function