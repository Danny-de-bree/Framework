
-- ===========================================================================
-- Author:		Flemming Haurum
-- Create date: 2016-04-05
-- Description:	Return the status of a trigger
-- ===========================================================================
CREATE FUNCTION [dbo].[ufnIsTriggerEnabled](@triggerName nvarchar(128))
RETURNS int
BEGIN
	DECLARE @status int;

	SELECT @status = CASE WHEN t.is_disabled = 0 THEN 1 ELSE 0 END
	FROM sys.triggers AS t
	WHERE (t.name = @triggerName)

	RETURN @status
END