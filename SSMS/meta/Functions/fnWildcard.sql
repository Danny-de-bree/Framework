CREATE FUNCTION [meta].[fnWildcard](@Wildcard nvarchar(8), @Input varchar(512))
RETURNS TABLE
AS
RETURN (
	WITH Series AS (
		SELECT CONVERT(int, SUBSTRING(@Input, 1, ISNULL(NULLIF(CHARINDEX(@Wildcard, @Input), 0), LEN(@Input) + 1) - 1)) AS num,
			SUBSTRING(@Input, 1, ISNULL(NULLIF(CHARINDEX(@Wildcard, @Input), 0), LEN(@Input) + 1) - 1) AS start,
			SUBSTRING(@Input, ISNULL(NULLIF(CHARINDEX(@Wildcard, @Input), 0), 1 - LEN(@Input)) + LEN(@Wildcard), LEN(@Input)) AS stop

		UNION ALL

		SELECT (s.num + 1) AS num, s.start, s.stop
		FROM Series AS s
		WHERE (s.num < s.stop)
	)

	SELECT s.num FROM Series AS s
)