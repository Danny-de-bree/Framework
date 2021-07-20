CREATE FUNCTION [dbo].[fnSplit](@Seperator NVARCHAR(1), @Input NVARCHAR(4000))
RETURNS TABLE 
WITH SCHEMABINDING
AS
RETURN (
	WITH Pieces(i, start, stop) AS (
		SELECT 1, 0, CHARINDEX(@Seperator, @Input, 0)

		UNION ALL

		SELECT p.i + 1, p.stop + 1, CHARINDEX(@Seperator, @Input, p.stop + 1)
		FROM Pieces AS p
		WHERE (p.stop > 0)
	)

	SELECT
		p.i,
		SUBSTRING(@Input, p.start, CASE WHEN (p.stop > 0) THEN (p.stop - p.start) ELSE 4000 END) AS part
	FROM Pieces AS p
)