CREATE FUNCTION dbo.ufnUTC2CET(@inputDateTime DATETIME)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN (
	SELECT CETDateTime = CAST(x.CETDateTime AS DATETIME)
	FROM (
	SELECT CETDateTime =
		CASE
			WHEN @inputDateTime BETWEEN DATEADD(HOUR,1,DATEADD(month,(YEAR(@inputDateTime) - 1900) * 12 + 2,30 - ((5 * YEAR(@inputDateTime)) / 4 + 4) % 7)) AND DATEADD(HOUR,1,DATEADD(month,(YEAR(@inputDateTime) - 1900) * 12 + 9,30 - ((5 * YEAR(@inputDateTime)) / 4 + 1) % 7)) THEN DATEADD(hour,2,@inputDateTime)
			ELSE DATEADD(hour,1,@inputDateTime)
		END
	) AS x
)