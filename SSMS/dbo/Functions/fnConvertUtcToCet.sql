CREATE FUNCTION dbo.fnConvertUtcToCet (@dt DATETIME)
RETURNS DATETIME
AS
BEGIN 
    DECLARE @Year AS INT = DATEPART(YEAR,@dt);
    DECLARE @Day AS INT = DATEPART(DAY,@dt);
    DECLARE @Month AS INT = DATEPART(MONTH, @dt);

    DECLARE @LastSundayOfMarch AS INT = 31 - ((((5 * @Year) / 4) + 4) % 7);
    DECLARE @LastSundayOfOctober AS INT = 31 - ((((5 * @Year) / 4) + 1) % 7);
    -- The timestamp in UTC of switching to DST (01:00 on the last Sunday of March)
    DECLARE @ToDstSwitchDate AS DATETIME = DATEADD(hour, 1, DATEADD(month, (@Year - 1900) * 12 + 2, @LastSundayOfMarch - 1));
    -- The timestamp in CET of switching back to standard time (01:00 on the last Sunday of October)
    DECLARE @FromDstSwitchDate AS DATETIME = DATEADD(hour, 1, DATEADD(month, (@Year - 1900) * 12 + 9, @LastSundayOfOctober - 1));

	DECLARE @result DATETIME
	
    IF (@dt BETWEEN @ToDstSwitchDate AND @FromDstSwitchDate)
        SET @result =  DATEADD(hour, 2, @dt)
    ELSE
        SET @result = DATEADD(hour, 1, @dt)
        
    RETURN @result
END