
CREATE FUNCTION [dbo].[ufnDateTimeUTC2CET] (@inputDateTime DATETIME) RETURNS DATETIME
AS
BEGIN 
  
   DECLARE @inputYear INT = YEAR(@inputDateTime);
   DECLARE @SwitchToDSTDateTime DATETIME = DATEADD(HOUR, 1, DATEADD(month, (@inputYear - 1900) * 12 + 2, 30 - ((((5 * @inputYear) / 4) + 4) % 7)))
          ,@SwitchFromDSTDateTime DATETIME = DATEADD(HOUR, 1, DATEADD(month, (@inputYear - 1900) * 12 + 9, 30 - ((((5 * @inputYear) / 4) + 1) % 7)));
       
   RETURN CASE 
     WHEN @inputDateTime BETWEEN @SwitchToDSTDateTime AND @SwitchFromDSTDateTime 
       THEN DATEADD(hour, 2, @inputDateTime) 
     ELSE DATEADD(hour, 1, @inputDateTime) 
   END
END