CREATE FUNCTION [dbo].[getlocaldate]()
RETURNS datetime
AS
BEGIN
    DECLARE @D datetimeoffset;
    SET @D = CONVERT(datetimeoffset, SYSDATETIMEOFFSET()) AT TIME ZONE 'Central European Standard Time';
    RETURN(CONVERT(datetime,@D));
END