
CREATE FUNCTION [dbo].[ufnGetUserName]() 
RETURNS [nvarchar](32)
BEGIN
	DECLARE @userName nvarchar(32);

	SET @userName = (SELECT SUSER_SNAME()); -- DOMAIN\user_login_name if using Windows Authentication, otherwise SQL Server login identification name

	RETURN @userName;
END