
-- =============================================

CREATE FUNCTION dbo.ufnGetEasterDaysFromYear (@year INT)
RETURNS TABLE
AS
	RETURN (
	SELECT 
		[CalendarDate]		=	dbo.ufnGetEasterSundayFromYear(@year)
	,	[EasterDayUKName]	=	'Easter'
	,	[EasterDayDKName]	=	'Påske' 
	
	UNION
	
	SELECT 
		[CalendarDate]		=	 DATEADD([DD],-2,dbo.ufnGetEasterSundayFromYear(@year)) 
	,	[EasterDayUKName]	=	'Good Friday'											
	,	[EasterDayDKName]	=	'Langfredag'											
	
	UNION
	
	SELECT 
		[CalendarDate]		=	DATEADD([DD],-3,dbo.ufnGetEasterSundayFromYear(@year))	
	,	[EasterDayUKName]	=	'Easter Thursday'										
	,	[EasterDayDKName]	=	'Skærtorsdag'											
	
	UNION
	
	SELECT 
		[CalendarDate]		=	DATEADD([DD],1,dbo.ufnGetEasterSundayFromYear(@year))	
	,	[EasterDayUKName]	=	'Easter Monday'											
	,	[EasterDayDKName]	=	'Påske mandag'											
	)