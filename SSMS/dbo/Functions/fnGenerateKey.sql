CREATE FUNCTION [dbo].[fnGenerateKey]
(		@field1 NVARCHAR(255)
	,	@field2 NVARCHAR(255) = NULL
	,	@field3 NVARCHAR(255) = NULL
	,	@field4 NVARCHAR(255) = NULL
	,	@field5 NVARCHAR(255) = NULL
)
RETURNS TABLE
WITH SCHEMABINDING
AS
RETURN
SELECT KeyVal =
	CONVERT(BIGINT,
		HASHBYTES('SHA2_256',
			UPPER(ISNULL(TRIM(@field1),'1')) + '|' +
			UPPER(ISNULL(TRIM(@field2),'2')) + '|' +
			UPPER(ISNULL(TRIM(@field3),'3')) + '|' +
			UPPER(ISNULL(TRIM(@field4),'4')) + '|' +
			UPPER(ISNULL(TRIM(@field5),'5')) + '|'
		)
	)