
-- ===========================================================================
-- Author:				Ivan Erik Kragh
-- Create date:			Oct 19 2019  2:41PM
-- Stored procedure:	[SSIS].[spLogStopTimeEGDP]
-- Description:			
--              
-- ===========================================================================
	
CREATE PROCEDURE [SSIS].[spExtract_ALL_06_LogStopTime]

		@PackageName nvarchar(255)

AS BEGIN

DECLARE @message nvarchar(max);

SET @message = 'Stop Azure Data Factory Pipeline: ' + @PackageName ;
EXEC dbo.spLog @subsystem = 'ETL', @source = @PackageName, @type = 'Info', @severity = 1, @message = @message;

end;